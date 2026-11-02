package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/results"
	"price-crawler/services/crawler/internal/scheduler"
)

func TestResultReadCursorIsTenantAndSubjectScoped(t *testing.T) {
	dsn := os.Getenv("SCHEDULER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SCHEDULER_TEST_DATABASE_URL to a fresh disposable database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, name := range []string{"001_initial.up.sql", "002_message_delivery.up.sql", "003_collection.up.sql", "004_source_admin.up.sql", "005_scheduler.up.sql", "006_domain_budget.up.sql", "007_domain_leases.up.sql", "008_crawl_scope.up.sql", "009_admin_notifications.up.sql", "010_crawl_leases.up.sql", "011_result_read_ownership.up.sql"} {
		raw, readErr := os.ReadFile("../../migrations/" + name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.Exec(string(raw)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	org := "00000000-0000-4000-8000-000000000201"
	otherOrg := "00000000-0000-4000-8000-000000000202"
	source := "00000000-0000-4000-8000-000000000203"
	subject := "00000000-0000-4000-8000-000000000204"
	otherSubject := "00000000-0000-4000-8000-000000000205"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,approved_categories) VALUES($1,$2,'store.example',true,10,'["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	ids := []string{"00000000-0000-4000-8000-000000000211", "00000000-0000-4000-8000-000000000212", "00000000-0000-4000-8000-000000000213"}
	for i, id := range ids {
		requester := subject
		if i == 1 {
			requester = otherSubject
		}
		created := base.Add(time.Duration(i) * time.Second)
		if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,correlation_id,source_domain_id,category,created_at) VALUES($1,$2,$3,'manual','completed',$4,$5,$6,'parts',$7)`, id, org, "https://store.example/item-"+id, "key-"+id, "00000000-0000-4000-8000-000000000206", source, created); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO crawl_intent_key(organization_id,idempotency_key,crawl_request_id,submitted_url,reason,source_domain_id,category,requester_id) VALUES($1,$2,$3,$4,'manual',$5,'parts',$6)`, org, "key-"+id, id, "https://store.example/item-"+id, source, requester); err != nil {
			t.Fatal(err)
		}
	}
	offer := "00000000-0000-4000-8000-000000000221"
	observation := "00000000-0000-4000-8000-000000000222"
	if _, err = db.Exec(`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url,current_price_cents,payment_method,availability,last_checked_at,last_valid_price_at) VALUES($1,$2,$3,'https://store.example/item-1',12000,'cash','available',$4,$4)`, offer, org, source, base); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE crawl_request SET offer_id=$2 WHERE organization_id=$1 AND id=$3`, org, offer, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,outcome,provenance,adapter_version) VALUES($1,$2,$3,12000,'cash','available',$4,$5,$6,'partial','{}','test')`, observation, org, offer, base, ids[0], ids[0]); err != nil {
		t.Fatal(err)
	}
	actor := results.Actor{Subject: subject, Role: "end_user"}
	page := results.Read(context.Background(), db, org, actor, "", nil, 1, time.Now().UTC())
	if page.Status != "ok" || len(page.Items) != 1 || page.Items[0].RequestID != ids[0] || page.Items[0].PriceStatus != "verified" || page.Items[0].PriceCents == nil || *page.Items[0].PriceCents != 12000 || page.NextCursor == nil {
		t.Fatalf("first page = %+v", page)
	}
	newID := "00000000-0000-4000-8000-000000000214"
	if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,correlation_id,source_domain_id,category,created_at) VALUES($1,$2,'https://store.example/new','manual','queued','new-key','00000000-0000-4000-8000-000000000206',$3,'parts',now()+interval '1 hour')`, newID, org, source); err != nil {
		t.Fatal(err)
	}
	second := results.Read(context.Background(), db, org, actor, "", page.NextCursor, 1, time.Now().UTC())
	if second.Status != "ok" || len(second.Items) != 1 || second.Items[0].RequestID != ids[2] || second.NextCursor != nil {
		t.Fatalf("second page crossed snapshot/subject boundary: %+v", second)
	}
	foreign := results.Read(context.Background(), db, otherOrg, actor, "", page.NextCursor, 1, time.Now().UTC())
	if foreign.Status != "forbidden" {
		t.Fatalf("foreign cursor status = %s", foreign.Status)
	}
	expired := results.Read(context.Background(), db, org, actor, "", page.NextCursor, 1, time.Now().UTC().Add(31*24*time.Hour))
	if expired.Status != "expired" {
		t.Fatalf("expired cursor status = %s", expired.Status)
	}
	other := results.Read(context.Background(), db, org, results.Actor{Subject: otherSubject, Role: "end_user"}, "", nil, 100, time.Now().UTC())
	if other.Status != "ok" || len(other.Items) != 1 || other.Items[0].RequestID != ids[1] {
		t.Fatalf("other subject scope = %+v", other)
	}
	missing := results.Read(context.Background(), db, org, actor, ids[1], nil, 10, time.Now().UTC())
	if missing.Status != "not_found" {
		t.Fatalf("foreign request status = %s", missing.Status)
	}
	ownerRequest := "00000000-0000-4000-8000-000000000215"
	if _, err = scheduler.Submit(context.Background(), db, scheduler.Intent{ID: ownerRequest, Organization: org, URL: "https://store.example/owned", Reason: "manual", IdempotencyKey: "owned-key", CorrelationID: "00000000-0000-4000-8000-000000000206", SourceID: source, Category: "parts", RequesterID: subject}); err != nil {
		t.Fatal(err)
	}
	var storedRequester string
	if err = db.QueryRow(`SELECT requester_id FROM crawl_intent_key WHERE organization_id=$1 AND idempotency_key='owned-key'`, org).Scan(&storedRequester); err != nil || storedRequester != subject {
		t.Fatalf("request owner=%s err=%v", storedRequester, err)
	}
	event := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000231", MessageType: "collection.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: org, CorrelationID: "00000000-0000-4000-8000-000000000206", Payload: []byte(`{"outcome":"partial"}`)}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = messaging.EnqueueOutbox(context.Background(), tx, event, "collection.result.v1"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := messaging.ClaimOutbox(context.Background(), tx, 10)
	if err != nil || len(claimed) != 1 || claimed[0].MessageID != event.MessageID {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = messaging.ScheduleOutboxRetry(context.Background(), tx, event.MessageID, 1, "broker unavailable", now); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var available time.Time
	var claimedUntil sql.NullTime
	if err = db.QueryRow(`SELECT attempts,available_at,claimed_until FROM message_outbox WHERE message_id=$1`, event.MessageID).Scan(&attempts, &available, &claimedUntil); err != nil || attempts != 1 || !available.After(now) || claimedUntil.Valid {
		t.Fatalf("retry attempts=%d available=%s claimed=%v err=%v", attempts, available, claimedUntil, err)
	}
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = messaging.ScheduleOutboxRetry(context.Background(), tx, event.MessageID, messaging.MaxDeliveryAttempts, "retry budget exhausted", now); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var deadLettered sql.NullTime
	if err = db.QueryRow(`SELECT dead_lettered_at FROM message_outbox WHERE message_id=$1`, event.MessageID).Scan(&deadLettered); err != nil || !deadLettered.Valid {
		t.Fatalf("exhausted outbox message was not retained for recovery: %v err=%v", deadLettered, err)
	}
}
