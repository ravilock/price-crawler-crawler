package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/collection"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/policy"
	"price-crawler/services/crawler/internal/scheduler"
)

func TestCollectionWorkerPersistsFinderHandoff(t *testing.T) {
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
		raw, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(raw)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	listener, err := net.Listen("tcp", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	var arrivalsMu sync.Mutex
	var arrivals []time.Time
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Path) >= 8 && r.URL.Path[:8] == "/budget/" {
			arrivalsMu.Lock()
			arrivals = append(arrivals, time.Now())
			arrivalsMu.Unlock()
		}
		if r.URL.Path == "/rate" {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nAllow: /"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<title>Widget</title><meta property="product:price:amount" content="100.00"><meta property="product:price:currency" content="BRL"><meta property="product:payment_method" content="cash">`))
	}), ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()
	origin := fmt.Sprintf("http://localhost:%d", listener.Addr().(*net.TCPAddr).Port)
	t.Setenv("MOCK_ENV", "test")
	t.Setenv("MOCK_SOURCE_ORIGIN", origin)
	t.Setenv("ROBOTS_SLOW_REQUESTS_PER_MINUTE", "60000")
	org := "00000000-0000-4000-8000-000000000001"
	source := "00000000-0000-4000-8000-000000000002"
	job := "00000000-0000-4000-8000-000000000003"
	corr := "00000000-0000-4000-8000-000000000004"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,adapter_name,approved_categories) VALUES($1,$2,'localhost',true,60000,'fixture','["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}')`, org, source); err != nil {
		t.Fatal(err)
	}
	_, err = scheduler.Submit(context.Background(), db, scheduler.Intent{ID: job, Organization: org, URL: origin + "/product", Reason: "manual", IdempotencyKey: "key", CorrelationID: corr, SourceID: source, Category: "parts"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		collection.Run(ctx, db, scheduler.NewBudget(2), 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() { cancel(); <-done }()
	var finderRequest messaging.Envelope
	handedOff := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		if err = db.QueryRow(`SELECT state FROM crawl_request WHERE id=$1`, job).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "failed" {
			t.Fatal("collection failed")
		}
		if state == "finding" {
			var raw []byte
			if err = db.QueryRow(`SELECT payload FROM message_outbox WHERE organization_id=$1 AND destination='product.find.requested.v1'`, org).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			e, err := messaging.Decode(raw)
			finderRequest = e
			if err != nil || e.CorrelationID != corr || e.Organization != org {
				t.Fatalf("handoff %+v %v", e, err)
			}
			var payload struct {
				Page struct {
					Fields []any `json:"fields"`
				} `json:"page"`
			}
			if err = json.Unmarshal(e.Payload, &payload); err != nil || len(payload.Page.Fields) < 3 {
				t.Fatalf("page %s: %v", e.Payload, err)
			}
			handedOff = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !handedOff {
		t.Fatal("collection did not hand off to finder")
	}
	fields := map[string]any{"title": map[string]any{"raw_value": "Widget", "normalized_value": "Widget", "evidence_ref": "title", "status": "valid", "confidence": 1, "reason": nil}}
	foundPayload, _ := json.Marshal(map[string]any{"crawl_request_id": job, "normalization_version": "1", "fields": fields, "attributes": []any{}, "offer": map[string]any{"price_cents": 10000, "currency": "BRL", "payment_method": "cash", "availability": "available", "price_status": "valid"}})
	found := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000006", MessageType: "product.information.found", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: corr, CausationID: finderRequest.MessageID, Payload: foundPayload}
	if err = collection.ApplyFinder(context.Background(), db, found); err != nil {
		t.Fatal(err)
	}
	if err = collection.ApplyFinder(context.Background(), db, found); err != nil {
		t.Fatal(err)
	}
	found.MessageID = "00000000-0000-4000-8000-000000000007"
	if err = collection.ApplyFinder(context.Background(), db, found); err != nil {
		t.Fatal(err)
	}
	var observations int
	var price sql.NullInt64
	var state string
	if err = db.QueryRow(`SELECT count(*) FROM price_availability_observation WHERE organization_id=$1 AND crawl_request_id=$2`, org, job).Scan(&observations); err != nil || observations != 1 {
		t.Fatalf("observations=%d err=%v", observations, err)
	}
	if err = db.QueryRow(`SELECT r.state,o.current_price_cents FROM crawl_request r JOIN offer o ON o.id=r.offer_id AND o.organization_id=r.organization_id WHERE r.id=$1`, job).Scan(&state, &price); err != nil || state != "completed" || !price.Valid || price.Int64 != 10000 {
		t.Fatalf("state=%s price=%v err=%v", state, price, err)
	}
	var eventRaw []byte
	if err = db.QueryRow(`SELECT payload FROM message_outbox WHERE organization_id=$1 AND destination='collection.result.v1'`, org).Scan(&eventRaw); err != nil {
		t.Fatal(err)
	}
	event, err := messaging.Decode(eventRaw)
	if err != nil || event.CorrelationID != corr {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	// All collection reasons and browser subrequests must use the same durable
	// source budget, even when they are handled concurrently.
	if _, err = db.Exec(`UPDATE source_domain_config SET max_requests_per_minute=1200 WHERE id=$1`, source); err != nil {
		t.Fatal(err)
	}
	for i, reason := range []string{"manual", "scheduled", "discovery"} {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", 30+i)
		_, err = scheduler.Submit(context.Background(), db, scheduler.Intent{ID: id, Organization: org, URL: origin + "/budget/" + reason, Reason: reason, IdempotencyKey: "budget-" + reason, CorrelationID: corr, SourceID: source, Category: "parts"})
		if err != nil {
			t.Fatal(err)
		}
	}
	renderID := "00000000-0000-4000-8000-000000000040"
	if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,correlation_id,source_domain_id,category) VALUES($1,$2,$3,'manual','rendering','budget-render',$4,$5,'parts')`, renderID, org, origin+"/budget/render", corr, source); err != nil {
		t.Fatal(err)
	}
	renderMessageID := "00000000-0000-4000-8000-000000000042"
	if _, err = db.Exec(`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,payload) VALUES($1,'render.page.requested.v1','render.page.requested',$2,$3,jsonb_build_object('payload',jsonb_build_object('crawl_request_id',$4::text,'source_domain_id',$5::text)))`, renderMessageID, org, corr, renderID, source); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"crawl_request_id": renderID, "source_domain_id": source, "url": origin + "/budget/render", "max_bytes": 4096, "deadline_ms": 5000})
	render := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000041", MessageType: "network.fetch.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: corr, CausationID: renderMessageID, Payload: payload}
	renderDone := make(chan error, 1)
	go func() {
		reply, fetchErr := policy.ExecuteFetch(context.Background(), db, scheduler.NewBudget(2), render)
		if fetchErr == nil && reply.Status != "ok" {
			fetchErr = fmt.Errorf("renderer admission status: %s", reply.Status)
		}
		renderDone <- fetchErr
	}()
	if err = <-renderDone; err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		arrivalsMu.Lock()
		count := len(arrivals)
		arrivalsMu.Unlock()
		if count == 4 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	arrivalsMu.Lock()
	observedArrivals := append([]time.Time(nil), arrivals...)
	arrivalsMu.Unlock()
	if len(observedArrivals) != 4 {
		t.Fatalf("shared budget saw %d requests, want 4", len(observedArrivals))
	}
	for i := 1; i < len(observedArrivals); i++ {
		if observedArrivals[i].Sub(observedArrivals[i-1]) < 40*time.Millisecond {
			t.Fatalf("shared budget pacing bypassed: %v", observedArrivals[i].Sub(observedArrivals[i-1]))
		}
	}
	retryJob := "00000000-0000-4000-8000-000000000005"
	_, err = scheduler.Submit(context.Background(), db, scheduler.Intent{ID: retryJob, Organization: org, URL: origin + "/rate", Reason: "manual", IdempotencyKey: "rate-key", CorrelationID: corr, SourceID: source, Category: "parts"})
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		var count int
		var next sql.NullTime
		if err = db.QueryRow(`SELECT state,attempt_count,next_attempt_at FROM crawl_request WHERE id=$1`, retryJob).Scan(&state, &count, &next); err != nil {
			t.Fatal(err)
		}
		if state == "retry_wait" {
			var cooldown time.Time
			if err = db.QueryRow(`SELECT cooldown_until FROM source_domain_budget WHERE domain='localhost'`).Scan(&cooldown); err != nil || count != 1 || !next.Valid || cooldown.Before(next.Time) {
				t.Fatalf("retry count=%d next=%v cooldown=%v err=%v", count, next, cooldown, err)
			}
			break
		}
		if state == "failed" {
			t.Fatal("429 failed without retry")
		}
		time.Sleep(25 * time.Millisecond)
	}
	var attempt int
	var retryState string
	if err = db.QueryRow(`SELECT state,attempt_count FROM crawl_request WHERE id=$1`, retryJob).Scan(&retryState, &attempt); err != nil || retryState != "retry_wait" || attempt != 1 {
		t.Fatalf("first retry state=%s attempt=%d err=%v", retryState, attempt, err)
	}
	for expected := 2; expected <= 4; expected++ {
		// Advance the disposable clock gates without waiting for Retry-After.
		if _, err = db.Exec(`UPDATE source_domain_budget SET cooldown_until='-infinity',next_allowed_at='-infinity' WHERE domain='localhost'`); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`UPDATE crawl_request SET next_attempt_at=now() WHERE id=$1`, retryJob); err != nil {
			t.Fatal(err)
		}
		deadline = time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if err = db.QueryRow(`SELECT state,attempt_count FROM crawl_request WHERE id=$1`, retryJob).Scan(&retryState, &attempt); err != nil {
				t.Fatal(err)
			}
			if (expected < 4 && retryState == "retry_wait" && attempt == expected) || (expected == 4 && retryState == "failed" && attempt == 3) {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if (expected < 4 && (retryState != "retry_wait" || attempt != expected)) || (expected == 4 && (retryState != "failed" || attempt != 3)) {
			t.Fatalf("attempt %d: state=%s retries=%d", expected, retryState, attempt)
		}
	}
	if _, err = db.Exec(`UPDATE page_fetch_outcome SET evidence_expires_at=observed_at+interval '1 second' WHERE organization_id=$1 AND crawl_request_id=$2 AND bounded_evidence IS NOT NULL`, org, job); err != nil {
		t.Fatal(err)
	}
	expired, err := collection.ExpireEvidence(context.Background(), db, time.Now().UTC().Add(2*time.Second), 10)
	if err != nil || expired != 1 {
		t.Fatalf("expired=%d err=%v", expired, err)
	}
	var evidenceHash, excerpt sql.NullString
	var evidenceExpiry sql.NullTime
	if err = db.QueryRow(`SELECT evidence_hash,bounded_evidence,evidence_expires_at FROM page_fetch_outcome WHERE organization_id=$1 AND crawl_request_id=$2`, org, job).Scan(&evidenceHash, &excerpt, &evidenceExpiry); err != nil || evidenceHash.Valid || excerpt.Valid || evidenceExpiry.Valid {
		t.Fatalf("expired evidence remains accessible: hash=%v excerpt=%v expiry=%v err=%v", evidenceHash, excerpt, evidenceExpiry, err)
	}
	var observationCount, provenance int
	if err = db.QueryRow(`SELECT count(*) FROM price_availability_observation WHERE organization_id=$1 AND crawl_request_id=$2`, org, job).Scan(&observationCount); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM product_information_candidate WHERE organization_id=$1 AND crawl_request_id=$2 AND field_provenance <> '{}'::jsonb`, org, job).Scan(&provenance); err != nil {
		t.Fatal(err)
	}
	if observationCount != 1 || provenance != 1 {
		t.Fatalf("retention deleted lifetime records: observations=%d provenance=%d", observationCount, provenance)
	}
}
