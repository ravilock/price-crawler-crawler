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
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/collection"
	"price-crawler/services/crawler/internal/messaging"
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
	for _, name := range []string{"001_initial.up.sql", "002_message_delivery.up.sql", "003_collection.up.sql", "004_source_admin.up.sql", "005_scheduler.up.sql", "006_domain_budget.up.sql", "007_domain_leases.up.sql", "008_crawl_scope.up.sql", "009_admin_notifications.up.sql", "010_crawl_leases.up.sql"} {
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
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			return
		}
		if state == "failed" {
			t.Fatal("429 failed without retry")
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("rate-limited request did not enter retry wait")
}
