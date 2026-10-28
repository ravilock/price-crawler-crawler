package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/messaging"
)

func TestStoreTransactions(t *testing.T) {
	t.Setenv("SOURCE_ADMIN_SUPPORTED_INTEGRATIONS", "fixture")
	dsn := os.Getenv("SOURCE_ADMIN_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SOURCE_ADMIN_TEST_DATABASE_URL to a migrated disposable database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	org := "00000000-0000-4000-8000-000000000001"
	actor := Principal{Subject: "admin-test", Role: "administrator", Organization: org}
	ctx := context.Background()
	send := func(payload string, key string, principal Principal) Result {
		t.Helper()
		e := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000002", MessageType: "source.admin.command", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: "00000000-0000-4000-8000-000000000003", IdempotencyKey: key, Payload: json.RawMessage(payload)}
		result, err := Execute(ctx, db, e, principal)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := `{"actor":{"subject":"admin-test","role":"administrator"},"action":"inclusion.request","domain":"example.com","requested_categories":["components"]}`
	first := send(request, "request-1", actor)
	if first.Status != "pending" || first.RequestID == "" {
		t.Fatalf("request: %+v", first)
	}
	again := send(request, "request-1", actor)
	if again.Status != first.Status || again.RequestID != first.RequestID {
		t.Fatalf("replay changed result: %+v", again)
	}
	if got := send(request, "request-2", Principal{}); got.Status != "forbidden" {
		t.Fatalf("unverified caller: %+v", got)
	}
	conflict := send(`{"actor":{"subject":"admin-test","role":"administrator"},"action":"inclusion.request","domain":"other.example","requested_categories":["components"]}`, "request-1", actor)
	if conflict.Status != "conflict" {
		t.Fatalf("idempotency conflict: %+v", conflict)
	}
	path := `{"actor":{"subject":"admin-test","role":"administrator"},"action":"robots.path_exception.create","source_id":"` + first.RequestID + `","path":"/item","reason":"reviewed"}`
	if got := send(path, "early-path", actor); got.Status != "conflict" {
		t.Fatalf("unapproved source path: %+v", got)
	}
	badReview := `{"actor":{"subject":"admin-test","role":"administrator"},"action":"inclusion.review","source_id":"` + first.RequestID + `","decision":"approve","approved_categories":["unrequested"],"review":{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture","request_pace_per_minute":10}}`
	if got := send(badReview, "bad-review", actor); got.Status != "invalid" {
		t.Fatalf("unrequested category: %+v", got)
	}
	crossOrg := "00000000-0000-4000-8000-000000000099"
	readPayload := []byte(`{"actor":{"subject":"other-admin","role":"administrator"},"action":"source.read","source_id":"` + first.RequestID + `"}`)
	crossEnvelope := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000002", MessageType: "source.admin.command", SchemaVersion: 1, OccurredAt: time.Now(), Organization: crossOrg, CorrelationID: "00000000-0000-4000-8000-000000000003", Payload: readPayload}
	cross, err := Execute(ctx, db, crossEnvelope, Principal{Subject: "other-admin", Role: "administrator", Organization: crossOrg})
	if err != nil || cross.Status != "forbidden" {
		t.Fatalf("cross-tenant read: %+v %v", cross, err)
	}
	review := `{"actor":{"subject":"admin-test","role":"administrator"},"action":"inclusion.review","source_id":"` + first.RequestID + `","decision":"approve","approved_categories":["components"],"review":{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture","request_pace_per_minute":10}}`
	if got := send(review, "review-1", actor); got.Status != "ok" || got.State != "approved" {
		t.Fatalf("review: %+v", got)
	}
	if got := send(path, "path-1", actor); got.Status != "ok" || got.AuditID == "" {
		t.Fatalf("path: %+v", got)
	}
	if _, err = db.ExecContext(ctx, `UPDATE source_admin_audit SET action=action`); err == nil {
		t.Fatal("audit update accepted")
	}
	for _, table := range []string{"source_admin_audit", "message_outbox", "source_admin_receipt"} {
		var n int
		if err = db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 3 {
			t.Fatalf("%s count %d: %v", table, n, err)
		}
	}
}
