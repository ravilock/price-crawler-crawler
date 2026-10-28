package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/crawler/internal/messaging"
)

func TestWorkerAndAuditRelay(t *testing.T) {
	dsn, broker := os.Getenv("SOURCE_ADMIN_TEST_DATABASE_URL"), os.Getenv("SOURCE_ADMIN_TEST_AMQP_URL")
	if dsn == "" || broker == "" {
		t.Skip("requires isolated migrated PostgreSQL and RabbitMQ")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	conn, err := amqp.Dial(broker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	if _, err = ch.QueueDeclare("audit.event.recorded.v1", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	reply, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := ch.Consume(reply.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	org := "00000000-0000-4000-8000-000000000001"
	cfg := IdentityConfig{Publisher: "source-test", Administrators: []Principal{{Subject: "worker-admin", Role: "administrator", Organization: org}}}
	raw, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "identity.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if os.Getenv("SOURCE_ADMIN_TEST_EXTERNAL_WORKER") == "" {
		done := make(chan error, 1)
		go func() { done <- consume(ctx, db, broker, path) }()
		defer func() { cancel(); <-done }()
	}

	for i := 0; i < 100; i++ {
		probe, e := conn.Channel()
		if e != nil {
			t.Fatal(e)
		}
		_, e = probe.QueueDeclarePassive("source.admin.command.v1", true, false, false, false, nil)
		_ = probe.Close()
		if e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
		if i == 99 {
			t.Fatal("worker queue unavailable")
		}
	}
	send := func(user, key, domain string) Result {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"actor": Actor{Subject: "worker-admin", Role: "administrator"}, "action": "inclusion.request", "domain": domain, "requested_categories": []string{"components"}})
		e := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000004", MessageType: "source.admin.command", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: "00000000-0000-4000-8000-000000000005", IdempotencyKey: key, Payload: body}
		wire, _ := e.Encode()
		if err = ch.PublishWithContext(ctx, "", "source.admin.command.v1", true, false, amqp.Publishing{UserId: user, ReplyTo: reply.Name, CorrelationId: e.CorrelationID, Body: wire}); err != nil {
			t.Fatal(err)
		}
		select {
		case delivery := <-responses:
			resp, err := messaging.DecodeDelivery(delivery)
			if err != nil {
				t.Fatal(err)
			}
			if resp.CausationID != e.MessageID || resp.CorrelationID != e.CorrelationID {
				t.Fatal("reply lost trace ids")
			}
			var result Result
			if err = json.Unmarshal(resp.Payload, &result); err != nil {
				t.Fatal(err)
			}
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("reply timed out")
		}
		return Result{}
	}
	if got := send("", "deny-1", "denied.example"); got.Status != "forbidden" {
		t.Fatalf("untrusted publisher: %+v", got)
	}
	if got := send("source-test", "allow-1", "worker.example"); got.Status != "pending" || got.RequestID == "" {
		t.Fatalf("authorized request: %+v", got)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err = relayOnce(ctx, db, broker, logger); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM message_outbox WHERE published_at IS NOT NULL`).Scan(&count); err != nil || count < 1 {
		t.Fatalf("outbox not confirmed: %d %v", count, err)
	}
}
