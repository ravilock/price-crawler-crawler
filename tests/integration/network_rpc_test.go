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
	"os/exec"
	"testing"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/policy"
	"price-crawler/services/crawler/internal/scheduler"
)

func TestNetworkRPCScopedFetch(t *testing.T) {
	dsn := os.Getenv("SCHEDULER_TEST_DATABASE_URL")
	broker := os.Getenv("CRAWLER_TEST_RABBITMQ_URL")
	if dsn == "" || broker == "" {
		t.Skip("set disposable database and broker URLs")
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
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte("User-agent: *\nAllow: /"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head><script>document.title="Fixture rendered"</script></head><body>fixture</body></html>`))
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
	renderID := "00000000-0000-4000-8000-000000000006"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,adapter_name,approved_categories) VALUES($1,$2,'localhost',true,60000,'fixture','["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}')`, org, source); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,correlation_id,source_domain_id,category) VALUES($1,$2,$3,'manual','rendering','key',$4,$5,'parts')`, job, org, origin+"/page", corr, source); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,payload) VALUES($1,'render.page.requested.v1','render.page.requested',$2,$3,jsonb_build_object('payload',jsonb_build_object('crawl_request_id',$4::text,'source_domain_id',$5::text)))`, renderID, org, corr, job, source); err != nil {
		t.Fatal(err)
	}
	var conn *amqp.Connection
	for i := 0; i < 100; i++ {
		conn, err = amqp.Dial(broker)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		policy.RunFetchConsumer(ctx, db, scheduler.NewBudget(2), broker, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() { cancel(); <-done }()
	var ch *amqp.Channel
	for i := 0; i < 100; i++ {
		ch, err = conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		_, err = ch.QueueDeclarePassive("network.fetch.requested.v1", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "price.dead-letter", "x-dead-letter-routing-key": "network.fetch.requested.v1.dead"})
		if err == nil {
			break
		}
		_ = ch.Close()
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := ch.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	call := func(organization string) string {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"crawl_request_id": job, "source_domain_id": source, "url": origin + "/page", "max_bytes": 1024, "deadline_ms": 5000})
		request := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000005", MessageType: "network.fetch.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: organization, CorrelationID: corr, CausationID: renderID, Payload: payload}
		raw, err := request.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err = ch.PublishWithContext(context.Background(), "", "network.fetch.requested.v1", false, false, amqp.Publishing{UserId: "test", ReplyTo: queue.Name, CorrelationId: corr, Body: raw}); err != nil {
			t.Fatal(err)
		}
		select {
		case delivery := <-responses:
			reply, err := messaging.Decode(delivery.Body)
			if err != nil || reply.CausationID != request.MessageID {
				t.Fatalf("reply %+v %v", reply, err)
			}
			var result struct {
				Status string `json:"status"`
			}
			if err = json.Unmarshal(reply.Payload, &result); err != nil {
				t.Fatal(err)
			}
			return result.Status
		case <-time.After(8 * time.Second):
			t.Fatal("RPC timed out")
			return ""
		}
	}
	if got := call(org); got != "ok" {
		t.Fatalf("authorized fetch status %s", got)
	}
	if got := call("00000000-0000-4000-8000-000000000099"); got != "blocked" {
		t.Fatalf("foreign tenant status %s", got)
	}
	executable := os.Getenv("RENDERER_TEST_CHROME")
	if executable == "" {
		t.Log("set RENDERER_TEST_CHROME to exercise the live renderer")
		return
	}
	runCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	command := exec.CommandContext(runCtx, "node", "--test", "test/live_crawler.integration.test.js")
	command.Dir = "../../../renderer-parser"
	command.Env = append(os.Environ(), "RENDERER_LIVE_BROKER="+broker, "RENDERER_LIVE_ORG="+org, "RENDERER_LIVE_SOURCE="+source, "RENDERER_LIVE_JOB="+job, "RENDERER_LIVE_URL="+origin+"/page", "RENDERER_LIVE_MESSAGE="+renderID, "RENDERER_LIVE_CORRELATION="+corr, "PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="+executable)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("live renderer: %v\n%s", err, output)
	}
}
