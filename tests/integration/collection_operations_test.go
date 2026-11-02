package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/policy"
	"price-crawler/services/crawler/internal/scheduler"
)

// Set this only to a new disposable database: migrations create owned tables.
func TestSchedulerCoalescingAndRecovery(t *testing.T) {
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
		data, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if name == "005_scheduler.up.sql" {
			if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES('00000000-0000-4000-8000-000000000999','00000000-0000-4000-8000-000000000001','https://store.example/old','manual','completed','old-key')`); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = db.Exec(string(data)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	org := "00000000-0000-4000-8000-000000000001"
	budgetCtx, budgetCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer budgetCancel()
	budgetA := scheduler.PersistentBudget{DB: db, Local: scheduler.NewBudget(2), Domain: "store.example", Interval: 100 * time.Millisecond}
	budgetB := scheduler.PersistentBudget{DB: db, Local: scheduler.NewBudget(2), Domain: "store.example", Interval: 100 * time.Millisecond}
	release, err := budgetA.Acquire(budgetCtx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if _, err = budgetB.Acquire(ctx); err == nil {
		cancel()
		release()
		t.Fatal("second instance bypassed active reservation")
	}
	cancel()
	release()
	started := time.Now()
	release, err = budgetB.Acquire(budgetCtx)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if time.Since(started) < 50*time.Millisecond {
		t.Fatal("persisted pacing was bypassed")
	}
	if err = budgetA.Cooldown(context.Background(), time.Now().Add(120*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	release, err = budgetB.Acquire(budgetCtx)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if time.Since(started) < 90*time.Millisecond {
		t.Fatal("persistent cooldown was bypassed")
	}
	lossCtx, lossCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	lostRelease, err := budgetA.Acquire(lossCtx)
	if err != nil {
		lossCancel()
		t.Fatal(err)
	}
	var terminated bool
	if err = db.QueryRow(`SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid() LIMIT 1`).Scan(&terminated); err != nil || !terminated {
		lostRelease()
		lossCancel()
		t.Fatalf("terminate lock session: %v", err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	if _, err = budgetB.Acquire(blockedCtx); err == nil {
		blockedCancel()
		lostRelease()
		lossCancel()
		t.Fatal("lost database session allowed overlapping work")
	}
	blockedCancel()
	lostRelease()
	lossCancel()
	time.Sleep(1300 * time.Millisecond)
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), time.Second)
	recovered, err := budgetB.Acquire(recoveryCtx)
	if err != nil {
		recoveryCancel()
		t.Fatalf("lease recovery: %v", err)
	}
	recovered()
	recoveryCancel()
	sourceID := "00000000-0000-4000-8000-000000000777"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,approved_categories) VALUES($1,$2,'store.example',true,10,'["parts"]')`, sourceID, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{}')`, org, sourceID); err != nil {
		t.Fatal(err)
	}
	commandPayload, _ := json.Marshal(map[string]string{"crawl_request_id": "00000000-0000-4000-8000-000000000778", "url": "https://store.example/command?utm_source=x&color=red", "reason": "manual", "idempotency_key": "command-key"})
	commandEnvelope := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000779", MessageType: "crawl.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: "00000000-0000-4000-8000-000000000780", Payload: commandPayload}
	commandBytes, err := commandEnvelope.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.HandleCommand(context.Background(), db, commandBytes, "attacker", "trusted"); !errors.Is(err, scheduler.ErrInvalidCommand) {
		t.Fatalf("publisher not denied: %v", err)
	}
	commandReceipt, err := scheduler.HandleCommand(context.Background(), db, commandBytes, "trusted", "trusted")
	if err != nil || !commandReceipt.Created {
		t.Fatalf("command receipt=%+v err=%v", commandReceipt, err)
	}
	var submitted string
	if err = db.QueryRow(`SELECT submitted_url FROM crawl_request WHERE id=$1`, commandReceipt.ID).Scan(&submitted); err != nil || submitted != "https://store.example/command?color=red" {
		t.Fatalf("normalized URL=%q err=%v", submitted, err)
	}
	scopedPayload, _ := json.Marshal(map[string]string{"crawl_request_id": "00000000-0000-4000-8000-000000000781", "url": "https://store.example/scoped", "source_domain_id": sourceID, "category": "parts", "reason": "manual", "idempotency_key": "scoped-key"})
	scopedEnvelope := commandEnvelope
	scopedEnvelope.MessageType = "crawl.scoped.requested"
	scopedEnvelope.Payload = scopedPayload
	scopedBytes, err := scopedEnvelope.Encode()
	if err != nil {
		t.Fatal(err)
	}
	scopedReceipt, err := scheduler.HandleCommand(context.Background(), db, scopedBytes, "trusted", "trusted")
	if err != nil || !scopedReceipt.Created {
		t.Fatalf("scoped receipt=%+v err=%v", scopedReceipt, err)
	}
	var storedSource, storedCategory string
	if err = db.QueryRow(`SELECT source_domain_id::text,category FROM crawl_request WHERE id=$1`, scopedReceipt.ID).Scan(&storedSource, &storedCategory); err != nil || storedSource != sourceID || storedCategory != "parts" {
		t.Fatalf("scope=%s/%s err=%v", storedSource, storedCategory, err)
	}
	scopedPayload, _ = json.Marshal(map[string]string{"crawl_request_id": "00000000-0000-4000-8000-000000000782", "url": "https://store.example/other", "source_domain_id": sourceID, "category": "unapproved", "reason": "manual", "idempotency_key": "scoped-bad"})
	scopedEnvelope.Payload = scopedPayload
	scopedBytes, _ = scopedEnvelope.Encode()
	if _, err = scheduler.HandleCommand(context.Background(), db, scopedBytes, "trusted", "trusted"); !errors.Is(err, scheduler.ErrInvalidCommand) {
		t.Fatalf("unapproved category: %v", err)
	}
	at := time.Now().UTC()
	recorder := policy.SQLRecorder{DB: db, WriteAlert: policy.WriteRobotsNotification}
	if err = recorder.RecordRobots(context.Background(), org, sourceID, policy.RobotsRecord{Status: "unreadable", CheckedAt: at, ExpiresAt: at.Add(time.Hour), AlertAdministrator: true}); err != nil {
		t.Fatal(err)
	}
	notices, err := policy.ReadNotifications(context.Background(), db, org, 50)
	if err != nil || len(notices) != 1 || notices[0].SourceID != sourceID {
		t.Fatalf("notifications=%+v err=%v", notices, err)
	}
	other, err := policy.ReadNotifications(context.Background(), db, "00000000-0000-4000-8000-000000000099", 50)
	if err != nil || len(other) != 0 {
		t.Fatalf("cross tenant notifications=%+v err=%v", other, err)
	}
	old := scheduler.Intent{ID: "00000000-0000-4000-8000-000000000998", Organization: org, URL: "https://store.example/old", Reason: "manual", IdempotencyKey: "old-key"}
	oldReceipt, err := scheduler.Submit(context.Background(), db, old)
	if err != nil || oldReceipt.ID != "00000000-0000-4000-8000-000000000999" || oldReceipt.Created {
		t.Fatalf("retained key: %+v %v", oldReceipt, err)
	}
	const n = 12
	var wg sync.WaitGroup
	ids := make(chan string, n)
	failures := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			intent := scheduler.Intent{ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+10), Organization: org, URL: "https://store.example/product", Reason: "manual", IdempotencyKey: fmt.Sprintf("key-%d", i)}
			receipt, err := scheduler.Submit(context.Background(), db, intent)
			if err != nil {
				failures <- err
				return
			}
			ids <- receipt.ID
		}(i)
	}
	wg.Wait()
	close(failures)
	close(ids)
	for err := range failures {
		t.Error(err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if first != id {
			t.Fatalf("multiple active requests: %s and %s", first, id)
		}
	}
	if first == "" {
		t.Fatal("no request accepted")
	}
	if _, err = db.Exec(`UPDATE crawl_request SET state='fetching' WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err = db.Exec(`UPDATE crawl_request SET lease_token='00000000-0000-4000-8000-000000000123' WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.ScheduleRetry(context.Background(), db, org, first, "store.example", "timeout", "", now, 0, "00000000-0000-4000-8000-000000000124"); err == nil {
		t.Fatal("stale worker scheduled a retry after losing its lease")
	}
	retried, err := scheduler.ScheduleRetry(context.Background(), db, org, first, "store.example", "rate_limited", "2", now, 0, "00000000-0000-4000-8000-000000000123")
	if err != nil || !retried {
		t.Fatalf("schedule retry: %v %v", retried, err)
	}
	var retryState string
	var attempts int
	var next, cooldown time.Time
	if err = db.QueryRow(`SELECT r.state,r.attempt_count,r.next_attempt_at,b.cooldown_until FROM crawl_request r JOIN source_domain_budget b ON b.domain='store.example' WHERE r.id=$1`, first).Scan(&retryState, &attempts, &next, &cooldown); err != nil || retryState != "retry_wait" || attempts != 1 || next.Before(now.Add(2*time.Second-time.Millisecond)) || cooldown.Before(next) {
		t.Fatalf("retry state=%s attempts=%d next=%v cooldown=%v err=%v", retryState, attempts, next, cooldown, err)
	}
	if _, err = db.Exec(`UPDATE crawl_request SET state='completed' WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	replay := scheduler.Intent{ID: "00000000-0000-4000-8000-000000000200", Organization: org, URL: "https://store.example/product", Reason: "manual", IdempotencyKey: "key-1"}
	got, err := scheduler.Submit(context.Background(), db, replay)
	if err != nil || got.ID != first || got.Created {
		t.Fatalf("replay %+v %v", got, err)
	}
	replay.URL = "https://store.example/other"
	if _, err = scheduler.Submit(context.Background(), db, replay); !errors.Is(err, scheduler.ErrConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
	replay.IdempotencyKey = "new-key"
	replay.URL = "https://store.example/product"
	got, err = scheduler.Submit(context.Background(), db, replay)
	if err != nil || !got.Created || got.ID == first {
		t.Fatalf("new intent %+v %v", got, err)
	}
}

type loopbackResolver struct{}

func (loopbackResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
}
func TestHTTPAndRendererShareBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	origin := "http://mock-store:" + parsed.Port()
	budget := scheduler.NewBudget(2)
	sourceBudget := scheduler.SourceBudget{Budget: budget, Domain: "mock-store", Interval: 80 * time.Millisecond, Concurrency: 1}
	robots := policy.HTTPRobotsFetcher{Origin: server.URL, MockOrigin: server.URL, LocalOrTest: true, Budget: sourceBudget}
	if _, _, err := robots.FetchRobots(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
	client := policy.NetworkClient{Policy: policy.NetworkPolicy{Domain: "mock-store", LocalOrTest: true, MockOrigin: origin, Resolver: loopbackResolver{}}, Budget: sourceBudget, Authorize: func(context.Context, string) error { return nil }}
	start := time.Now()
	result, err := client.Fetch(context.Background(), origin+"/page", 1024)
	if err != nil || string(result.Body) != "ok" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if time.Since(start) < 60*time.Millisecond {
		t.Fatal("browser request bypassed the HTTP source budget")
	}
}

func TestCrawlConsumerBrokerAndDatabase(t *testing.T) {
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
		data, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(data)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	org := "00000000-0000-4000-8000-000000000001"
	source := "00000000-0000-4000-8000-000000000002"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,approved_categories) VALUES($1,$2,'store.example',true,10,'["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{}')`, org, source); err != nil {
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
		scheduler.RunCommandConsumer(ctx, db, broker, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() { cancel(); <-done }()
	var ch *amqp.Channel
	for i := 0; i < 100; i++ {
		ch, err = conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		_, err = ch.QueueDeclarePassive("crawl.requested.v1", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "price.dead-letter", "x-dead-letter-routing-key": "crawl.requested.v1.dead"})
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
	id := "00000000-0000-4000-8000-000000000003"
	payload, _ := json.Marshal(map[string]string{"crawl_request_id": id, "url": "https://store.example/offer?utm_source=x", "reason": "manual", "idempotency_key": "broker-key"})
	body, err := (messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000004", MessageType: "crawl.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: "00000000-0000-4000-8000-000000000005", Payload: payload}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err = ch.PublishWithContext(context.Background(), "", "crawl.requested.v1", false, false, amqp.Publishing{UserId: "test", Body: body}); err != nil {
		t.Fatal(err)
	}
	persisted := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err = db.QueryRow(`SELECT count(*) FROM crawl_request WHERE id=$1 AND submitted_url='https://store.example/offer'`, id).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			persisted = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !persisted {
		t.Fatal("crawl intent was not persisted")
	}
	at := time.Now().UTC()
	record := policy.RobotsRecord{Status: "unreadable", CheckedAt: at, ExpiresAt: at.Add(time.Hour), AlertAdministrator: true}
	if err = (policy.SQLRecorder{DB: db, WriteAlert: policy.WriteRobotsNotification}).RecordRobots(context.Background(), org, source, record); err != nil {
		t.Fatal(err)
	}
	identity, _ := json.Marshal(map[string]any{"publisher": "test", "administrators": []map[string]string{{"subject": "admin", "role": "administrator", "organization_id": org}}})
	t.Setenv("SOURCE_ADMIN_IDENTITY_JSON", string(identity))
	alertsDone := make(chan struct{})
	go func() {
		defer close(alertsDone)
		policy.RunNotificationReader(ctx, db, broker, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() { cancel(); <-alertsDone }()
	var alertCh *amqp.Channel
	for i := 0; i < 100; i++ {
		alertCh, err = conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		_, err = alertCh.QueueDeclarePassive("source.alerts.read.requested.v1", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "price.dead-letter", "x-dead-letter-routing-key": "source.alerts.read.requested.v1.dead"})
		if err == nil {
			break
		}
		_ = alertCh.Close()
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = alertCh.Close() }()
	replyQueue, err := alertCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	replies, err := alertCh.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	alertPayload, _ := json.Marshal(map[string]any{"actor": map[string]string{"subject": "admin", "role": "administrator"}, "limit": 50})
	correlation := "00000000-0000-4000-8000-000000000006"
	alertRequest := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000007", MessageType: "source.alerts.read.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: correlation, Payload: alertPayload}
	alertBytes, err := alertRequest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err = alertCh.PublishWithContext(context.Background(), "", "source.alerts.read.requested.v1", false, false, amqp.Publishing{UserId: "test", ReplyTo: replyQueue.Name, CorrelationId: correlation, Body: alertBytes}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-replies:
		reply, err := messaging.Decode(message.Body)
		if err != nil {
			t.Fatal(err)
		}
		var data struct {
			Status string `json:"status"`
			Items  []struct {
				SourceID string `json:"source_id"`
			} `json:"items"`
		}
		if err = json.Unmarshal(reply.Payload, &data); err != nil || data.Status != "ok" || len(data.Items) != 1 || data.Items[0].SourceID != source {
			t.Fatalf("notification reply=%s err=%v", reply.Payload, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification reply timed out")
	}
}
