package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/crawler/internal/admin"
	"price-crawler/services/crawler/internal/collection"
	"price-crawler/services/crawler/internal/scheduler"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}
func TestControlledSourceToObservation(t *testing.T) {
	dsn, broker, mockBin, finderBin := os.Getenv("SCHEDULER_TEST_DATABASE_URL"), os.Getenv("CRAWLER_TEST_RABBITMQ_URL"), os.Getenv("MOCK_TEST_BINARY"), os.Getenv("FINDER_TEST_BINARY")
	if dsn == "" || broker == "" || mockBin == "" || finderBin == "" {
		t.Skip("set disposable dependencies and fixture binaries")
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
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	sourcePort, controlPort := freePort(t), freePort(t)
	origin := fmt.Sprintf("http://localhost:%d", sourcePort)
	fixtureDir, err := filepath.Abs("../../../../deploy/mock-store")
	if err != nil {
		t.Fatal(err)
	}
	mock := exec.CommandContext(ctx, mockBin)
	mock.Dir = fixtureDir
	mock.Env = append(os.Environ(), "MOCK_ENV=test", "MOCK_CONTROL_TOKEN=test-token", "MOCK_STATE_DIR="+t.TempDir(), fmt.Sprintf("MOCK_SOURCE_ADDR=[::]:%d", sourcePort), fmt.Sprintf("MOCK_CONTROL_ADDR=[::]:%d", controlPort))
	if err = mock.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = mock.Wait() }()
	client := &http.Client{Timeout: time.Second}
	ready := false
	for i := 0; i < 100; i++ {
		response, err := client.Get(origin + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("mock source failed to start")
	}
	t.Setenv("MOCK_ENV", "test")
	t.Setenv("MOCK_SOURCE_ORIGIN", origin)
	t.Setenv("ROBOTS_SLOW_REQUESTS_PER_MINUTE", "60000")
	org := "00000000-0000-4000-8000-000000000001"
	source := "00000000-0000-4000-8000-000000000002"
	corr := "00000000-0000-4000-8000-000000000003"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,adapter_name,approved_categories) VALUES($1,$2,'localhost',true,60000,'fixture','["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}')`, org, source); err != nil {
		t.Fatal(err)
	}
	var connection *amqp.Connection
	for i := 0; i < 100; i++ {
		connection, err = amqp.Dial(broker)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	if _, err = channel.QueueDeclare("collection.result.v1", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	launch := func(run func()) { workers.Add(1); go func() { defer workers.Done(); run() }() }
	budget := scheduler.NewBudget(2)
	launch(func() { collection.Run(ctx, db, budget, 1, logger) })
	launch(func() { collection.RunFinderResults(ctx, db, broker, "test", logger) })
	launch(func() { admin.RunAuditRelay(ctx, db, broker, logger) })
	finder := exec.CommandContext(ctx, finderBin)
	finder.Env = append(os.Environ(), "RABBITMQ_URL="+broker, "HTTP_ADDR=127.0.0.1:0")
	if err = finder.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = finder.Wait() }()
	runJob := func(id, key string, wantPrice int64, wantAvailability string) {
		t.Helper()
		_, err := scheduler.Submit(context.Background(), db, scheduler.Intent{ID: id, Organization: org, URL: origin + "/products/offer-a", Reason: "manual", IdempotencyKey: key, CorrelationID: corr, SourceID: source, Category: "parts"})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(25 * time.Second)
		for time.Now().Before(deadline) {
			var state string
			if err = db.QueryRow(`SELECT state FROM crawl_request WHERE id=$1`, id).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state == "failed" {
				t.Fatal("crawl failed")
			}
			if state == "completed" {
				var price sql.NullInt64
				var availability string
				if err = db.QueryRow(`SELECT price_cents,availability FROM price_availability_observation WHERE crawl_request_id=$1`, id).Scan(&price, &availability); err != nil || !price.Valid || price.Int64 != wantPrice || availability != wantAvailability {
					t.Fatalf("price=%v availability=%s err=%v", price, availability, err)
				}
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("crawl did not complete")
	}
	runJob("00000000-0000-4000-8000-000000000004", "first", 10000, "available")
	controlURL := fmt.Sprintf("http://localhost:%d/offers/offer-a", controlPort)
	req, err := http.NewRequest(http.MethodPut, controlURL, strings.NewReader(`{"price_cents":12345,"availability":"unavailable"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("control status %d", response.StatusCode)
	}
	runJob("00000000-0000-4000-8000-000000000005", "second", 12345, "unavailable")
}
