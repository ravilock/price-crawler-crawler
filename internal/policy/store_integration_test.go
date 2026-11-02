package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/scheduler"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// Run against a fresh disposable database, never a retained application volume.
func TestPolicyStoreIntegration(t *testing.T) {
	dsn := os.Getenv("POLICY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set POLICY_TEST_DATABASE_URL to a fresh disposable database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	for _, f := range []string{"../../migrations/001_initial.up.sql", "../../migrations/002_message_delivery.up.sql", "../../migrations/003_collection.up.sql", "../../migrations/004_source_admin.up.sql"} {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, string(data)); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	org := "00000000-0000-4000-8000-000000000001"
	src := "00000000-0000-4000-8000-000000000002"
	for _, statement := range []string{
		`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,crawl_delay_seconds,adapter_name,approved_categories) VALUES($1,$2,'example.com',true,30,3,'fixture','["parts"]')`,
		`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($2,$1,'["parts"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}')`,
		`INSERT INTO source_path_override(id,organization_id,source_domain_id,path_pattern,actor_id,reason) VALUES('00000000-0000-4000-8000-000000000003',$2,$1,'/item','admin','reviewed')`,
	} {
		if _, err = db.ExecContext(ctx, statement, src, org); err != nil {
			t.Fatal(err)
		}
	}

	source, overrides, err := LoadSource(ctx, db, org, src, 2)
	if err != nil || !source.Active || !source.AccessReviewed || !source.TermsReviewed || !source.IntegrationSupported || len(overrides) != 1 || source.CrawlDelay != 3*time.Second {
		t.Fatalf("source=%+v overrides=%+v err=%v", source, overrides, err)
	}
	if _, _, err = LoadSource(ctx, db, "00000000-0000-4000-8000-000000000099", src, 2); !errors.Is(err, ErrDenied) {
		t.Fatalf("tenant isolation: %v", err)
	}
	now := time.Now().UTC()
	record := RobotsRecord{Status: "unreadable", CheckedAt: now, ExpiresAt: now.Add(time.Hour), AlertAdministrator: true}
	recorder := SQLRecorder{DB: db, WriteAlert: func(context.Context, *sql.Tx, string, string, RobotsRecord) error { return errors.New("alert failed") }}
	if err = recorder.RecordRobots(ctx, org, src, record); err == nil {
		t.Fatal("expected alert failure")
	}
	var status sql.NullString
	if err = db.QueryRowContext(ctx, `SELECT robots_status FROM source_domain_config WHERE id=$1`, src).Scan(&status); err != nil || status.Valid {
		t.Fatalf("snapshot committed despite failed alert: %v %v", status, err)
	}
	recorder.WriteAlert = WriteRobotsAudit
	if err = recorder.RecordRobots(ctx, org, src, record); err != nil {
		t.Fatal(err)
	}
	var audits, outbox int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM source_admin_audit WHERE organization_id=$1 AND action='source.robots.unreadable'`, org).Scan(&audits); err != nil || audits != 1 {
		t.Fatalf("audit count=%d err=%v", audits, err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM message_outbox WHERE organization_id=$1 AND message_type='audit.event.recorded'`, org).Scan(&outbox); err != nil || outbox != 1 {
		t.Fatalf("outbox count=%d err=%v", outbox, err)
	}
	called := false
	recorder.WriteAlert = func(_ context.Context, _ *sql.Tx, gotOrg, gotSrc string, _ RobotsRecord) error {
		called = gotOrg == org && gotSrc == src
		return nil
	}
	if err = recorder.RecordRobots(ctx, org, src, record); err != nil || !called {
		t.Fatalf("record: called=%v err=%v", called, err)
	}
	if err = db.QueryRowContext(ctx, `SELECT robots_status FROM source_domain_config WHERE id=$1`, src).Scan(&status); err != nil || status.String != "unreadable" {
		t.Fatalf("status: %v %v", status, err)
	}
}

func TestAuthorizedFetchWithRobotsAndNotifications(t *testing.T) {
	dsn := os.Getenv("POLICY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set POLICY_TEST_DATABASE_URL to a fresh disposable database")
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
	var unavailable atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			if unavailable.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /denied"))
			return
		}
		_, _ = w.Write([]byte("fixture"))
	}), ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()
	origin := fmt.Sprintf("http://localhost:%d", listener.Addr().(*net.TCPAddr).Port)
	t.Setenv("MOCK_ENV", "test")
	t.Setenv("MOCK_SOURCE_ORIGIN", origin)
	t.Setenv("ROBOTS_SLOW_REQUESTS_PER_MINUTE", "60000")
	org := "00000000-0000-4000-8000-000000000001"
	src := "00000000-0000-4000-8000-000000000002"
	job := "00000000-0000-4000-8000-000000000003"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,adapter_name,approved_categories) VALUES($1,$2,'localhost',true,60000,'fixture','["parts"]')`, src, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["parts"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}')`, org, src); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,source_domain_id,category) VALUES($1,$2,$3,'manual','rendering','test-key',$4,'parts')`, job, org, origin+"/product", src); err != nil {
		t.Fatal(err)
	}
	invoke := func(path, organization string) fetchReply {
		t.Helper()
		payload, _ := json.Marshal(fetchCommand{CrawlRequestID: job, SourceID: src, URL: origin + path, MaxBytes: 1024, DeadlineMS: 5000})
		e := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000004", MessageType: "network.fetch.requested", SchemaVersion: 1, OccurredAt: time.Now(), Organization: organization, CorrelationID: "00000000-0000-4000-8000-000000000005", Payload: payload}
		reply, err := ExecuteFetch(context.Background(), db, scheduler.NewBudget(2), e)
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	if reply := invoke("/product", org); reply.Status != "ok" {
		t.Fatalf("allowed: %+v", reply)
	}
	if reply := invoke("/denied", org); reply.Status != "blocked" {
		t.Fatalf("disallowed: %+v", reply)
	}
	if reply := invoke("/product", "00000000-0000-4000-8000-000000000099"); reply.Status != "blocked" {
		t.Fatalf("foreign tenant: %+v", reply)
	}
	if _, err = db.Exec(`INSERT INTO source_path_override(id,organization_id,source_domain_id,path_pattern,actor_id,reason) VALUES('00000000-0000-4000-8000-000000000006',$1,$2,'/denied','admin','reviewed')`, org, src); err != nil {
		t.Fatal(err)
	}
	if reply := invoke("/denied", org); reply.Status != "ok" {
		t.Fatalf("override: %+v", reply)
	}
	unavailable.Store(true)
	robotsCache.Lock()
	delete(robotsCache.values, org+"/"+src+"/"+origin)
	robotsCache.Unlock()
	if reply := invoke("/product", org); reply.Status != "ok" {
		t.Fatalf("unreadable robots should collect at slow rate: %+v", reply)
	}
	notifications, err := ReadNotifications(context.Background(), db, org, 50)
	if err != nil || len(notifications) != 1 || notifications[0].SourceID != src {
		t.Fatalf("notifications=%+v err=%v", notifications, err)
	}
}
