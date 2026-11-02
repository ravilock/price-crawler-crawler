package integration

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/scheduler"
)

func TestApprovedSitemapDiscoveryPersistsScopedDeduplicatedIntents(t *testing.T) {
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
	org := "00000000-0000-4000-8000-000000000101"
	source := "00000000-0000-4000-8000-000000000102"
	if _, err = db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,adapter_name,approved_categories) VALUES($1,$2,'store.example',true,5,'fixture','["components"]')
		ON CONFLICT (id) DO NOTHING`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state,review) VALUES($1,$2,'["components"]','admin','approved','{"access_rules_reviewed":true,"terms_reviewed":true,"supported_integration":"fixture"}') ON CONFLICT DO NOTHING`, org, source); err != nil {
		t.Fatal(err)
	}
	body := `<urlset><url><loc>https://store.example/a?utm_source=demo&amp;color=red</loc></url><url><loc>/a?color=red</loc></url><url><loc>https://other.example/outside</loc></url><url><loc>https://store.example/b</loc></url></urlset>`
	got, err := scheduler.SubmitSitemap(context.Background(), db, org, source, "components", "sitemap-run", "00000000-0000-4000-8000-000000000103", strings.NewReader(body))
	if err != nil || len(got) != 2 {
		t.Fatalf("receipts=%+v err=%v", got, err)
	}
	replay, err := scheduler.SubmitSitemap(context.Background(), db, org, source, "components", "sitemap-run", "00000000-0000-4000-8000-000000000103", strings.NewReader(body))
	if err != nil || len(replay) != 2 || replay[0].Created || replay[1].Created {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM crawl_request WHERE organization_id=$1 AND reason='discovery' AND source_domain_id=$2`, org, source).Scan(&count); err != nil || count != 2 {
		t.Fatalf("discovery requests=%d err=%v", count, err)
	}
	if _, err = scheduler.SubmitSitemap(context.Background(), db, org, source, "unapproved", "other-run", "", strings.NewReader(body)); err == nil {
		t.Fatal("unapproved category accepted")
	}
}
