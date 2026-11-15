package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"price-crawler/services/crawler/internal/results"
	"price-crawler/services/crawler/internal/scheduler"
)

// rankingDB applies the retained crawler schema in an isolated PostgreSQL
// schema. includeLatest=false stops before the T158 revision migration so the
// terminal-outcome backfill can be exercised against retained rows.
func rankingDB(t *testing.T, includeLatest bool) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CRAWLER_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("CRAWLER_TEST_DATABASE_URL is required in CI")
		}
		t.Skip("set CRAWLER_TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	schema := fmt.Sprintf("ranking_test_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, e := db.Exec(`DROP SCHEMA "` + schema + `" CASCADE`); e != nil {
			t.Error(e)
		}
		_ = db.Close()
	})
	if _, err = db.Exec(`SET search_path TO "` + schema + `"`); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	for _, file := range files {
		if !includeLatest && strings.Contains(file, "014_ranking_facts") {
			continue
		}
		data, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec(string(data)); e != nil {
			t.Fatalf("%s: %v", file, e)
		}
	}
	return db
}

func seedRankingSource(t *testing.T, db *sql.DB, org, source, offer string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute,freshness_window_seconds,approved_categories) VALUES($1,$2,$3,true,10,86400,'["parts"]')`, source, org, "store-"+source[len(source)-4:]+".example"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url) VALUES($1,$2,$3,$4)`, offer, org, source, "https://store-"+offer[:8]+".example/item"); err != nil {
		t.Fatal(err)
	}
}

func seedRankingRequest(t *testing.T, db *sql.DB, org, offer, request, state, outcome string, at time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,terminal_outcome,completed_at,updated_at,offer_id) VALUES($1,$2,'https://store.example/item','scheduled',$3,$4,NULLIF($5,''),$6,$6,$7)`, request, org, state, "key-"+request, outcome, at, offer); err != nil {
		t.Fatal(err)
	}
}

func seedRankingObservation(t *testing.T, db *sql.DB, org, offer, observation, request string, price any, payment any, status any, at time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,price_status) VALUES($1,$2,$3,$4,$5,'available',$6,$7,$8,$9)`, observation, org, offer, price, payment, at, "key-"+observation, request, status); err != nil {
		t.Fatal(err)
	}
}

func TestRankingFactsReadIsTenantScopedAndOrdered(t *testing.T) {
	db := rankingDB(t, true)
	org := "00000000-0000-4000-8000-000000000301"
	foreignOrg := "00000000-0000-4000-8000-000000000302"
	source := "00000000-0000-4000-8000-000000000303"
	offer := "00000000-0000-4000-8000-000000000304"
	foreignOffer := "00000000-0000-4000-8000-000000000305"
	seedRankingSource(t, db, org, source, offer)
	seedRankingSource(t, db, foreignOrg, "00000000-0000-4000-8000-000000000306", foreignOffer)

	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000311", "completed", "partial", base)
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000312", "completed", "complete", base.Add(time.Minute))
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000313", "completed", "complete", base.Add(2*time.Minute))
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000314", "failed", "failed", base.Add(3*time.Minute))

	seedRankingObservation(t, db, org, offer, "00000000-0000-4000-8000-000000000321", "00000000-0000-4000-8000-000000000311", int64(12000), "cash", nil, base)
	seedRankingObservation(t, db, org, offer, "00000000-0000-4000-8000-000000000322", "00000000-0000-4000-8000-000000000312", nil, nil, "invalid", base.Add(time.Minute))
	seedRankingObservation(t, db, org, offer, "00000000-0000-4000-8000-000000000323", "00000000-0000-4000-8000-000000000313", int64(13000), "cash", nil, base.Add(2*time.Minute))

	read := results.ReadRankingFacts(context.Background(), db, org, offer)
	if read.Status != "ok" || read.Facts == nil {
		t.Fatalf("authorized read status=%s", read.Status)
	}
	f := read.Facts
	if f.SourceID != source || f.OfferRevision != 1 || f.SourceRevision != 1 || f.FreshnessWindowSeconds != 86400 {
		t.Fatalf("facts identity or revisions: %+v", f)
	}
	if f.LatestCollection == nil || f.LatestCollection.CrawlRequestID != "00000000-0000-4000-8000-000000000314" || f.LatestCollection.Outcome != "failed" {
		t.Fatalf("latest terminal collection must be the failure: %+v", f.LatestCollection)
	}
	if f.LatestObservation == nil || f.LatestObservation.ObservationID != "00000000-0000-4000-8000-000000000323" || f.LatestObservation.Position != 3 {
		t.Fatalf("latest observation must use the T137 position: %+v", f.LatestObservation)
	}
	if f.LastVerifiedCashPrice == nil || f.LastVerifiedCashPrice.ObservationID != "00000000-0000-4000-8000-000000000323" || f.LastVerifiedCashPrice.PriceCents != 13000 || f.LastVerifiedCashPrice.Currency != "BRL" {
		t.Fatalf("verified cash price: %+v", f.LastVerifiedCashPrice)
	}
	if got := f.Payload()["latest_collection"].(map[string]any)["outcome"]; got != "failed" {
		t.Fatalf("payload outcome = %v", got)
	}

	if read = results.ReadRankingFacts(context.Background(), db, foreignOrg, offer); read.Status != "forbidden" {
		t.Fatalf("cross-tenant read status=%s", read.Status)
	}
	if read = results.ReadRankingFacts(context.Background(), db, org, foreignOffer); read.Status != "forbidden" {
		t.Fatalf("cross-tenant offer read status=%s", read.Status)
	}
	if read = results.ReadRankingFacts(context.Background(), db, org, "00000000-0000-4000-8000-000000000399"); read.Status != "not_found" {
		t.Fatalf("missing offer read status=%s", read.Status)
	}
	if read = results.ReadRankingFacts(context.Background(), db, org, "not-a-uuid"); read.Status != "invalid" {
		t.Fatalf("malformed offer read status=%s", read.Status)
	}
}

func TestRankingFactsPreserveOlderVerifiedPriceAfterInvalidObservation(t *testing.T) {
	db := rankingDB(t, true)
	org := "00000000-0000-4000-8000-000000000331"
	source := "00000000-0000-4000-8000-000000000332"
	offer := "00000000-0000-4000-8000-000000000333"
	seedRankingSource(t, db, org, source, offer)
	base := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000334", "completed", "complete", base)
	seedRankingRequest(t, db, org, offer, "00000000-0000-4000-8000-000000000335", "completed", "complete", base.Add(time.Minute))
	seedRankingObservation(t, db, org, offer, "00000000-0000-4000-8000-000000000336", "00000000-0000-4000-8000-000000000334", int64(9900), "cash", nil, base)
	seedRankingObservation(t, db, org, offer, "00000000-0000-4000-8000-000000000337", "00000000-0000-4000-8000-000000000335", nil, nil, "missing", base.Add(time.Minute))

	read := results.ReadRankingFacts(context.Background(), db, org, offer)
	if read.Status != "ok" {
		t.Fatalf("status=%s", read.Status)
	}
	f := read.Facts
	if f.LatestObservation == nil || f.LatestObservation.Position != 2 || f.LatestObservation.Availability != "available" {
		t.Fatalf("latest observation: %+v", f.LatestObservation)
	}
	if f.LastVerifiedCashPrice == nil || f.LastVerifiedCashPrice.Position != 1 || f.LastVerifiedCashPrice.PriceCents != 9900 {
		t.Fatalf("verified price must survive a newer invalid observation: %+v", f.LastVerifiedCashPrice)
	}
}

func TestRankingRevisionsBumpOnlyOnRankingChanges(t *testing.T) {
	db := rankingDB(t, true)
	org := "00000000-0000-4000-8000-000000000341"
	source := "00000000-0000-4000-8000-000000000342"
	offer := "00000000-0000-4000-8000-000000000343"
	seedRankingSource(t, db, org, source, offer)

	offerRevision := func() int64 {
		var revision int64
		if err := db.QueryRow(`SELECT offer_revision FROM offer WHERE organization_id=$1 AND id=$2`, org, offer).Scan(&revision); err != nil {
			t.Fatal(err)
		}
		return revision
	}
	sourceRevision := func() int64 {
		var revision int64
		if err := db.QueryRow(`SELECT source_revision FROM source_domain_config WHERE organization_id=$1 AND id=$2`, org, source).Scan(&revision); err != nil {
			t.Fatal(err)
		}
		return revision
	}

	if got := offerRevision(); got != 1 {
		t.Fatalf("new offer revision = %d", got)
	}
	if _, err := db.Exec(`UPDATE offer SET current_price_cents=15000,payment_method='cash',availability='available' WHERE organization_id=$1 AND id=$2`, org, offer); err != nil {
		t.Fatal(err)
	}
	if got := offerRevision(); got != 2 {
		t.Fatalf("offer revision after price change = %d", got)
	}
	if _, err := db.Exec(`UPDATE offer SET current_price_cents=15000 WHERE organization_id=$1 AND id=$2`, org, offer); err != nil {
		t.Fatal(err)
	}
	if got := offerRevision(); got != 2 {
		t.Fatalf("idempotent offer write advanced revision to %d", got)
	}

	if got := sourceRevision(); got != 1 {
		t.Fatalf("new source revision = %d", got)
	}
	if _, err := db.Exec(`UPDATE source_domain_config SET freshness_window_seconds=43200 WHERE organization_id=$1 AND id=$2`, org, source); err != nil {
		t.Fatal(err)
	}
	if got := sourceRevision(); got != 2 {
		t.Fatalf("source revision after window change = %d", got)
	}
	if _, err := db.Exec(`UPDATE source_domain_config SET freshness_window_seconds=43200 WHERE organization_id=$1 AND id=$2`, org, source); err != nil {
		t.Fatal(err)
	}
	if got := sourceRevision(); got != 2 {
		t.Fatalf("idempotent source write advanced revision to %d", got)
	}
	if _, err := db.Exec(`UPDATE source_domain_config SET is_active=false WHERE organization_id=$1 AND id=$2`, org, source); err != nil {
		t.Fatal(err)
	}
	if got := sourceRevision(); got != 3 {
		t.Fatalf("source revision after deactivation = %d", got)
	}

	// A failed terminal collection advances offer state even without an
	// observation, while a duplicate terminal write must not advance it again.
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,offer_id) VALUES('00000000-0000-4000-8000-000000000344',$1,'https://store.example/item','scheduled','queued','key-344',$2)`, org, offer); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE crawl_request SET state='failed',terminal_outcome='failed',completed_at=now() WHERE organization_id=$1 AND id='00000000-0000-4000-8000-000000000344'`, org); err != nil {
		t.Fatal(err)
	}
	if got := offerRevision(); got != 3 {
		t.Fatalf("offer revision after failed terminal collection = %d", got)
	}
	if _, err := db.Exec(`UPDATE crawl_request SET state='failed',terminal_outcome='failed' WHERE organization_id=$1 AND id='00000000-0000-4000-8000-000000000344'`, org); err != nil {
		t.Fatal(err)
	}
	if got := offerRevision(); got != 3 {
		t.Fatalf("duplicate terminal write advanced offer revision to %d", got)
	}
}

func TestRankingFactsMigrationBackfillsTerminalOutcome(t *testing.T) {
	db := rankingDB(t, false)
	org := "00000000-0000-4000-8000-000000000351"
	source := "00000000-0000-4000-8000-000000000352"
	offer := "00000000-0000-4000-8000-000000000353"
	seedRankingSource(t, db, org, source, offer)
	base := time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)
	// Retained terminal requests have no recorded outcome and the failed one has
	// no completion time; the migration must derive both deterministically.
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,completed_at,updated_at,offer_id) VALUES
		('00000000-0000-4000-8000-000000000354',$1,'https://store.example/item','scheduled','completed','key-354',$2,$2,$4),
		('00000000-0000-4000-8000-000000000355',$1,'https://store.example/item','scheduled','failed','key-355',NULL,$3,$4),
		('00000000-0000-4000-8000-000000000356',$1,'https://store.example/item','scheduled','paused','key-356',NULL,$3,$4)`, org, base, base.Add(time.Hour), offer); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id) VALUES('00000000-0000-4000-8000-000000000357',$1,$2,12500,'cash','available',$3,'key-357','00000000-0000-4000-8000-000000000354')`, org, offer, base); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../migrations/014_ranking_facts.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(data)); err != nil {
		t.Fatalf("apply ranking migration: %v", err)
	}

	var completedOutcome, failedOutcome, pausedOutcome string
	var failedCompleted, pausedCompleted time.Time
	if err = db.QueryRow(`SELECT terminal_outcome,completed_at FROM crawl_request WHERE id='00000000-0000-4000-8000-000000000354'`).Scan(&completedOutcome, &failedCompleted); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT terminal_outcome,completed_at FROM crawl_request WHERE id='00000000-0000-4000-8000-000000000355'`).Scan(&failedOutcome, &failedCompleted); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT terminal_outcome,completed_at FROM crawl_request WHERE id='00000000-0000-4000-8000-000000000356'`).Scan(&pausedOutcome, &pausedCompleted); err != nil {
		t.Fatal(err)
	}
	if completedOutcome != "partial" || failedOutcome != "failed" || pausedOutcome != "paused" {
		t.Fatalf("backfilled outcomes = %q %q %q", completedOutcome, failedOutcome, pausedOutcome)
	}
	if failedCompleted.IsZero() || pausedCompleted.IsZero() {
		t.Fatalf("terminal completion times were not backfilled: failed=%v paused=%v", failedCompleted, pausedCompleted)
	}
	var offerRevision, sourceRevision int64
	if err = db.QueryRow(`SELECT o.offer_revision,s.source_revision FROM offer o JOIN source_domain_config s ON s.organization_id=o.organization_id AND s.id=o.source_domain_id WHERE o.id=$1`, offer).Scan(&offerRevision, &sourceRevision); err != nil {
		t.Fatal(err)
	}
	if offerRevision != 1 || sourceRevision != 1 {
		t.Fatalf("backfilled revisions offer=%d source=%d", offerRevision, sourceRevision)
	}
	read := results.ReadRankingFacts(context.Background(), db, org, offer)
	if read.Status != "ok" || read.Facts.LatestCollection == nil || read.Facts.LatestCollection.Outcome != "paused" {
		t.Fatalf("read after backfill status=%s facts=%+v", read.Status, read.Facts)
	}
	if read.Facts.LastVerifiedCashPrice == nil || read.Facts.LastVerifiedCashPrice.PriceCents != 12500 {
		t.Fatalf("verified price after backfill: %+v", read.Facts.LastVerifiedCashPrice)
	}
}

func TestScheduledRefreshFailureAdvancesOfferRevision(t *testing.T) {
	db := rankingDB(t, true)
	org := "00000000-0000-4000-8000-000000000361"
	source := "00000000-0000-4000-8000-000000000362"
	offer := "00000000-0000-4000-8000-000000000363"
	seedRankingSource(t, db, org, source, offer)
	request := "00000000-0000-4000-8000-000000000364"
	receipt, err := scheduler.Submit(context.Background(), db, scheduler.Intent{
		ID:             request,
		Organization:   org,
		URL:            "https://store-" + offer[:8] + ".example/item",
		Reason:         "scheduled",
		IdempotencyKey: "refresh-" + request,
		OfferID:        offer,
	})
	if err != nil || receipt.ID != request || !receipt.Created {
		t.Fatalf("submit refresh intent: receipt=%+v err=%v", receipt, err)
	}
	var storedOffer string
	if err = db.QueryRow(`SELECT COALESCE(offer_id::text,'') FROM crawl_request WHERE organization_id=$1 AND id=$2`, org, request).Scan(&storedOffer); err != nil || storedOffer != offer {
		t.Fatalf("scheduled refresh must retain its offer: offer=%q err=%v", storedOffer, err)
	}
	if _, err = db.Exec(`UPDATE crawl_request SET state='fetching',lease_token='00000000-0000-4000-8000-000000000365',updated_at=now() WHERE organization_id=$1 AND id=$2`, org, request); err != nil {
		t.Fatal(err)
	}
	retried, err := scheduler.ScheduleRetry(context.Background(), db, org, request, "store-"+offer[:8]+".example", "blocked", "", time.Now().UTC(), 0, "00000000-0000-4000-8000-000000000365")
	if err != nil || retried {
		t.Fatalf("terminal failure: retried=%t err=%v", retried, err)
	}
	var revision int64
	if err = db.QueryRow(`SELECT offer_revision FROM offer WHERE organization_id=$1 AND id=$2`, org, offer).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if revision != 2 {
		t.Fatalf("failed refresh revision = %d", revision)
	}
	read := results.ReadRankingFacts(context.Background(), db, org, offer)
	if read.Status != "ok" || read.Facts.LatestCollection == nil || read.Facts.LatestCollection.Outcome != "failed" {
		t.Fatalf("failed refresh facts status=%s facts=%+v", read.Status, read.Facts)
	}
	if read.Facts.LatestObservation != nil || read.Facts.LastVerifiedCashPrice != nil {
		t.Fatalf("failure must not invent observations: %+v", read.Facts)
	}
}
