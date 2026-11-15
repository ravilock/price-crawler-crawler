package integration

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// orderingDB applies every crawler migration up to (but excluding) the T137
// ordering migration so retained rows can be backfilled deterministically.
func orderingDB(t *testing.T) *sql.DB {
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
	schema := fmt.Sprintf("ordering_test_%d", time.Now().UnixNano())
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
		if strings.Contains(file, "013_observation_ordering") || strings.Contains(file, "014_ranking_facts") {
			continue
		}
		data, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		if _, err = db.Exec(string(data)); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
	}
	return db
}

func applyOrderingMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	data, err := os.ReadFile("../../migrations/013_observation_ordering.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(data)); err != nil {
		t.Fatalf("apply ordering migration: %v", err)
	}
}

func orderingFixture(t *testing.T, db *sql.DB, org, source, offer, request string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,is_active,max_requests_per_minute) VALUES($1,$2,$3,true,10)`, source, org, "store-"+source[len(source)-4:]+".example"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url) VALUES($1,$2,$3,$4)`, offer, org, source, "https://store-"+offer[:8]+".example/item"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,$3,'scheduled','completed',$4)`, request, org, "https://store-"+offer[:8]+".example/item", "key-"+request); err != nil {
		t.Fatal(err)
	}
}

func TestObservationOrderingBackfillIsDeterministic(t *testing.T) {
	db := orderingDB(t)
	org := "00000000-0000-4000-8000-000000000101"
	source := "00000000-0000-4000-8000-000000000102"
	offer := "00000000-0000-4000-8000-000000000103"
	otherOffer := "00000000-0000-4000-8000-000000000104"
	orderingFixture(t, db, org, source, offer, "00000000-0000-4000-8000-000000000105")
	orderingFixture(t, db, org, "00000000-0000-4000-8000-000000000107", otherOffer, "00000000-0000-4000-8000-000000000106")

	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	// Ties resolve by ascending observation id; the later timestamp sorts last.
	rows := []struct {
		id, request, target string
		at                  time.Time
		price               any
		payment             any
	}{
		{"00000000-0000-4000-8000-000000000114", "00000000-0000-4000-8000-000000000114", offer, base.Add(2 * time.Minute), int64(13000), "cash"},
		{"00000000-0000-4000-8000-000000000112", "00000000-0000-4000-8000-000000000112", offer, base, int64(11000), "cash"},
		{"00000000-0000-4000-8000-000000000111", "00000000-0000-4000-8000-000000000111", offer, base, nil, nil},
		{"00000000-0000-4000-8000-000000000113", "00000000-0000-4000-8000-000000000113", offer, base.Add(time.Minute), int64(12000), "installment"},
		{"00000000-0000-4000-8000-000000000121", "00000000-0000-4000-8000-000000000121", otherOffer, base, int64(9000), "cash"},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,'https://store.example/retained','scheduled','completed',$3)`, r.request, org, "key-"+r.request); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id) VALUES($1,$2,$3,$4,$5,'available',$6,$7,$8)`, r.id, org, r.target, r.price, r.payment, r.at, "key-"+r.id, r.request); err != nil {
			t.Fatal(err)
		}
	}

	applyOrderingMigration(t, db)

	expected := []struct {
		id      string
		pos     int
		status  string
		payment bool
	}{
		{"00000000-0000-4000-8000-000000000111", 1, "missing", false},
		{"00000000-0000-4000-8000-000000000112", 2, "valid", true},
		{"00000000-0000-4000-8000-000000000113", 3, "valid", false},
		{"00000000-0000-4000-8000-000000000114", 4, "valid", true},
	}
	for _, want := range expected {
		var pos int
		var status string
		var eligible bool
		if err := db.QueryRow(`SELECT position,price_status,payment_eligible FROM price_availability_observation WHERE id=$1`, want.id).Scan(&pos, &status, &eligible); err != nil {
			t.Fatal(err)
		}
		if pos != want.pos || status != want.status || eligible != want.payment {
			t.Fatalf("observation %s got position=%d status=%s payment_eligible=%t, want %d/%s/%t", want.id, pos, status, eligible, want.pos, want.status, want.payment)
		}
	}
	var otherPosition int
	if err := db.QueryRow(`SELECT position FROM price_availability_observation WHERE offer_id=$1`, otherOffer).Scan(&otherPosition); err != nil || otherPosition != 1 {
		t.Fatalf("other offer position=%d err=%v", otherPosition, err)
	}
}

func TestObservationOrderingAssignsAndProtectsPositions(t *testing.T) {
	db := orderingDB(t)
	applyOrderingMigration(t, db)
	org := "00000000-0000-4000-8000-000000000201"
	source := "00000000-0000-4000-8000-000000000202"
	offer := "00000000-0000-4000-8000-000000000203"
	orderingFixture(t, db, org, source, offer, "00000000-0000-4000-8000-000000000204")
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

	insert := func(id, request string, at time.Time, price any, payment any) error {
		if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,'https://store.example/item','scheduled','completed',$3)`, request, org, "key-"+request); err != nil {
			return err
		}
		_, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id) VALUES($1,$2,$3,$4,$5,'available',$6,$7,$8)`, id, org, offer, price, payment, at, "key-"+id, request)
		return err
	}
	if err := insert("00000000-0000-4000-8000-000000000211", "00000000-0000-4000-8000-000000000211", base, int64(10000), "cash"); err != nil {
		t.Fatal(err)
	}
	if err := insert("00000000-0000-4000-8000-000000000212", "00000000-0000-4000-8000-000000000212", base.Add(time.Minute), nil, nil); err != nil {
		t.Fatal(err)
	}
	// A duplicate request is rejected and must not consume a position.
	if err := insert("00000000-0000-4000-8000-000000000213", "00000000-0000-4000-8000-000000000211", base.Add(2*time.Minute), int64(12000), "cash"); err == nil {
		t.Fatal("expected duplicate request rejection")
	}
	if err := insert("00000000-0000-4000-8000-000000000214", "00000000-0000-4000-8000-000000000214", base.Add(3*time.Minute), int64(11000), "cash"); err != nil {
		t.Fatal(err)
	}
	var position int
	if err := db.QueryRow(`SELECT position FROM price_availability_observation WHERE id=$1`, "00000000-0000-4000-8000-000000000214").Scan(&position); err != nil || position != 3 {
		t.Fatalf("next position=%d err=%v, want 3", position, err)
	}

	if _, err := db.Exec(`UPDATE price_availability_observation SET position=9 WHERE id=$1`, "00000000-0000-4000-8000-000000000211"); err == nil {
		t.Fatal("expected append-only position protection")
	}
	if _, err := db.Exec(`UPDATE price_availability_observation SET price_cents=1 WHERE id=$1`, "00000000-0000-4000-8000-000000000214"); err == nil {
		t.Fatal("expected append-only price protection")
	}
	if _, err := db.Exec(`DELETE FROM price_availability_observation WHERE id=$1`, "00000000-0000-4000-8000-000000000211"); err == nil {
		t.Fatal("expected append-only delete protection")
	}
	if _, err := db.Exec(`UPDATE price_availability_observation SET provenance='{"title":{"raw_value":null}}' WHERE id=$1`, "00000000-0000-4000-8000-000000000212"); err != nil {
		t.Fatalf("evidence redaction must remain allowed: %v", err)
	}

	// Explicit status stays authoritative for non-cash and invalid prices.
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,'https://store.example/item','scheduled','completed',$3)`, "00000000-0000-4000-8000-000000000216", org, "key-216"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,price_status) VALUES($1,$2,$3,NULL,NULL,'unknown',$4,'key-216','00000000-0000-4000-8000-000000000216','invalid')`, "00000000-0000-4000-8000-000000000215", org, offer, base.Add(4*time.Minute)); err != nil {
		t.Fatalf("explicit invalid status: %v", err)
	}
	var status string
	var eligible bool
	if err := db.QueryRow(`SELECT price_status,payment_eligible FROM price_availability_observation WHERE id=$1`, "00000000-0000-4000-8000-000000000215").Scan(&status, &eligible); err != nil || status != "invalid" || eligible {
		t.Fatalf("status=%s eligible=%t err=%v", status, eligible, err)
	}
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,'https://store.example/item','scheduled','completed',$3)`, "00000000-0000-4000-8000-000000000218", org, "key-218"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,price_status) VALUES($1,$2,$3,NULL,'cash','available',$4,'key-218','00000000-0000-4000-8000-000000000218','valid')`, "00000000-0000-4000-8000-000000000217", org, offer, base.Add(5*time.Minute)); err == nil {
		t.Fatal("expected valid status to require a price")
	}
}
