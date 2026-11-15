package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"price-crawler/services/crawler/internal/collection"
	"price-crawler/services/crawler/internal/messaging"
)

func retentionDB(t *testing.T, beforeUpgrade bool) *sql.DB {
	t.Helper()
	dsn := os.Getenv("T162_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("T162_TEST_DATABASE_URL is required in CI")
		}
		t.Skip("set T162_TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	schema := fmt.Sprintf("retention_test_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, e := db.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		if e != nil {
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
		if beforeUpgrade && strings.HasSuffix(file, "012_evidence_retention.up.sql") {
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

func TestEvidenceRetentionRedactsAndRecovers(t *testing.T) {
	db := retentionDB(t, false)
	org := "10000000-0000-4000-8000-000000000001"
	source := "20000000-0000-4000-8000-000000000001"
	req := "30000000-0000-4000-8000-000000000001"
	page := "40000000-0000-4000-8000-000000000001"
	offer := "50000000-0000-4000-8000-000000000001"
	candidate := "60000000-0000-4000-8000-000000000001"
	msg := "70000000-0000-4000-8000-000000000001"
	obs := "80000000-0000-4000-8000-000000000001"
	now := time.Now().UTC().Truncate(time.Microsecond)
	observed := now.Add(-time.Hour)
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO source_domain_config(id,organization_id,domain,max_requests_per_minute) VALUES($1,$2,'example.com',60)`, []any{source, org}},
		{`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,source_domain_id,category) VALUES($1,$2,'https://example.com/a','manual','finding','key',$3,'parts')`, []any{req, org, source}},
		{`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,evidence_hash,bounded_evidence,evidence_expires_at) VALUES($1,$2,$3,$4,'https://example.com/a','http',$5,'partial','hash','raw page',$6)`, []any{page, org, req, source, observed, now.Add(-time.Minute)}},
		{`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url) VALUES($1,$2,$3,'https://example.com/a')`, []any{offer, org, source}},
		{`INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,attributes,field_provenance,normalization_version) VALUES($1,$2,$3,$4,'Widget','[{"name":"variant","value":"blue","field":{"raw_value":"secret variant","normalized_value":"blue"}}]','{"title":{"raw_value":"secret title","normalized_value":"Widget","status":"valid"}}','1')`, []any{candidate, org, req, page}},
		{`INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,provenance) VALUES($1,$2,$3,100,'cash','available',$4,'observation',$5,'{"title":{"raw_value":"secret observation","normalized_value":"Widget"}}')`, []any{obs, org, offer, observed, req}},
		{`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,payload,claimed_until) VALUES($1,'product.find.requested.v1','product.find.requested',$2,$1,jsonb_build_object('payload',jsonb_build_object('crawl_request_id',$3::text,'page_parsed_message_id',$4::text,'page',jsonb_build_object('title','secret page'))),now()+interval '1 minute')`, []any{msg, org, req, page}},
	}
	for _, st := range statements {
		if _, err := db.Exec(st.query, st.args...); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := collection.ExpireEvidence(context.Background(), db, observed, 10); err != nil || n != 0 {
		t.Fatalf("premature expiry: count=%d err=%v", n, err)
	}
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(os.Getenv("T162_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	reader, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	locked, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = locked.Exec(`SELECT id FROM crawl_request WHERE id=$1 FOR UPDATE`, req); err != nil {
		t.Fatal(err)
	}
	if n, err := collection.ExpireEvidence(context.Background(), reader, now, 10); err != nil || n != 0 {
		_ = locked.Rollback()
		t.Fatalf("locked request was not skipped: count=%d err=%v", n, err)
	}
	if err = locked.Commit(); err != nil {
		t.Fatal(err)
	}
	n, err := collection.ExpireEvidence(context.Background(), db, now, 10)
	if err != nil || n != 1 {
		t.Fatalf("expired=%d err=%v", n, err)
	}
	var fields, attrs, provenance, payload, state string
	var deadline, expired time.Time
	var dead sql.NullTime
	err = db.QueryRow(`SELECT c.field_provenance::text,c.attributes::text,o.provenance::text,m.payload::text,r.state,p.raw_evidence_deadline,p.raw_evidence_expired_at,m.dead_lettered_at FROM product_information_candidate c JOIN page_fetch_outcome p ON p.id=c.page_fetch_outcome_id JOIN crawl_request r ON r.id=c.crawl_request_id JOIN price_availability_observation o ON o.crawl_request_id=r.id JOIN message_outbox m ON m.message_id=$2 WHERE c.id=$1`, candidate, msg).Scan(&fields, &attrs, &provenance, &payload, &state, &deadline, &expired, &dead)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{fields, attrs, provenance, payload} {
		if strings.Contains(v, "secret") {
			t.Fatalf("raw evidence remains: %s", v)
		}
	}
	if !strings.Contains(fields, "Widget") || !strings.Contains(attrs, "blue") || state != "queued" || !dead.Valid || !deadline.Before(expired) {
		t.Fatalf("lost normalized data or recovery: fields=%s attrs=%s state=%s deadline=%v expired=%v dead=%v", fields, attrs, state, deadline, expired, dead)
	}
	if _, err = db.Exec(`UPDATE product_information_candidate SET attributes='[]' WHERE id=$1`, candidate); err == nil {
		t.Fatal("identity mutation accepted")
	}
	// Simulate an old finder reply arriving after a recovery attempt starts.
	if _, err = db.Exec(`UPDATE crawl_request SET state='finding' WHERE id=$1`, req); err != nil {
		t.Fatal(err)
	}
	finder := messaging.Envelope{MessageID: "90000000-0000-4000-8000-000000000001", MessageType: "product.information.found", SchemaVersion: 1, OccurredAt: now, Organization: org, CorrelationID: msg, CausationID: msg, Payload: []byte(fmt.Sprintf(`{"crawl_request_id":%q,"normalization_version":"1","fields":{},"attributes":[],"offer":{"availability":"unknown","price_status":"missing"}}`, req))}
	if err = collection.ApplyFinder(context.Background(), db, finder); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT state FROM crawl_request WHERE id=$1`, req).Scan(&state); err != nil || state != "queued" {
		t.Fatalf("late reply recovery state=%s err=%v", state, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM product_information_candidate WHERE crawl_request_id=$1`, req).Scan(&count); err != nil || count != 1 {
		t.Fatalf("late reply changed candidates count=%d err=%v", count, err)
	}
	n, err = collection.ExpireEvidence(context.Background(), db, now, 10)
	if err != nil || n != 0 {
		t.Fatalf("replay expired=%d err=%v", n, err)
	}
}

func TestEvidenceRetentionUpgradeTreatsMissingDeadlinesAsExpired(t *testing.T) {
	db := retentionDB(t, true)
	org := "10000000-0000-4000-8000-000000000002"
	source := "20000000-0000-4000-8000-000000000002"
	req := "30000000-0000-4000-8000-000000000002"
	page := "40000000-0000-4000-8000-000000000002"
	candidate := "60000000-0000-4000-8000-000000000002"
	for _, st := range []struct {
		q string
		a []any
	}{
		{`INSERT INTO source_domain_config(id,organization_id,domain,max_requests_per_minute) VALUES($1,$2,'example.com',60)`, []any{source, org}},
		{`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,terminal_outcome) VALUES($1,$2,'https://example.com/a','manual','completed','key','partial')`, []any{req, org}},
		{`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome) VALUES($1,$2,$3,$4,'https://example.com/a','rendered',now(),'partial')`, []any{page, org, req, source}},
		{`INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,normalization_version,field_provenance) VALUES($1,$2,$3,$4,'Widget','1','{"title":{"raw_value":"old secret","normalized_value":"Widget"}}')`, []any{candidate, org, req, page}},
	} {
		if _, err := db.Exec(st.q, st.a...); err != nil {
			t.Fatal(err)
		}
	}
	migration, err := os.ReadFile("../../migrations/012_evidence_retention.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}
	n, err := collection.ExpireEvidence(context.Background(), db, time.Now().UTC().Add(time.Second), 10)
	if err != nil || n != 1 {
		t.Fatalf("legacy expiry count=%d err=%v", n, err)
	}
	var value string
	if err = db.QueryRow(`SELECT field_provenance::text FROM product_information_candidate WHERE id=$1`, candidate).Scan(&value); err != nil || strings.Contains(value, "old secret") || !strings.Contains(value, "Widget") {
		t.Fatalf("legacy evidence %s err=%v", value, err)
	}
}

func TestEvidenceRetentionDoesNotRequeueNewerFinderAttempt(t *testing.T) {
	db := retentionDB(t, false)
	org := "10000000-0000-4000-8000-000000000003"
	source := "20000000-0000-4000-8000-000000000003"
	req := "30000000-0000-4000-8000-000000000003"
	oldPage := "40000000-0000-4000-8000-000000000003"
	newPage := "40000000-0000-4000-8000-000000000004"
	oldMsg := "70000000-0000-4000-8000-000000000003"
	newMsg := "70000000-0000-4000-8000-000000000004"
	offer := "50000000-0000-4000-8000-000000000003"
	candidate := "60000000-0000-4000-8000-000000000003"
	observation := "80000000-0000-4000-8000-000000000003"
	now := time.Now().UTC()
	for _, st := range []struct {
		q string
		a []any
	}{
		{`INSERT INTO source_domain_config(id,organization_id,domain,max_requests_per_minute) VALUES($1,$2,'example.com',60)`, []any{source, org}},
		{`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key) VALUES($1,$2,'https://example.com/a','manual','finding','key')`, []any{req, org}},
		{`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,raw_evidence_deadline) VALUES($1,$2,$3,$4,'https://example.com/a','http',$5,'partial',$6)`, []any{oldPage, org, req, source, now.Add(-time.Hour), now.Add(-time.Minute)}},
		{`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,raw_evidence_deadline) VALUES($1,$2,$3,$4,'https://example.com/a','http',$5,'partial',$6)`, []any{newPage, org, req, source, now, now.Add(time.Hour)}},
		{`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,created_at,payload) VALUES($1,'product.find.requested.v1','product.find.requested',$2,$1,$5,jsonb_build_object('payload',jsonb_build_object('crawl_request_id',$3::text,'page_parsed_message_id',$4::text,'page',jsonb_build_object('title','raw'))))`, []any{oldMsg, org, req, oldPage, now.Add(-time.Hour)}},
		{`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,created_at,payload) VALUES($1,'product.find.requested.v1','product.find.requested',$2,$1,$5,jsonb_build_object('payload',jsonb_build_object('crawl_request_id',$3::text,'page_parsed_message_id',$4::text,'page',jsonb_build_object('title','raw'))))`, []any{newMsg, org, req, newPage, now}},
		{`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url) VALUES($1,$2,$3,'https://example.com/a')`, []any{offer, org, source}},
		{`INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,normalization_version) VALUES($1,$2,$3,$4,'Widget','1')`, []any{candidate, org, req, newPage}},
		{`INSERT INTO price_availability_observation(id,organization_id,offer_id,availability,observed_at,idempotency_key,crawl_request_id,provenance) VALUES($1,$2,$3,'available',$4,'observation',$5,'{"title":{"raw_value":"new raw"}}')`, []any{observation, org, offer, now, req}},
	} {
		if _, err := db.Exec(st.q, st.a...); err != nil {
			t.Fatal(err)
		}
	}
	n, err := collection.ExpireEvidence(context.Background(), db, now, 10)
	if err != nil || n != 1 {
		t.Fatalf("expiry count=%d err=%v", n, err)
	}
	var state, payload string
	if err = db.QueryRow(`SELECT state FROM crawl_request WHERE id=$1`, req).Scan(&state); err != nil || state != "finding" {
		t.Fatalf("new attempt state=%s err=%v", state, err)
	}
	if err = db.QueryRow(`SELECT payload::text FROM message_outbox WHERE message_id=$1`, newMsg).Scan(&payload); err != nil || !strings.Contains(payload, "raw") {
		t.Fatalf("new message modified: %s err=%v", payload, err)
	}
	if err = db.QueryRow(`SELECT provenance::text FROM price_availability_observation WHERE id=$1`, observation).Scan(&payload); err != nil || !strings.Contains(payload, "new raw") {
		t.Fatalf("new observation modified: %s err=%v", payload, err)
	}
}
