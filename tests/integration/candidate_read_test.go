package integration

import (
	"context"
	"testing"
	"time"

	"price-crawler/services/crawler/internal/results"
)

func TestCandidateReadPinsIdentityTenantAndEvidenceExpiry(t *testing.T) {
	db := retentionDB(t, false)
	org, otherOrg := "10000000-0000-4000-8000-000000000101", "10000000-0000-4000-8000-000000000102"
	source := "20000000-0000-4000-8000-000000000101"
	offer, request, page, candidate := "50000000-0000-4000-8000-000000000101", "30000000-0000-4000-8000-000000000101", "40000000-0000-4000-8000-000000000101", "60000000-0000-4000-8000-000000000101"
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.Exec(`INSERT INTO source_domain_config(id,organization_id,domain,max_requests_per_minute,approved_categories) VALUES($1,$2,'candidate.example',60,'["parts"]')`, source, org); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO offer(id,organization_id,source_domain_id,canonical_url) VALUES($1,$2,$3,'https://candidate.example/item')`, offer, org, source); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,source_domain_id,offer_id,category,terminal_outcome,completed_at) VALUES($1,$2,'https://candidate.example/item','scheduled','completed','candidate-read',$3,$4,'parts','complete',$5)`, request, org, source, offer, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,evidence_hash,bounded_evidence,evidence_expires_at) VALUES($1,$2,$3,$4,'https://candidate.example/item','http',$5,'parsed','hash','raw evidence',$6)`, page, org, request, source, now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE page_fetch_outcome SET raw_evidence_expired_at=$2,raw_evidence_deadline=$2 WHERE id=$1`, page, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,attributes,field_provenance,normalization_version) VALUES($1,$2,$3,$4,'Widget','[]','{"title":{"status":"valid","normalized_value":"Widget","raw_value":"secret title"}}','1')`, candidate, org, request, page); err != nil {
		t.Fatal(err)
	}
	newRequest, newPage, newCandidate := "30000000-0000-4000-8000-000000000102", "40000000-0000-4000-8000-000000000102", "60000000-0000-4000-8000-000000000102"
	if _, err := db.Exec(`INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,source_domain_id,offer_id,category,terminal_outcome,completed_at) VALUES($1,$2,'https://candidate.example/item','scheduled','completed','candidate-read-newer',$3,$4,'parts','complete',$5)`, newRequest, org, source, offer, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,evidence_hash,bounded_evidence,evidence_expires_at) VALUES($1,$2,$3,$4,'https://candidate.example/item','http',$5,'parsed','hash-new','raw newer',$6)`, newPage, org, newRequest, source, now.Add(time.Minute), now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,attributes,field_provenance,normalization_version) VALUES($1,$2,$3,$4,'Newer Widget','[]','{"title":{"status":"valid","normalized_value":"Newer Widget"}}','2')`, newCandidate, org, newRequest, newPage); err != nil {
		t.Fatal(err)
	}
	read := func(o, r, c string) results.CandidateResult {
		return results.ReadCandidate(context.Background(), db, org, results.CandidateRequest{OfferID: o, CrawlRequestID: r, CandidateID: c}, now)
	}
	got := read(offer, request, candidate)
	if got.Status != "found" || got.Candidate == nil || !got.Candidate.EvidenceExpired {
		t.Fatalf("exact candidate read = %+v", got)
	}
	if got.Candidate.Fields["title"]["normalized_value"] != "Widget" {
		t.Fatalf("newer candidate replaced exact result: %+v", got.Candidate)
	}
	field := got.Candidate.Fields["title"]
	if field["raw_value"] != nil || field["normalized_value"] != "Widget" {
		t.Fatalf("expired evidence was not redacted: %#v", field)
	}
	if got = read(offer, "30000000-0000-4000-8000-000000000199", candidate); got.Status != "forbidden" {
		t.Fatalf("old request status = %s", got.Status)
	}
	if got = read("50000000-0000-4000-8000-000000000199", request, candidate); got.Status != "forbidden" {
		t.Fatalf("wrong offer status = %s", got.Status)
	}
	if got = results.ReadCandidate(context.Background(), db, otherOrg, results.CandidateRequest{OfferID: offer, CrawlRequestID: request, CandidateID: candidate}, now); got.Status != "forbidden" {
		t.Fatalf("foreign tenant status = %s", got.Status)
	}
	if got = read(offer, request, "60000000-0000-4000-8000-000000000199"); got.Status != "not_found" {
		t.Fatalf("wrong candidate status = %s", got.Status)
	}
}
