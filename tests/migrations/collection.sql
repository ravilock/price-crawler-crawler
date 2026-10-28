-- Acceptance constraints from data-model.md:
-- Normalized hostname; unique per organization. Reject IP literals and unsupported schemes for user submitted source URLs.
-- Positive bounded request policy; effective request pace must honor both.
-- `manual`, `scheduled`, or `discovery`.
-- `queued`, `fetching`, `rendering`, `finding`, `completed`, `retry_wait`, `failed`, `paused`.
-- `http` or `rendered`.
-- `parsed`, `partial`, `blocked`, `invalid_url`, `not_found`, `unsupported_page`, `timeout`, `rate_limited`, or `server_error`.
-- Hash or at most 8 KiB excerpt only; expiry is required and must be between observation time and 30 days later. Never indefinite full HTML storage.
-- Normalized approved source URL; recognized tracking parameters are removed while other query parameters are preserved. Unique with source domain within organization.
BEGIN;
INSERT INTO source_domain_config (id, organization_id,domain,max_requests_per_minute) VALUES ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000010','example.com',60);
INSERT INTO crawl_request (id,organization_id,submitted_url,reason,state,idempotency_key) VALUES ('00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000010','https://example.com/item?color=red','manual','queued','req-1');
INSERT INTO offer (id,organization_id,source_domain_id,canonical_url) VALUES ('00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000001','https://example.com/item?color=red');
INSERT INTO price_availability_observation (id, organization_id, offer_id, crawl_request_id, price_cents, availability, observed_at, idempotency_key)
 VALUES ('00000000-0000-0000-0000-000000000006','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000002',10000,'available',now(),'observation-1');
DO $$ BEGIN
 BEGIN INSERT INTO price_availability_observation (id,organization_id,offer_id,crawl_request_id,availability,observed_at,idempotency_key) VALUES ('00000000-0000-0000-0000-000000000007','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000002','available',now(),'observation-2'); RAISE EXCEPTION 'duplicate crawl observation accepted'; EXCEPTION WHEN unique_violation THEN NULL; END;
 BEGIN INSERT INTO price_availability_observation (id,organization_id,offer_id,availability,observed_at,idempotency_key) VALUES ('00000000-0000-0000-0000-000000000007','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000003','available',now(),'observation-2'); RAISE EXCEPTION 'missing crawl request accepted'; EXCEPTION WHEN check_violation THEN NULL; END;
 IF collection_valid_url('http://127.0.0.1/item') OR collection_valid_url('file://example.com/a') OR collection_valid_url('https://Example.com/a') THEN RAISE EXCEPTION 'invalid URL accepted'; END IF;
 BEGIN INSERT INTO offer (id,organization_id,source_domain_id,canonical_url) VALUES ('00000000-0000-0000-0000-000000000004','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000001','https://example.com/item?utm_source=x'); RAISE EXCEPTION 'tracking URL accepted'; EXCEPTION WHEN check_violation THEN NULL; END;
 BEGIN INSERT INTO offer (id,organization_id,source_domain_id,canonical_url) VALUES ('00000000-0000-0000-0000-000000000004','00000000-0000-0000-0000-000000000011','00000000-0000-0000-0000-000000000001','https://example.com/item'); RAISE EXCEPTION 'cross-tenant source accepted'; EXCEPTION WHEN foreign_key_violation THEN NULL; END;
 BEGIN INSERT INTO page_fetch_outcome (id,organization_id,crawl_request_id,source_domain_id,requested_url,mode,observed_at,outcome,bounded_evidence,evidence_expires_at) VALUES ('00000000-0000-0000-0000-000000000005','00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001','https://example.com/','http',now(),'parsed','evidence',now()+interval '31 days'); RAISE EXCEPTION 'excess evidence retention accepted'; EXCEPTION WHEN check_violation THEN NULL; END;
END $$;
ROLLBACK;
