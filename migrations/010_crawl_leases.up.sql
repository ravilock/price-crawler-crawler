ALTER TABLE crawl_request ADD COLUMN lease_token uuid, ADD COLUMN lease_until timestamptz;
CREATE INDEX crawl_request_work ON crawl_request(state,next_attempt_at,lease_until) WHERE source_domain_id IS NOT NULL AND category IS NOT NULL;
