CREATE TABLE crawl_intent_key (
 organization_id uuid NOT NULL,
 idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
 crawl_request_id uuid NOT NULL,
 submitted_url text NOT NULL,
 reason text NOT NULL CHECK (reason IN ('manual','scheduled','discovery')),
 PRIMARY KEY (organization_id,idempotency_key),
 FOREIGN KEY (organization_id,crawl_request_id) REFERENCES crawl_request(organization_id,id)
);
INSERT INTO crawl_intent_key(organization_id,idempotency_key,crawl_request_id,submitted_url,reason)
 SELECT organization_id,idempotency_key,id,submitted_url,reason FROM crawl_request;
