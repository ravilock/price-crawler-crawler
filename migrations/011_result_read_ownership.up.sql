ALTER TABLE crawl_intent_key ADD COLUMN requester_id text;
CREATE INDEX crawl_intent_requester_idx
  ON crawl_intent_key(organization_id, requester_id, crawl_request_id)
  WHERE requester_id IS NOT NULL;
