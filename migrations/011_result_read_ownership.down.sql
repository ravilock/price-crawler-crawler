DROP INDEX IF EXISTS crawl_intent_requester_idx;
ALTER TABLE crawl_intent_key DROP COLUMN IF EXISTS requester_id;
