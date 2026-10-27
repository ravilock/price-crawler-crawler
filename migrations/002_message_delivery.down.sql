-- Version 1 already owns message_outbox; retain it when reverting version 2.
-- Destructive: removes inbox deduplication history; explicit operator rollback only.
DROP TABLE IF EXISTS message_inbox;
DROP INDEX IF EXISTS message_outbox_pending_idx;
