DROP TRIGGER source_revision_bump ON source_domain_config;
DROP FUNCTION bump_source_revision();
DROP TRIGGER crawl_request_terminal_offer_revision ON crawl_request;
DROP FUNCTION bump_offer_revision_on_terminal();
DROP TRIGGER offer_revision_bump ON offer;
DROP FUNCTION bump_offer_revision();
DROP INDEX observation_verified_cash_idx;
DROP INDEX crawl_request_terminal_offer_idx;
ALTER TABLE crawl_request
  DROP CONSTRAINT crawl_terminal_outcome_presence,
  DROP CONSTRAINT crawl_terminal_outcome,
  DROP COLUMN terminal_outcome;
ALTER TABLE source_domain_config
  DROP CONSTRAINT source_revision_positive,
  DROP COLUMN source_revision;
ALTER TABLE offer
  DROP CONSTRAINT offer_revision_positive,
  DROP COLUMN offer_revision;
