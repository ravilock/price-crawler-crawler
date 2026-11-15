-- T158: durable offer-state and source-policy revisions with the terminal
-- collection outcome used by the ranking-facts read boundary.
--
-- Existing rows start at revision 1. Retained terminal requests have no
-- recorded outcome, so they are backfilled deterministically: completed
-- requests become 'partial' (the current finder path emits partial results),
-- failed become 'failed' and paused become 'paused'. A missing completion time
-- is filled from the last update so every terminal row is readable.
ALTER TABLE offer ADD COLUMN offer_revision bigint NOT NULL DEFAULT 1;
ALTER TABLE offer ADD CONSTRAINT offer_revision_positive CHECK (offer_revision > 0);

ALTER TABLE source_domain_config ADD COLUMN source_revision bigint NOT NULL DEFAULT 1;
ALTER TABLE source_domain_config ADD CONSTRAINT source_revision_positive CHECK (source_revision > 0);

ALTER TABLE crawl_request ADD COLUMN terminal_outcome text;
UPDATE crawl_request
SET terminal_outcome = CASE state
      WHEN 'completed' THEN 'partial'
      WHEN 'failed' THEN 'failed'
      WHEN 'paused' THEN 'paused'
    END,
    completed_at = COALESCE(completed_at, updated_at)
WHERE state IN ('completed', 'failed', 'paused');

ALTER TABLE crawl_request
  ADD CONSTRAINT crawl_terminal_outcome
    CHECK (terminal_outcome IS NULL OR terminal_outcome IN ('complete', 'partial', 'failed', 'paused')),
  ADD CONSTRAINT crawl_terminal_outcome_presence
    CHECK ((state IN ('completed', 'failed', 'paused')) = (terminal_outcome IS NOT NULL));

CREATE INDEX crawl_request_terminal_offer_idx
  ON crawl_request(organization_id, offer_id, completed_at DESC, id DESC)
  WHERE terminal_outcome IS NOT NULL;

CREATE INDEX observation_verified_cash_idx
  ON price_availability_observation(organization_id, offer_id, position DESC)
  WHERE price_status = 'valid' AND payment_eligible AND price_cents IS NOT NULL;

-- An offer revision advances only when ranking-relevant offer state changes.
-- Idempotent replays are rejected by the inbox before this update, so they
-- cannot advance the counter.
CREATE FUNCTION bump_offer_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.current_price_cents, OLD.payment_method, OLD.availability, OLD.last_valid_price_at,
      OLD.last_checked_at, OLD.consecutive_failures, OLD.is_paused)
     IS DISTINCT FROM
     (NEW.current_price_cents, NEW.payment_method, NEW.availability, NEW.last_valid_price_at,
      NEW.last_checked_at, NEW.consecutive_failures, NEW.is_paused)
  THEN
    NEW.offer_revision := OLD.offer_revision + 1;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER offer_revision_bump
  BEFORE UPDATE ON offer
  FOR EACH ROW EXECUTE FUNCTION bump_offer_revision();

-- A terminal collection outcome changes ranking facts even when the failed
-- attempt produced no observation.
CREATE FUNCTION bump_offer_revision_on_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.offer_id IS NOT NULL AND NEW.terminal_outcome IS NOT NULL
     AND (OLD.terminal_outcome IS DISTINCT FROM NEW.terminal_outcome
          OR OLD.state IS DISTINCT FROM NEW.state)
  THEN
    UPDATE offer SET offer_revision = offer_revision + 1
      WHERE organization_id = NEW.organization_id AND id = NEW.offer_id;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER crawl_request_terminal_offer_revision
  AFTER UPDATE ON crawl_request
  FOR EACH ROW EXECUTE FUNCTION bump_offer_revision_on_terminal();

-- A source revision advances when effective freshness or policy changes; the
-- catalog revalidates affected projections and keeps historical facts.
CREATE FUNCTION bump_source_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.freshness_window_seconds, OLD.is_active, OLD.approved_categories,
      OLD.max_requests_per_minute, OLD.crawl_delay_seconds, OLD.rendering_allowed)
     IS DISTINCT FROM
     (NEW.freshness_window_seconds, NEW.is_active, NEW.approved_categories,
      NEW.max_requests_per_minute, NEW.crawl_delay_seconds, NEW.rendering_allowed)
  THEN
    NEW.source_revision := OLD.source_revision + 1;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER source_revision_bump
  BEFORE UPDATE ON source_domain_config
  FOR EACH ROW EXECUTE FUNCTION bump_source_revision();
