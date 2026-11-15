-- T137: stable per-offer observation ordering with explicit price status and
-- payment eligibility. The observation row id remains the stable observation_id.
--
-- Backfill is deterministic: retained observations are ordered per
-- (organization_id, offer_id) by observed_at ascending, then by id ascending so
-- equal timestamps keep a stable, reproducible order. Legacy rows have no
-- recorded price status, so a retained price becomes 'valid' and a null price
-- becomes 'missing'.
ALTER TABLE price_availability_observation
  ADD COLUMN position bigint,
  ADD COLUMN price_status text,
  ADD COLUMN payment_eligible boolean;

WITH ordered AS (
  SELECT id,
         row_number() OVER (
           PARTITION BY organization_id, offer_id
           ORDER BY observed_at, id
         ) AS pos
  FROM price_availability_observation
)
UPDATE price_availability_observation o
SET position = ordered.pos,
    price_status = CASE WHEN o.price_cents IS NOT NULL THEN 'valid' ELSE 'missing' END,
    payment_eligible = (o.price_cents IS NOT NULL AND o.payment_method = 'cash')
FROM ordered
WHERE o.id = ordered.id;

ALTER TABLE price_availability_observation
  ALTER COLUMN position SET NOT NULL,
  ALTER COLUMN price_status SET NOT NULL,
  ALTER COLUMN payment_eligible SET NOT NULL,
  ADD CONSTRAINT observation_position_positive CHECK (position > 0),
  ADD CONSTRAINT observation_price_status
    CHECK (price_status IN ('valid', 'missing', 'invalid', 'uncertain')),
  ADD CONSTRAINT observation_price_status_consistency
    CHECK ((price_status = 'valid') = (price_cents IS NOT NULL)),
  ADD CONSTRAINT observation_payment_eligibility
    CHECK (payment_eligible = (price_status = 'valid' AND payment_method = 'cash')),
  ADD CONSTRAINT observation_offer_position
    UNIQUE (organization_id, offer_id, position);

-- Positions are assigned under the offer lock in the same transaction, so two
-- concurrent collectors for one offer cannot reuse a position. A duplicate
-- request still fails the existing (organization_id, crawl_request_id)
-- uniqueness constraint; its tentative position is never committed and never
-- consumed.
CREATE FUNCTION assign_observation_ordering() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.position IS NULL THEN
    PERFORM 1 FROM offer
      WHERE organization_id = NEW.organization_id AND id = NEW.offer_id
      FOR UPDATE;
    SELECT COALESCE(MAX(position), 0) + 1 INTO NEW.position
      FROM price_availability_observation
      WHERE organization_id = NEW.organization_id AND offer_id = NEW.offer_id;
  END IF;
  IF NEW.price_status IS NULL THEN
    NEW.price_status := CASE WHEN NEW.price_cents IS NOT NULL THEN 'valid' ELSE 'missing' END;
  END IF;
  NEW.payment_eligible :=
    (NEW.price_status = 'valid' AND NEW.price_cents IS NOT NULL AND NEW.payment_method = 'cash');
  RETURN NEW;
END $$;

CREATE TRIGGER observation_ordering_assign
  BEFORE INSERT ON price_availability_observation
  FOR EACH ROW EXECUTE FUNCTION assign_observation_ordering();

-- History stays append-only: identity, ordering, price and stock facts cannot
-- change. Evidence redaction (provenance), adapter version and outcome remain
-- updateable for retention and diagnostics.
CREATE FUNCTION observation_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.id, OLD.organization_id, OLD.offer_id, OLD.crawl_request_id, OLD.idempotency_key,
      OLD.position, OLD.observed_at, OLD.price_cents, OLD.price_status, OLD.payment_method,
      OLD.availability, OLD.payment_eligible)
     IS DISTINCT FROM
     (NEW.id, NEW.organization_id, NEW.offer_id, NEW.crawl_request_id, NEW.idempotency_key,
      NEW.position, NEW.observed_at, NEW.price_cents, NEW.price_status, NEW.payment_method,
      NEW.availability, NEW.payment_eligible)
  THEN RAISE EXCEPTION 'observations are append-only'; END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER observation_append_only_update
  BEFORE UPDATE ON price_availability_observation
  FOR EACH ROW EXECUTE FUNCTION observation_append_only();

CREATE FUNCTION observation_no_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'observations are append-only';
END $$;

CREATE TRIGGER observation_append_only_delete
  BEFORE DELETE ON price_availability_observation
  FOR EACH ROW EXECUTE FUNCTION observation_no_delete();
