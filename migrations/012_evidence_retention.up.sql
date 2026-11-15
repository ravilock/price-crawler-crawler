-- Preserve the original deadline even after the evidence columns are cleared.
ALTER TABLE page_fetch_outcome ADD COLUMN raw_evidence_deadline timestamptz;
ALTER TABLE page_fetch_outcome ADD COLUMN raw_evidence_expired_at timestamptz;
UPDATE page_fetch_outcome SET raw_evidence_deadline = COALESCE(evidence_expires_at, LEAST(now(), observed_at + interval '30 days'));
ALTER TABLE page_fetch_outcome ALTER COLUMN raw_evidence_deadline SET NOT NULL;
CREATE FUNCTION set_raw_evidence_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP = 'INSERT' AND NEW.raw_evidence_deadline IS NULL THEN
   NEW.raw_evidence_deadline := COALESCE(NEW.evidence_expires_at, NEW.observed_at + interval '30 days');
 ELSIF TG_OP = 'UPDATE' AND NEW.evidence_expires_at IS NOT NULL AND NEW.evidence_expires_at IS DISTINCT FROM OLD.evidence_expires_at THEN
   NEW.raw_evidence_deadline := LEAST(OLD.raw_evidence_deadline, NEW.evidence_expires_at);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER page_raw_evidence_deadline BEFORE INSERT OR UPDATE OF evidence_expires_at ON page_fetch_outcome
 FOR EACH ROW EXECUTE FUNCTION set_raw_evidence_deadline();
CREATE INDEX page_raw_evidence_due ON page_fetch_outcome(raw_evidence_deadline) WHERE raw_evidence_expired_at IS NULL;

-- Keep object and array structure and every non-raw fact intact.
CREATE FUNCTION redact_raw_evidence(value jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE result jsonb;
BEGIN
 CASE jsonb_typeof(value)
 WHEN 'object' THEN
   SELECT COALESCE(jsonb_object_agg(key, CASE WHEN key = 'raw_value' THEN 'null'::jsonb ELSE redact_raw_evidence(val) END), '{}'::jsonb)
   INTO result FROM jsonb_each(value) AS e(key,val);
   RETURN result;
 WHEN 'array' THEN
   SELECT COALESCE(jsonb_agg(redact_raw_evidence(val) ORDER BY ord), '[]'::jsonb)
   INTO result FROM jsonb_array_elements(value) WITH ORDINALITY AS e(val,ord);
   RETURN result;
 ELSE RETURN value;
 END CASE;
END $$;

CREATE OR REPLACE FUNCTION candidate_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (OLD.title, OLD.description, OLD.brand, OLD.manufacturer_model, OLD.gtin, OLD.normalization_version)
    IS DISTINCT FROM (NEW.title, NEW.description, NEW.brand, NEW.manufacturer_model, NEW.gtin, NEW.normalization_version)
    OR (NEW.attributes IS DISTINCT FROM OLD.attributes AND NEW.attributes IS DISTINCT FROM redact_raw_evidence(OLD.attributes))
 THEN RAISE EXCEPTION 'candidate core identity is immutable'; END IF;
 RETURN NEW;
END $$;
