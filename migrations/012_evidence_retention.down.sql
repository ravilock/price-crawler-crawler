-- Redacted raw evidence cannot be restored by rollback.
CREATE OR REPLACE FUNCTION candidate_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (OLD.title, OLD.description, OLD.brand, OLD.manufacturer_model, OLD.gtin, OLD.attributes, OLD.normalization_version)
    IS DISTINCT FROM (NEW.title, NEW.description, NEW.brand, NEW.manufacturer_model, NEW.gtin, NEW.attributes, NEW.normalization_version)
 THEN RAISE EXCEPTION 'candidate core identity is immutable'; END IF;
 RETURN NEW;
END $$;
DROP FUNCTION redact_raw_evidence(jsonb);
DROP INDEX page_raw_evidence_due;
DROP TRIGGER page_raw_evidence_deadline ON page_fetch_outcome;
DROP FUNCTION set_raw_evidence_deadline();
ALTER TABLE page_fetch_outcome DROP COLUMN raw_evidence_deadline, DROP COLUMN raw_evidence_expired_at;
