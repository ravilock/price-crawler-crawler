-- Reject literal IPs and non-HTTP schemes for submitted and canonical URLs.
CREATE FUNCTION collection_valid_url(value text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE host text;
BEGIN
  IF value !~ '^https?://[^/@:?#]+(:[0-9]+)?(/|\?|$)' THEN RETURN false; END IF;
  host := substring(value FROM '^https?://([^/@:?#]+)');
  IF host IS NULL OR host <> lower(host) OR host !~ '^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$' OR host ~ '\.\.' THEN RETURN false; END IF;
  BEGIN
    PERFORM host::inet;
    RETURN false;
  EXCEPTION WHEN invalid_text_representation THEN
    RETURN true;
  END;
END $$;
-- Collection tables extend the retained 001 schema; message delivery remains owned by 002.
ALTER TABLE source_domain_config
  ADD CONSTRAINT source_domain_tenant_id UNIQUE (organization_id, id),
  ADD CONSTRAINT source_domain_normalized CHECK (domain = lower(domain) AND domain ~ '^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$' AND domain !~ '(^|\.)[0-9]+(\.[0-9]+){3}$' AND domain !~ '\.\.' AND collection_valid_url('https://' || domain)),
  ADD CONSTRAINT source_pacing_bounds CHECK (max_requests_per_minute BETWEEN 1 AND 60000 AND crawl_delay_seconds BETWEEN 0 AND 86400),
  ADD COLUMN robots_status text,
  ADD COLUMN robots_checked_at timestamptz,
  ADD COLUMN robots_expires_at timestamptz,
  ADD COLUMN robots_content_hash text,
  ADD COLUMN adapter_name text,
  ADD COLUMN adapter_version text,
  ADD COLUMN approved_categories jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(approved_categories) = 'array'),
  ADD COLUMN rendering_allowed boolean NOT NULL DEFAULT false,
  ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE source_path_override (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, source_domain_id uuid NOT NULL,
 path_pattern text NOT NULL CHECK (path_pattern LIKE '/%'), actor_id text NOT NULL CHECK (length(trim(actor_id)) > 0),
 reason text NOT NULL CHECK (length(trim(reason)) > 0), created_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz, revoked_at timestamptz,
 FOREIGN KEY (organization_id, source_domain_id) REFERENCES source_domain_config(organization_id,id)
);
ALTER TABLE offer
  ADD CONSTRAINT offer_tenant_id UNIQUE (organization_id, id),
  ADD CONSTRAINT offer_source_tenant_fk FOREIGN KEY (organization_id,source_domain_id) REFERENCES source_domain_config(organization_id,id),
  ADD CONSTRAINT offer_price_nonnegative CHECK (current_price_cents IS NULL OR current_price_cents >= 0),
  ADD CONSTRAINT offer_availability CHECK (availability IN ('available','unavailable','unknown')),
  ADD CONSTRAINT offer_url_scheme CHECK (collection_valid_url(canonical_url) AND canonical_url !~* '[?&](utm_[^=&#]*|gclid|fbclid)=' AND canonical_url !~ '#'),
  ADD COLUMN last_valid_price_at timestamptz,
  ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE crawl_request
  ADD CONSTRAINT crawl_tenant_id UNIQUE (organization_id,id),
  ADD CONSTRAINT crawl_state CHECK (state IN ('queued','fetching','rendering','finding','completed','retry_wait','failed','paused')),
  ADD CONSTRAINT crawl_url_scheme CHECK (collection_valid_url(submitted_url)),
  ADD COLUMN offer_id uuid,
  ADD COLUMN next_attempt_at timestamptz,
  ADD COLUMN last_error_code text,
  ADD COLUMN correlation_id uuid,
  ADD COLUMN completed_at timestamptz,
  ADD CONSTRAINT crawl_offer_tenant_fk FOREIGN KEY (organization_id,offer_id) REFERENCES offer(organization_id,id);
CREATE TABLE page_fetch_outcome (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, crawl_request_id uuid NOT NULL, source_domain_id uuid NOT NULL,
 requested_url text NOT NULL, final_url text, mode text NOT NULL CHECK (mode IN ('http','rendered')),
 http_status integer CHECK (http_status BETWEEN 100 AND 599), content_type text, observed_at timestamptz NOT NULL,
 outcome text NOT NULL CHECK (outcome IN ('parsed','partial','blocked','invalid_url','not_found','unsupported_page','timeout','rate_limited','server_error')),
 evidence_hash text, bounded_evidence text CHECK (octet_length(bounded_evidence) <= 8192), evidence_expires_at timestamptz,
 adapter_version text, renderer_version text, parser_version text,
 UNIQUE (organization_id,id), FOREIGN KEY (organization_id,crawl_request_id) REFERENCES crawl_request(organization_id,id),
 FOREIGN KEY (organization_id,source_domain_id) REFERENCES source_domain_config(organization_id,id),
 CHECK ((evidence_hash IS NULL AND bounded_evidence IS NULL AND evidence_expires_at IS NULL) OR
        ((evidence_hash IS NOT NULL OR bounded_evidence IS NOT NULL) AND evidence_expires_at IS NOT NULL AND evidence_expires_at >= observed_at AND evidence_expires_at <= observed_at + interval '30 days'))
);
CREATE TABLE product_information_candidate (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, crawl_request_id uuid NOT NULL, page_fetch_outcome_id uuid NOT NULL,
 title text, description text, brand text, manufacturer_model text, gtin text,
 attributes jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(attributes) = 'array'),
 field_provenance jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(field_provenance) = 'object'),
 candidate_price_cents bigint CHECK (candidate_price_cents >= 0), payment_method text,
 availability text NOT NULL DEFAULT 'unknown' CHECK (availability IN ('available','unavailable','unknown')),
 normalization_version text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (organization_id,id), FOREIGN KEY (organization_id,crawl_request_id) REFERENCES crawl_request(organization_id,id),
 FOREIGN KEY (organization_id,page_fetch_outcome_id) REFERENCES page_fetch_outcome(organization_id,id)
);
CREATE FUNCTION candidate_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (OLD.title, OLD.description, OLD.brand, OLD.manufacturer_model, OLD.gtin, OLD.attributes, OLD.normalization_version)
    IS DISTINCT FROM (NEW.title, NEW.description, NEW.brand, NEW.manufacturer_model, NEW.gtin, NEW.attributes, NEW.normalization_version)
 THEN RAISE EXCEPTION 'candidate core identity is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER candidate_identity_immutable_trigger BEFORE UPDATE ON product_information_candidate
 FOR EACH ROW EXECUTE FUNCTION candidate_identity_immutable();
ALTER TABLE offer ADD COLUMN product_information_candidate_id uuid,
 ADD CONSTRAINT offer_candidate_tenant_fk FOREIGN KEY (organization_id,product_information_candidate_id) REFERENCES product_information_candidate(organization_id,id);
ALTER TABLE price_availability_observation
 ADD COLUMN crawl_request_id uuid,
 ADD COLUMN outcome text,
 ADD COLUMN provenance jsonb,
 ADD COLUMN adapter_version text,
 ADD CONSTRAINT observation_price_nonnegative CHECK (price_cents IS NULL OR price_cents >= 0),
 ADD CONSTRAINT observation_offer_tenant_fk FOREIGN KEY (organization_id,offer_id) REFERENCES offer(organization_id,id),
 ADD CONSTRAINT observation_request_tenant_fk FOREIGN KEY (organization_id,crawl_request_id) REFERENCES crawl_request(organization_id,id),
 ADD CONSTRAINT observation_request_once UNIQUE (organization_id,crawl_request_id),
 ADD CONSTRAINT observation_request_required CHECK (crawl_request_id IS NOT NULL) NOT VALID;
