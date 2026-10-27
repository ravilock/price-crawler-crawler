CREATE TABLE IF NOT EXISTS source_domain_config (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, domain text NOT NULL,
 is_active boolean NOT NULL DEFAULT false, max_requests_per_minute integer NOT NULL CHECK (max_requests_per_minute > 0),
 crawl_delay_seconds integer NOT NULL DEFAULT 0 CHECK (crawl_delay_seconds >= 0), freshness_window_seconds integer NOT NULL DEFAULT 86400 CHECK (freshness_window_seconds > 0),
 UNIQUE(organization_id, domain)
);
CREATE TABLE IF NOT EXISTS crawl_request (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, submitted_url text NOT NULL,
 reason text NOT NULL CHECK (reason IN ('manual','scheduled','discovery')),
 state text NOT NULL, idempotency_key text NOT NULL, attempt_count integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(organization_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS offer (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, source_domain_id uuid NOT NULL REFERENCES source_domain_config(id),
 canonical_url text NOT NULL, current_price_cents bigint, payment_method text, availability text NOT NULL DEFAULT 'unknown',
 last_checked_at timestamptz, consecutive_failures integer NOT NULL DEFAULT 0, is_paused boolean NOT NULL DEFAULT false,
 UNIQUE(organization_id,source_domain_id,canonical_url)
);
CREATE TABLE IF NOT EXISTS price_availability_observation (
 id uuid PRIMARY KEY, organization_id uuid NOT NULL, offer_id uuid NOT NULL REFERENCES offer(id),
 price_cents bigint, payment_method text, availability text NOT NULL CHECK (availability IN ('available','unavailable','unknown')),
 observed_at timestamptz NOT NULL, idempotency_key text NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS message_outbox (
 message_id uuid PRIMARY KEY, destination text NOT NULL, message_type text NOT NULL,
 organization_id uuid NOT NULL, correlation_id uuid NOT NULL, payload jsonb NOT NULL,
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0), available_at timestamptz NOT NULL DEFAULT now(),
 claimed_until timestamptz, dead_lettered_at timestamptz, last_error text,
 published_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
