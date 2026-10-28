-- Source administration is crawler-owned. No tenant database access is required.
CREATE TABLE source_admin_review (
 organization_id uuid NOT NULL,
 source_domain_id uuid NOT NULL,
 requested_categories jsonb NOT NULL CHECK (jsonb_typeof(requested_categories) = 'array'),
 requested_by text NOT NULL,
 state text NOT NULL CHECK (state IN ('pending_review', 'approved', 'rejected')),
 reviewed_by text,
 review jsonb,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (organization_id, source_domain_id),
 FOREIGN KEY (organization_id, source_domain_id) REFERENCES source_domain_config(organization_id, id)
);
CREATE TABLE source_admin_receipt (
 organization_id uuid NOT NULL,
 actor_id text NOT NULL,
 action text NOT NULL,
 idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
 request_hash text NOT NULL,
 result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (organization_id, actor_id, action, idempotency_key)
);
CREATE TABLE source_admin_audit (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 actor_id text NOT NULL,
 actor_role text NOT NULL,
 action text NOT NULL,
 entity_type text NOT NULL,
 entity_id uuid NOT NULL,
 before_value jsonb,
 after_value jsonb NOT NULL,
 occurred_at timestamptz NOT NULL
);

CREATE FUNCTION source_admin_audit_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'source administration audit is append-only';
END $$;
CREATE TRIGGER source_admin_audit_immutable BEFORE UPDATE OR DELETE ON source_admin_audit
 FOR EACH ROW EXECUTE FUNCTION source_admin_audit_append_only();
