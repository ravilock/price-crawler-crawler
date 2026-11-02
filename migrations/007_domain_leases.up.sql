ALTER TABLE source_domain_budget ADD COLUMN active_until timestamptz NOT NULL DEFAULT '-infinity', ADD COLUMN active_token text;
