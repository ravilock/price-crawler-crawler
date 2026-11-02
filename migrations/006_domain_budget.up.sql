CREATE TABLE source_domain_budget (
 domain text PRIMARY KEY CHECK (length(domain) BETWEEN 1 AND 253 AND domain = lower(domain)),
 next_allowed_at timestamptz NOT NULL DEFAULT '-infinity',
 cooldown_until timestamptz NOT NULL DEFAULT '-infinity'
);
