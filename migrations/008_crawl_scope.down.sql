ALTER TABLE crawl_intent_key DROP CONSTRAINT crawl_intent_category_scope, DROP CONSTRAINT crawl_intent_source_fk, DROP COLUMN category, DROP COLUMN source_domain_id;
ALTER TABLE crawl_request DROP CONSTRAINT crawl_category_scope, DROP CONSTRAINT crawl_source_tenant_fk, DROP COLUMN category, DROP COLUMN source_domain_id;
