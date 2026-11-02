ALTER TABLE crawl_request ADD COLUMN source_domain_id uuid, ADD COLUMN category text;
ALTER TABLE crawl_request ADD CONSTRAINT crawl_source_tenant_fk FOREIGN KEY (organization_id,source_domain_id) REFERENCES source_domain_config(organization_id,id);
ALTER TABLE crawl_request ADD CONSTRAINT crawl_category_scope CHECK ((source_domain_id IS NULL AND category IS NULL) OR (source_domain_id IS NOT NULL AND category IS NOT NULL AND length(category) BETWEEN 1 AND 100));
ALTER TABLE crawl_intent_key ADD COLUMN source_domain_id uuid, ADD COLUMN category text;
ALTER TABLE crawl_intent_key ADD CONSTRAINT crawl_intent_source_fk FOREIGN KEY (organization_id,source_domain_id) REFERENCES source_domain_config(organization_id,id);
ALTER TABLE crawl_intent_key ADD CONSTRAINT crawl_intent_category_scope CHECK ((source_domain_id IS NULL AND category IS NULL) OR (source_domain_id IS NOT NULL AND category IS NOT NULL AND length(category) BETWEEN 1 AND 100));
