CREATE TABLE source_admin_notification (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 source_domain_id uuid NOT NULL,
 kind text NOT NULL CHECK (kind='robots_unreadable'),
 occurred_at timestamptz NOT NULL,
 FOREIGN KEY (organization_id,source_domain_id) REFERENCES source_domain_config(organization_id,id)
);
CREATE INDEX source_admin_notification_read ON source_admin_notification(organization_id,occurred_at DESC,id DESC);
