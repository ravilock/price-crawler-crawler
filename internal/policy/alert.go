package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"

	"price-crawler/services/crawler/internal/messaging"
)

// WriteRobotsAudit stores a system operational event using the existing audit
// contract. The owner transaction also updates the source's robots status.
// An audit event alone is not an administrator notification channel.
func WriteRobotsAudit(ctx context.Context, tx *sql.Tx, organization, sourceID string, record RobotsRecord) error {
	if tx == nil || !record.AlertAdministrator || record.Status != "unreadable" {
		return fmt.Errorf("invalid robots audit event")
	}
	value, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	uuidValue := value.String()
	after, err := json.Marshal(map[string]any{"status": record.Status, "checked_at": record.CheckedAt, "expires_at": record.ExpiresAt})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO source_admin_audit(id,organization_id,actor_id,actor_role,action,entity_type,entity_id,before_value,after_value,occurred_at) VALUES($1,$2,'crawler-policy','system','source.robots.unreadable','source',$3,NULL,$4,$5)`, uuidValue, organization, sourceID, after, record.CheckedAt); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"actor": map[string]string{"subject": "crawler-policy", "role": "system"}, "organization_id": organization, "action": "source.robots.unreadable", "entity": map[string]string{"type": "source", "id": sourceID}, "before": nil, "after": json.RawMessage(after), "occurred_at": record.CheckedAt})
	if err != nil {
		return err
	}
	event := messaging.Envelope{MessageID: uuidValue, MessageType: "audit.event.recorded", SchemaVersion: 1, OccurredAt: record.CheckedAt, Organization: organization, CorrelationID: uuidValue, Payload: payload}
	return messaging.EnqueueOutbox(ctx, tx, event, "audit.event.recorded.v1")
}

// WriteRobotsNotification records an administrator-facing notification and the
// corresponding audit event in the source snapshot transaction.
func WriteRobotsNotification(ctx context.Context, tx *sql.Tx, organization, sourceID string, record RobotsRecord) error {
	if err := WriteRobotsAudit(ctx, tx, organization, sourceID, record); err != nil {
		return err
	}
	id, err := fetchID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO source_admin_notification(id,organization_id,source_domain_id,kind,occurred_at) VALUES($1,$2,$3,'robots_unreadable',$4)`, id, organization, sourceID, record.CheckedAt)
	return err
}
