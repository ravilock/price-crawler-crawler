package policy

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"price-crawler/services/crawler/internal/admin"
	"price-crawler/services/crawler/internal/messaging"
)

type alertRead struct {
	Actor admin.Actor `json:"actor"`
	Limit int         `json:"limit"`
}
type notification struct {
	ID         string    `json:"id"`
	SourceID   string    `json:"source_id"`
	Domain     string    `json:"domain"`
	OccurredAt time.Time `json:"occurred_at"`
}

func ReadNotifications(ctx context.Context, db *sql.DB, organization string, limit int) ([]notification, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("invalid notification limit")
	}
	rows, err := db.QueryContext(ctx, `SELECT n.id::text,n.source_domain_id::text,s.domain,n.occurred_at FROM source_admin_notification n JOIN source_domain_config s ON s.organization_id=n.organization_id AND s.id=n.source_domain_id WHERE n.organization_id=$1 ORDER BY n.occurred_at DESC,n.id DESC LIMIT $2`, organization, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []notification{}
	for rows.Next() {
		var item notification
		if err = rows.Scan(&item.ID, &item.SourceID, &item.Domain, &item.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
func RunNotificationReader(ctx context.Context, db *sql.DB, brokerURL, identityPath string, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := consumeNotifications(ctx, db, brokerURL, identityPath); err != nil && ctx.Err() == nil {
			logger.Warn("source notification reader disconnected", "error", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func consumeNotifications(ctx context.Context, db *sql.DB, brokerURL, identityPath string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "source.alerts.read.requested.v1", "crawler-source-alerts", 1)
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()
	publisher, err := messaging.NewPublisher(brokerURL)
	if err != nil {
		return err
	}
	defer func() { _ = publisher.Close() }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-consumer.Deliveries():
			if !ok {
				return errors.New("notification queue closed")
			}
			e, err := messaging.DecodeDelivery(delivery)
			if err != nil || e.MessageType != "source.alerts.read.requested" || delivery.ReplyTo == "" || len(delivery.ReplyTo) > 255 || delivery.CorrelationId != e.CorrelationID {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			var req alertRead
			decoder := json.NewDecoder(bytes.NewReader(e.Payload))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF || req.Limit < 1 || req.Limit > 100 || req.Actor.Role != "administrator" {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			identity, err := admin.LoadIdentity(identityPath)
			if err != nil {
				return err
			}
			_, err = identity.Verify(delivery.UserId, e.Organization, req.Actor.Subject)
			status := "forbidden"
			items := []notification{}
			if err == nil {
				status = "ok"
				items, err = ReadNotifications(ctx, db, e.Organization, req.Limit)
				if err != nil {
					return err
				}
			}
			payload, _ := json.Marshal(map[string]any{"status": status, "items": items})
			id, err := fetchID()
			if err != nil {
				return err
			}
			reply := messaging.Envelope{MessageID: id, MessageType: "source.alerts.read.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = publisher.Publish(bounded, delivery.ReplyTo, reply)
			cancel()
			if errors.Is(err, messaging.ErrUnroutable) {
				if err = delivery.Ack(false); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
