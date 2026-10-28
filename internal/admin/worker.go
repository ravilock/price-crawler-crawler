package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/crawler/internal/messaging"
)

// RunWorker connects the owner handler to the durable command queue. Identity
// configuration is reread for each delivery so revocation takes effect promptly.
func RunWorker(ctx context.Context, db *sql.DB, brokerURL, identityPath string, log *slog.Logger) {
	delay := time.Second
	for ctx.Err() == nil {
		err := consume(ctx, db, brokerURL, identityPath)
		if ctx.Err() != nil {
			return
		}
		log.Warn("source admin consumer disconnected", "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}
func consume(ctx context.Context, db *sql.DB, brokerURL, path string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "source.admin.command.v1", "crawler-source-admin", 1)
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
		case d, ok := <-consumer.Deliveries():
			if !ok {
				return errors.New("source admin deliveries closed")
			}
			if err = handle(ctx, db, publisher, path, d); err != nil {
				return err
			}
		}
	}
}
func handle(ctx context.Context, db *sql.DB, publisher *messaging.Publisher, path string, d amqp.Delivery) error {
	e, err := messaging.DecodeDelivery(d)
	if err != nil || e.MessageType != "source.admin.command" || d.ReplyTo == "" || len(d.ReplyTo) > 255 || d.CorrelationId != e.CorrelationID {
		return d.Reject(false)
	}
	var data struct {
		Actor Actor `json:"actor"`
	}
	if json.Unmarshal(e.Payload, &data) != nil {
		return d.Reject(false)
	}
	identity, err := LoadIdentity(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("source admin identity configuration unavailable")
	}
	principal, verifyErr := identity.Verify(d.UserId, e.Organization, data.Actor.Subject)
	result := failure("forbidden")
	if verifyErr == nil {
		result, err = Execute(ctx, db, e, principal)
		if err != nil {
			return err
		}
	}
	id, err := newID()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	reply := messaging.Envelope{MessageID: id, MessageType: "source.admin.result", SchemaVersion: 1, OccurredAt: now, Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = publisher.Publish(bounded, d.ReplyTo, reply); err != nil {
		if errors.Is(err, messaging.ErrUnroutable) {
			return d.Ack(false)
		}
		return err
	}
	return d.Ack(false)
}
