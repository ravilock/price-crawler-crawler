package admin

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"price-crawler/services/crawler/internal/messaging"
)

// RunAuditRelay publishes committed outbox messages with broker confirms.
func RunAuditRelay(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := relayOnce(ctx, db, url, logger); err != nil {
				logger.Warn("outbox relay unavailable", "error", err)
			}
		}
	}
}
func relayOnce(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	messages, err := messaging.ClaimOutbox(ctx, tx, 25)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	publisher, err := messaging.NewPublisher(url)
	if publisher != nil {
		defer func() { _ = publisher.Close() }()
	}
	for _, m := range messages {
		var publishErr error
		if err != nil {
			publishErr = err
		} else {
			var envelope messaging.Envelope
			envelope, publishErr = messaging.Decode(m.Payload)
			if publishErr == nil {
				bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
				publishErr = publisher.Publish(bounded, m.Destination, envelope)
				cancel()
			}
		}
		update, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		if publishErr == nil {
			e = messaging.MarkOutboxPublished(ctx, update, m.MessageID)
		} else {
			e = messaging.ScheduleOutboxRetry(ctx, update, m.MessageID, m.Attempts+1, "publication failed", time.Now().UTC())
			if m.Attempts+1 >= messaging.MaxDeliveryAttempts {
				logger.Error("outbox delivery requires operator recovery", "message_id", m.MessageID)
			}
		}
		if e != nil {
			_ = update.Rollback()
			return e
		}
		if e = update.Commit(); e != nil {
			return e
		}
	}
	return nil
}
