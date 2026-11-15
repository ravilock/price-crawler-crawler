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
		// Re-read under a row lock: retention may have redacted the message
		// since it was claimed. Keep the lock until broker confirmation.
		update, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		var payload []byte
		var canPublish bool
		e = update.QueryRowContext(ctx, `SELECT o.payload, CASE WHEN o.destination='product.find.requested.v1' THEN EXISTS (SELECT 1 FROM page_fetch_outcome p WHERE p.organization_id=o.organization_id AND p.id::text=o.payload->'payload'->>'page_parsed_message_id' AND p.raw_evidence_expired_at IS NULL AND p.raw_evidence_deadline > clock_timestamp()+interval '5 seconds') ELSE true END FROM message_outbox o WHERE o.message_id=$1 AND o.published_at IS NULL AND o.dead_lettered_at IS NULL FOR UPDATE OF o`, m.MessageID).Scan(&payload, &canPublish)
		if e == sql.ErrNoRows {
			_ = update.Rollback()
			continue
		}
		if e != nil {
			_ = update.Rollback()
			return e
		}
		if !canPublish {
			_ = update.Rollback()
			continue
		}
		var publishErr error
		if err != nil {
			publishErr = err
		} else {
			var envelope messaging.Envelope
			envelope, publishErr = messaging.Decode(payload)
			if publishErr == nil {
				bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
				publishErr = publisher.Publish(bounded, m.Destination, envelope)
				cancel()
			}
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
