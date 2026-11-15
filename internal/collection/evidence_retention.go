package collection

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

const evidenceBatchLimit = 1000

// ExpireEvidence redacts expired raw evidence and recovers unfinished finder work.
func ExpireEvidence(ctx context.Context, db *sql.DB, now time.Time, limit int) (int, error) {
	if db == nil || now.IsZero() || limit < 1 || limit > evidenceBatchLimit {
		return 0, errors.New("invalid evidence retention request")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	// Lock the request before its page, as the finder result handler does.
	rows, err := tx.QueryContext(ctx, `SELECT p.organization_id::text,p.id::text,p.crawl_request_id::text FROM crawl_request r JOIN page_fetch_outcome p ON p.organization_id=r.organization_id AND p.crawl_request_id=r.id WHERE p.raw_evidence_deadline <= $1 AND p.raw_evidence_expired_at IS NULL ORDER BY p.raw_evidence_deadline,p.id FOR UPDATE OF r,p SKIP LOCKED LIMIT $2`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	type page struct{ org, id, request string }
	var due []page
	for rows.Next() {
		var p page
		if err = rows.Scan(&p.org, &p.id, &p.request); err != nil {
			_ = rows.Close()
			return 0, err
		}
		due = append(due, p)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	for _, p := range due {
		// In-flight senders hold this row lock until their publish completes.
		if _, err = tx.ExecContext(ctx, `UPDATE message_outbox SET payload=jsonb_set(payload,'{payload,page}','null'::jsonb), dead_lettered_at=CASE WHEN published_at IS NULL THEN $3 ELSE dead_lettered_at END, claimed_until=NULL, last_error=CASE WHEN published_at IS NULL THEN 'raw evidence expired; crawl requeued' ELSE last_error END WHERE organization_id=$1 AND destination='product.find.requested.v1' AND payload->'payload'->>'page_parsed_message_id'=$2`, p.org, p.id, now.UTC()); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='queued',next_attempt_at=NULL,last_error_code='evidence_expired',updated_at=$4 WHERE organization_id=$1 AND id::text=$2 AND state='finding' AND $3=(SELECT payload->'payload'->>'page_parsed_message_id' FROM message_outbox WHERE organization_id=$1 AND destination='product.find.requested.v1' AND payload->'payload'->>'crawl_request_id'=$2 ORDER BY created_at DESC,message_id DESC LIMIT 1)`, p.org, p.request, p.id, now.UTC()); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE product_information_candidate SET field_provenance=redact_raw_evidence(field_provenance),attributes=redact_raw_evidence(attributes) WHERE organization_id=$1 AND page_fetch_outcome_id::text=$2`, p.org, p.id); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE price_availability_observation SET provenance=redact_raw_evidence(provenance) WHERE organization_id=$1 AND crawl_request_id::text=$2 AND EXISTS (SELECT 1 FROM product_information_candidate c WHERE c.organization_id=$1 AND c.crawl_request_id::text=$2 AND c.page_fetch_outcome_id::text=$3)`, p.org, p.request, p.id); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE page_fetch_outcome SET bounded_evidence=NULL,evidence_hash=NULL,evidence_expires_at=NULL,raw_evidence_expired_at=$3 WHERE organization_id=$1 AND id::text=$2`, p.org, p.id, now.UTC()); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(due), nil
}

// RunEvidenceRetention performs a startup sweep and then repeats at a bounded
// interval. The injected clock in ExpireEvidence keeps expiry decisions
// deterministic in tests.
func RunEvidenceRetention(ctx context.Context, db *sql.DB, interval time.Duration, batchSize int, logger *slog.Logger) {
	if interval <= 0 {
		interval = time.Hour
	}
	if batchSize < 1 || batchSize > evidenceBatchLimit {
		batchSize = 100
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for ctx.Err() == nil {
				count, err := ExpireEvidence(ctx, db, time.Now().UTC(), batchSize)
				if err != nil {
					if logger != nil {
						logger.Warn("evidence retention sweep failed", "error", err)
					}
					break
				}
				if count < batchSize {
					break
				}
			}
			timer.Reset(interval)
		}
	}
}
