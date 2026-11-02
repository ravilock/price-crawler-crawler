package collection

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

const evidenceBatchLimit = 1000

// ExpireEvidence clears bounded excerpts, page hashes, and their expiry
// references in a locked, bounded batch. Candidate provenance and observations
// are deliberately outside this deletion boundary.
func ExpireEvidence(ctx context.Context, db *sql.DB, now time.Time, limit int) (int, error) {
	if db == nil || now.IsZero() || limit < 1 || limit > evidenceBatchLimit {
		return 0, errors.New("invalid evidence retention request")
	}
	result, err := db.ExecContext(ctx, `WITH due AS (
		SELECT id, organization_id FROM page_fetch_outcome
		WHERE evidence_expires_at IS NOT NULL AND evidence_expires_at <= $1
		  AND (bounded_evidence IS NOT NULL OR evidence_hash IS NOT NULL)
		ORDER BY evidence_expires_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	)
	UPDATE page_fetch_outcome p
	SET bounded_evidence=NULL, evidence_hash=NULL, evidence_expires_at=NULL
	FROM due
	WHERE p.organization_id=due.organization_id AND p.id=due.id`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	return int(count), err
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
