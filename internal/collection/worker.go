package collection

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"net/url"
	"sync"
	"time"

	"price-crawler/services/crawler/internal/adapters"
	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/policy"
	"price-crawler/services/crawler/internal/scheduler"
)

type job struct{ ID, Organization, URL, SourceID, Category, Correlation, Lease string }

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func claim(ctx context.Context, db *sql.DB) (job, error) {
	token, err := newID()
	if err != nil {
		return job{}, err
	}
	var j job
	err = db.QueryRowContext(ctx, `WITH candidate AS (SELECT organization_id,id FROM crawl_request WHERE source_domain_id IS NOT NULL AND category IS NOT NULL AND correlation_id IS NOT NULL AND (state='queued' OR (state='retry_wait' AND next_attempt_at<=now()) OR (state='fetching' AND lease_until<now())) ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE crawl_request r SET state='fetching',lease_token=$1,lease_until=now()+interval '3 minutes',updated_at=now() FROM candidate c WHERE r.organization_id=c.organization_id AND r.id=c.id RETURNING r.id::text,r.organization_id::text,r.submitted_url,r.source_domain_id::text,r.category,r.correlation_id::text,r.lease_token::text`, token).Scan(&j.ID, &j.Organization, &j.URL, &j.SourceID, &j.Category, &j.Correlation, &j.Lease)
	return j, err
}

// Run uses bounded workers and persistent leases. Only scoped jobs are eligible;
// legacy unscoped requests cannot initiate network access.
func Run(ctx context.Context, db *sql.DB, local *scheduler.Budget, workers int, logger *slog.Logger) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				j, err := claim(ctx, db)
				if errors.Is(err, sql.ErrNoRows) {
					timer := time.NewTimer(500 * time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						logger.Warn("collection claim failed", "error", err)
					}
					timer := time.NewTimer(time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					continue
				}
				bounded, cancel := context.WithTimeout(ctx, 110*time.Second)
				err = process(bounded, db, local, j)
				cancel()
				if err != nil && ctx.Err() == nil {
					logger.Warn("collection attempt failed", "request_id", j.ID, "error", err)
				}
			}
		}()
	}
	wg.Wait()
}
func process(ctx context.Context, db *sql.DB, local *scheduler.Budget, j job) error {
	fetcher := policy.AuthorizedFetcher{DB: db, Local: local, Organization: j.Organization, SourceID: j.SourceID, Category: j.Category}
	page, err := adapters.Collect(ctx, fetcher, j.URL, time.Now().UTC())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return retry(ctx, db, j, "timeout", "")
	}
	switch page.Outcome {
	case "timeout", "rate_limited", "server_error":
		return retry(ctx, db, j, page.Outcome, page.RetryAfter)
	case "unsupported_page":
		var allowed bool
		if err = db.QueryRowContext(ctx, `SELECT rendering_allowed FROM source_domain_config WHERE organization_id=$1 AND id::text=$2 AND is_active`, j.Organization, j.SourceID).Scan(&allowed); err != nil {
			return err
		}
		if allowed {
			return requestRender(ctx, db, j)
		}
		return finishFailure(ctx, db, j, page.Outcome)
	case "blocked", "invalid_url", "not_found":
		return finishFailure(ctx, db, j, page.Outcome)
	case "partial", "parsed":
		return persistPage(ctx, db, j, page)
	default:
		return finishFailure(ctx, db, j, "unsupported_page")
	}
}
func retry(ctx context.Context, db *sql.DB, j job, outcome, retryAfter string) error {
	u, err := url.Parse(j.URL)
	if err != nil {
		return err
	}
	_, err = scheduler.ScheduleRetry(ctx, db, j.Organization, j.ID, u.Hostname(), outcome, retryAfter, time.Now().UTC(), mathrand.Float64(), j.Lease)
	return err
}
func finishFailure(ctx context.Context, db *sql.DB, j job, outcome string) error {
	result, err := db.ExecContext(ctx, `UPDATE crawl_request SET state='failed',last_error_code=$4,lease_token=NULL,lease_until=NULL,updated_at=now() WHERE organization_id=$1 AND id::text=$2 AND lease_token::text=$3 AND state='fetching'`, j.Organization, j.ID, j.Lease, outcome)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("crawl lease lost")
	}
	return nil
}
func persistPage(ctx context.Context, db *sql.DB, j job, page adapters.Page) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var state, token string
	if err = tx.QueryRowContext(ctx, `SELECT state,COALESCE(lease_token::text,'') FROM crawl_request WHERE organization_id=$1 AND id::text=$2 FOR UPDATE`, j.Organization, j.ID).Scan(&state, &token); err != nil {
		return err
	}
	if state != "fetching" || token != j.Lease {
		return errors.New("crawl lease lost")
	}
	pageID, err := newID()
	if err != nil {
		return err
	}
	messageID, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,final_url,mode,http_status,content_type,observed_at,outcome,evidence_hash,bounded_evidence,evidence_expires_at,adapter_version) VALUES($1,$2,$3,$4,$5,$6,'http',$7,$8,$9,'partial',$10,$11,$12,'http-v1')`, pageID, j.Organization, j.ID, j.SourceID, page.RequestedURL, page.FinalURL, page.HTTPStatus, page.ContentType, page.ObservedAt, page.EvidenceHash, page.EvidenceExcerpt, page.EvidenceExpiresAt)
	if err != nil {
		return err
	}
	parsed := map[string]any{"crawl_request_id": j.ID, "requested_url": page.RequestedURL, "final_url": page.FinalURL, "outcome": "partial", "observed_at": page.ObservedAt, "parser_version": "http-v1", "page_title": page.Title, "page_description": page.Description, "fields": page.Fields, "structured_data": page.StructuredData}
	payload, err := json.Marshal(map[string]any{"crawl_request_id": j.ID, "page_parsed_message_id": pageID, "normalization_profile": "1", "page": parsed})
	if err != nil {
		return err
	}
	envelope := messaging.Envelope{MessageID: messageID, MessageType: "product.find.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: j.Organization, CorrelationID: j.Correlation, CausationID: pageID, Payload: payload}
	if err = messaging.EnqueueOutbox(ctx, tx, envelope, "product.find.requested.v1"); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='finding',lease_token=NULL,lease_until=NULL,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, j.Organization, j.ID)
	if err != nil {
		return err
	}
	return tx.Commit()
}
