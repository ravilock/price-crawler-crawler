package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"sort"
	"strings"
)

var ErrConflict = errors.New("idempotency key conflicts with an existing request")

type Intent struct{ ID, Organization, URL, Reason, IdempotencyKey, CorrelationID, SourceID, Category, RequesterID string }
type Receipt struct {
	ID, State string
	Created   bool
}

// Submit coalesces active intents for the same tenant and normalized URL. The
// transaction locks both the idempotency key and URL, including across service
// processes. Existing idempotency keys never change their original meaning.
func Submit(ctx context.Context, db *sql.DB, intent Intent) (Receipt, error) {
	if db == nil || intent.ID == "" || intent.Organization == "" || len(intent.IdempotencyKey) < 1 || len(intent.IdempotencyKey) > 200 || (intent.RequesterID != "" && !commandUUID.MatchString(intent.RequesterID)) || (intent.Reason != "manual" && intent.Reason != "scheduled" && intent.Reason != "discovery") {
		return Receipt{}, errors.New("invalid crawl intent")
	}
	if (intent.SourceID == "") != (intent.Category == "") || len(intent.Category) > 100 {
		return Receipt{}, errors.New("invalid crawl scope")
	}
	u, err := url.Parse(intent.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Hostname() == "" || net.ParseIP(u.Hostname()) != nil || u.Hostname() != strings.ToLower(u.Hostname()) || u.Fragment != "" {
		return Receipt{}, errors.New("invalid crawl URL")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	keys := []string{"crawl-key/" + intent.Organization + "/" + intent.IdempotencyKey, "crawl-url/" + intent.Organization + "/" + intent.URL}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
			return Receipt{}, err
		}
	}
	var result Receipt
	var existingURL, existingReason, existingSource, existingCategory, existingRequester string
	err = tx.QueryRowContext(ctx, `SELECT r.id::text,r.state,k.submitted_url,k.reason,COALESCE(k.source_domain_id::text,''),COALESCE(k.category,''),COALESCE(k.requester_id,'') FROM crawl_intent_key k JOIN crawl_request r ON r.organization_id=k.organization_id AND r.id=k.crawl_request_id WHERE k.organization_id=$1 AND k.idempotency_key=$2`, intent.Organization, intent.IdempotencyKey).Scan(&result.ID, &result.State, &existingURL, &existingReason, &existingSource, &existingCategory, &existingRequester)
	if err == nil {
		if existingURL != intent.URL || existingReason != intent.Reason || existingSource != intent.SourceID || existingCategory != intent.Category || (existingRequester != "" && intent.RequesterID != existingRequester) {
			return Receipt{}, ErrConflict
		}
		if intent.RequesterID != "" && existingRequester == "" {
			if _, err = tx.ExecContext(ctx, `UPDATE crawl_intent_key SET requester_id=$3 WHERE organization_id=$1 AND idempotency_key=$2`, intent.Organization, intent.IdempotencyKey, intent.RequesterID); err != nil {
				return Receipt{}, err
			}
		}
		return result, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, err
	}
	err = tx.QueryRowContext(ctx, `SELECT id::text,state,COALESCE(source_domain_id::text,''),COALESCE(category,'') FROM crawl_request WHERE organization_id=$1 AND submitted_url=$2 AND state IN ('queued','fetching','rendering','finding','retry_wait','paused') ORDER BY created_at,id LIMIT 1`, intent.Organization, intent.URL).Scan(&result.ID, &result.State, &existingSource, &existingCategory)
	if err == nil {
		if existingSource != intent.SourceID || existingCategory != intent.Category {
			return Receipt{}, ErrConflict
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO crawl_intent_key(organization_id,idempotency_key,crawl_request_id,submitted_url,reason,source_domain_id,category,requester_id) VALUES($1,$2,$3,$4,$5,NULLIF($6,'')::uuid,NULLIF($7,''),NULLIF($8,''))`, intent.Organization, intent.IdempotencyKey, result.ID, intent.URL, intent.Reason, intent.SourceID, intent.Category, intent.RequesterID); err != nil {
			return Receipt{}, err
		}
		return result, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO crawl_request(id,organization_id,submitted_url,reason,state,idempotency_key,correlation_id,source_domain_id,category) VALUES($1,$2,$3,$4,'queued',$5,NULLIF($6,'')::uuid,NULLIF($7,'')::uuid,NULLIF($8,''))`, intent.ID, intent.Organization, intent.URL, intent.Reason, intent.IdempotencyKey, intent.CorrelationID, intent.SourceID, intent.Category)
	if err != nil {
		return Receipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO crawl_intent_key(organization_id,idempotency_key,crawl_request_id,submitted_url,reason,source_domain_id,category,requester_id) VALUES($1,$2,$3,$4,$5,NULLIF($6,'')::uuid,NULLIF($7,''),NULLIF($8,''))`, intent.Organization, intent.IdempotencyKey, intent.ID, intent.URL, intent.Reason, intent.SourceID, intent.Category, intent.RequesterID); err != nil {
		return Receipt{}, err
	}
	result = Receipt{ID: intent.ID, State: "queued", Created: true}
	return result, tx.Commit()
}
