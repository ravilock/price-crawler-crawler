package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RetryDecision returns the next attempt time for at most three collection
// retries. jitter is injected as a fraction in [0,1]; it is never used for
// terminal failures. Delivery retries are managed separately by messaging.
func RetryDecision(outcome string, attempts int, retryAfter string, now time.Time, jitter float64) (time.Time, bool) {
	if attempts < 0 || attempts >= 3 || jitter < 0 || jitter > 1 {
		return time.Time{}, false
	}
	switch outcome {
	case "timeout", "rate_limited", "server_error":
	default:
		return time.Time{}, false
	}
	delay := time.Second * time.Duration(1<<attempts)
	delay += time.Duration(float64(delay) * jitter)
	next := now.Add(delay)
	if outcome == "rate_limited" && strings.TrimSpace(retryAfter) != "" {
		raw := strings.TrimSpace(retryAfter)
		var until time.Time
		if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
			if seconds > 3600 {
				return time.Time{}, false
			}
			until = now.Add(time.Duration(seconds) * time.Second)
		} else if value, err := http.ParseTime(raw); err == nil && value.After(now) {
			if value.After(now.Add(time.Hour)) {
				return time.Time{}, false
			}
			until = value
		}
		if until.After(next) {
			next = until
		}
	}
	return next, true
}

// ScheduleRetry atomically records the next collection attempt and a shared
// domain cooldown. It rejects terminal or non-active requests.
func ScheduleRetry(ctx context.Context, db *sql.DB, organization, requestID, domain, outcome, retryAfter string, now time.Time, jitter float64) (bool, error) {
	if db == nil || organization == "" || requestID == "" || domain == "" || domain != strings.ToLower(domain) {
		return false, errors.New("invalid retry request")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var attempts int
	var state, submittedURL string
	if err = tx.QueryRowContext(ctx, `SELECT attempt_count,state,submitted_url FROM crawl_request WHERE organization_id=$1 AND id::text=$2 FOR UPDATE`, organization, requestID).Scan(&attempts, &state, &submittedURL); err != nil {
		return false, err
	}
	parsed, parseErr := url.Parse(submittedURL)
	if parseErr != nil || parsed.Hostname() != domain {
		return false, errors.New("retry domain does not match request")
	}
	if state != "fetching" && state != "rendering" && state != "finding" {
		return false, errors.New("crawl request is not active")
	}
	next, retry := RetryDecision(outcome, attempts, retryAfter, now, jitter)
	if !retry {
		_, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='failed',last_error_code=$3,updated_at=$4 WHERE organization_id=$1 AND id::text=$2`, organization, requestID, outcome, now)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='retry_wait',attempt_count=attempt_count+1,next_attempt_at=$3,last_error_code=$4,updated_at=$5 WHERE organization_id=$1 AND id::text=$2`, organization, requestID, next, outcome, now)
		if err == nil && outcome == "rate_limited" {
			_, err = tx.ExecContext(ctx, `INSERT INTO source_domain_budget(domain,cooldown_until) VALUES($1,$2) ON CONFLICT(domain) DO UPDATE SET cooldown_until=GREATEST(source_domain_budget.cooldown_until,EXCLUDED.cooldown_until)`, domain, next)
		}
	}
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return retry, nil
}
