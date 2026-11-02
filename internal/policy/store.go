package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// LoadSource reads approval and exceptions from one consistent tenant-scoped
// database snapshot. The configured slow limit is supplied by the operator.
func LoadSource(ctx context.Context, db *sql.DB, organization, sourceID string, slowPerMinute int) (Source, []Override, error) {
	if db == nil || organization == "" || sourceID == "" || slowPerMinute < 1 || slowPerMinute > 60000 {
		return Source{}, nil, errors.New("invalid source policy request")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return Source{}, nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var s Source
	var categories, review []byte
	var state string
	var delay int64
	var adapter sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT s.organization_id::text,s.domain,s.is_active,s.approved_categories,s.max_requests_per_minute,s.crawl_delay_seconds,s.adapter_name,r.state,r.review FROM source_domain_config s JOIN source_admin_review r ON r.organization_id=s.organization_id AND r.source_domain_id=s.id WHERE s.organization_id=$1 AND s.id::text=$2`, organization, sourceID).Scan(&s.Organization, &s.Domain, &s.Active, &categories, &s.RequestsPerMinute, &delay, &adapter, &state, &review)
	if errors.Is(err, sql.ErrNoRows) {
		return Source{}, nil, ErrDenied
	}
	if err != nil {
		return Source{}, nil, err
	}
	var attestations struct {
		Access      bool   `json:"access_rules_reviewed"`
		Terms       bool   `json:"terms_reviewed"`
		Integration string `json:"supported_integration"`
	}
	if json.Unmarshal(categories, &s.Categories) != nil || json.Unmarshal(review, &attestations) != nil {
		return Source{}, nil, errors.New("invalid persisted source review")
	}
	s.Active = s.Active && state == "approved"
	s.AccessReviewed = attestations.Access
	s.TermsReviewed = attestations.Terms
	s.IntegrationSupported = adapter.Valid && adapter.String != "" && adapter.String == attestations.Integration
	s.CrawlDelay = time.Duration(delay) * time.Second
	s.SlowRequestsPerMinute = slowPerMinute
	rows, err := tx.QueryContext(ctx, `SELECT path_pattern,actor_id,reason,expires_at,revoked_at FROM source_path_override WHERE organization_id=$1 AND source_domain_id::text=$2`, organization, sourceID)
	if err != nil {
		return Source{}, nil, err
	}
	var overrides []Override
	for rows.Next() {
		var o Override
		var expires, revoked sql.NullTime
		if err = rows.Scan(&o.Path, &o.Actor, &o.Reason, &expires, &revoked); err != nil {
			_ = rows.Close()
			return Source{}, nil, err
		}
		if expires.Valid {
			o.ExpiresAt = expires.Time
		}
		if revoked.Valid {
			o.RevokedAt = revoked.Time
		}
		overrides = append(overrides, o)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return Source{}, nil, err
	}
	if err = rows.Close(); err != nil {
		return Source{}, nil, err
	}
	if err = tx.Commit(); err != nil {
		return Source{}, nil, err
	}
	return s, overrides, nil
}

// SQLRecorder updates source snapshot metadata and invokes the durable alert
// writer within the same transaction. A missing alert writer is an error for an
// unreadable response, so callers cannot silently drop required alerts.
type SQLRecorder struct {
	DB         *sql.DB
	WriteAlert func(context.Context, *sql.Tx, string, string, RobotsRecord) error
}

func (s SQLRecorder) RecordRobots(ctx context.Context, organization, sourceID string, record RobotsRecord) error {
	if s.DB == nil || organization == "" || sourceID == "" || record.CheckedAt.IsZero() || !record.ExpiresAt.After(record.CheckedAt) || (record.Status != "readable" && record.Status != "unreadable") || (record.Status == "unreadable") != record.AlertAdministrator || (record.Status == "readable" && record.ContentHash == "") {
		return errors.New("invalid robots record")
	}
	if record.AlertAdministrator && s.WriteAlert == nil {
		return errors.New("durable robots alert writer required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE source_domain_config SET robots_status=$3,robots_checked_at=$4,robots_expires_at=$5,robots_content_hash=NULLIF($6,''),updated_at=now() WHERE organization_id=$1 AND id::text=$2 AND is_active`, organization, sourceID, record.Status, record.CheckedAt, record.ExpiresAt, record.ContentHash)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrDenied
	}
	if record.AlertAdministrator {
		if err = s.WriteAlert(ctx, tx, organization, sourceID, record); err != nil {
			return err
		}
	}
	return tx.Commit()
}
