package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"price-crawler/services/crawler/internal/messaging"
)

type Result struct {
	Status             string       `json:"status"`
	RequestID          string       `json:"request_id,omitempty"`
	SourceID           string       `json:"source_id,omitempty"`
	State              string       `json:"state,omitempty"`
	ApprovedCategories []string     `json:"approved_categories,omitempty"`
	PathExceptionID    string       `json:"path_exception_id,omitempty"`
	AuditID            string       `json:"audit_id,omitempty"`
	Error              *ResultError `json:"error,omitempty"`
}
type ResultError struct {
	Code string `json:"code"`
}

func failure(code string) Result { return Result{Status: code, Error: &ResultError{Code: code}} }
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// Execute atomically stores source changes, an audit event, and a replay receipt.
// The caller must supply an independently verified principal.
func Execute(ctx context.Context, db *sql.DB, e messaging.Envelope, verified Principal) (Result, error) {
	if db == nil {
		return Result{}, errors.New("database required")
	}
	if e.MessageType != "source.admin.command" || e.Validate() != nil {
		return failure("invalid"), nil
	}
	c, err := ParseCommand(e.Payload, e.Organization, verified)
	if errors.Is(err, ErrForbidden) {
		return failure("forbidden"), nil
	}
	if err != nil {
		return failure("invalid"), nil
	}
	if c.Action != "source.read" && e.IdempotencyKey == "" {
		return failure("invalid"), nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var hash string
	if c.Action != "source.read" {
		canonical, _ := json.Marshal(c)
		sum := sha256.Sum256(canonical)
		hash = fmt.Sprintf("%x", sum)
		key := e.Organization + "/" + verified.Subject + "/" + c.Action + "/" + e.IdempotencyKey
		if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
			return Result{}, err
		}
		var old string
		var raw []byte
		err = tx.QueryRowContext(ctx, `SELECT request_hash,result FROM source_admin_receipt WHERE organization_id=$1 AND actor_id=$2 AND action=$3 AND idempotency_key=$4`, e.Organization, verified.Subject, c.Action, e.IdempotencyKey).Scan(&old, &raw)
		if err == nil {
			if old != hash {
				return failure("conflict"), nil
			}
			var r Result
			if err = json.Unmarshal(raw, &r); err != nil {
				return Result{}, err
			}
			return r, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Result{}, err
		}
	}
	result, err := apply(ctx, tx, e, c)
	if err != nil {
		return Result{}, err
	}
	if c.Action != "source.read" && (result.Status == "ok" || result.Status == "pending") {
		raw, _ := json.Marshal(result)
		if _, err = tx.ExecContext(ctx, `INSERT INTO source_admin_receipt(organization_id,actor_id,action,idempotency_key,request_hash,result) VALUES($1,$2,$3,$4,$5,$6)`, e.Organization, verified.Subject, c.Action, e.IdempotencyKey, hash, raw); err != nil {
			return Result{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return Result{}, err
	}
	return result, nil
}
func apply(ctx context.Context, tx *sql.Tx, e messaging.Envelope, c Command) (Result, error) {
	var before json.RawMessage
	if c.Action != "inclusion.request" {
		var state string
		var active bool
		var cats, requested []byte
		err := tx.QueryRowContext(ctx, `SELECT r.state,s.is_active,s.approved_categories,r.requested_categories,to_jsonb(s) FROM source_domain_config s JOIN source_admin_review r ON r.organization_id=s.organization_id AND r.source_domain_id=s.id WHERE s.organization_id=$1 AND s.id::text=$2 FOR UPDATE OF s,r`, e.Organization, c.SourceID).Scan(&state, &active, &cats, &requested, &before)
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM source_domain_config WHERE id::text=$1)`, c.SourceID).Scan(&exists); err != nil {
				return Result{}, err
			}
			if exists {
				return failure("forbidden"), nil
			}
			return failure("not_found"), nil
		}
		if err != nil {
			return Result{}, err
		}
		if c.Action == "source.read" {
			var approved []string
			if err = json.Unmarshal(cats, &approved); err != nil {
				return Result{}, err
			}
			return Result{Status: "ok", SourceID: c.SourceID, State: state, ApprovedCategories: approved}, nil
		}
		if c.Action == "inclusion.review" && c.Decision == "approve" {
			var requestedCategories []string
			if err = json.Unmarshal(requested, &requestedCategories); err != nil {
				return Result{}, err
			}
			allowed := map[string]bool{}
			for _, v := range requestedCategories {
				allowed[v] = true
			}
			for _, v := range c.ApprovedCategories {
				if !allowed[v] {
					return failure("invalid"), nil
				}
			}
		}
		if c.Action == "robots.path_exception.create" && (state != "approved" || !active) {
			return failure("conflict"), nil
		}
	}
	var result Result
	var entityID, entityType string
	switch c.Action {
	case "inclusion.request":
		id, err := newID()
		if err != nil {
			return Result{}, err
		}
		cats, _ := json.Marshal(c.RequestedCategories)
		var inserted string
		err = tx.QueryRowContext(ctx, `INSERT INTO source_domain_config(id,organization_id,domain,max_requests_per_minute) VALUES($1,$2,$3,1) ON CONFLICT(organization_id,domain) DO NOTHING RETURNING id::text`, id, e.Organization, c.Domain).Scan(&inserted)
		if errors.Is(err, sql.ErrNoRows) {
			return failure("conflict"), nil
		}
		if err != nil {
			return Result{}, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO source_admin_review(organization_id,source_domain_id,requested_categories,requested_by,state) VALUES($1,$2,$3,$4,'pending_review')`, e.Organization, id, cats, c.Actor.Subject); err != nil {
			return Result{}, err
		}
		result = Result{Status: "pending", RequestID: id}
		entityID = id
		entityType = "source"
	case "inclusion.review":
		approved := c.Decision == "approve"
		state := "rejected"
		if approved {
			state = "approved"
		}
		cats := c.ApprovedCategories
		if !approved {
			cats = []string{}
		}
		encodedCats, _ := json.Marshal(cats)
		review, _ := json.Marshal(c.Review)
		if _, err := tx.ExecContext(ctx, `UPDATE source_domain_config SET is_active=$3,approved_categories=$4,adapter_name=$5,max_requests_per_minute=$6,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, c.SourceID, approved, encodedCats, c.Review.SupportedIntegration, c.Review.RequestPacePerMinute); err != nil {
			return Result{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE source_admin_review SET state=$3,reviewed_by=$4,review=$5,updated_at=now() WHERE organization_id=$1 AND source_domain_id::text=$2`, e.Organization, c.SourceID, state, c.Actor.Subject, review); err != nil {
			return Result{}, err
		}
		result = Result{Status: "ok", SourceID: c.SourceID, State: state, ApprovedCategories: cats}
		entityID = c.SourceID
		entityType = "source"
	case "robots.path_exception.create":
		id, err := newID()
		if err != nil {
			return Result{}, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO source_path_override(id,organization_id,source_domain_id,path_pattern,actor_id,reason) VALUES($1,$2,$3,$4,$5,$6)`, id, e.Organization, c.SourceID, c.Path, c.Actor.Subject, c.Reason); err != nil {
			return Result{}, err
		}
		result = Result{Status: "ok", PathExceptionID: id}
		entityID = id
		entityType = "source_path_override"
		before = nil
	default:
		return failure("invalid"), nil
	}
	auditID, err := newID()
	if err != nil {
		return Result{}, err
	}
	now := time.Now().UTC()
	after, _ := json.Marshal(map[string]any{"command": c, "result": result})
	var beforeValue any
	if len(before) > 0 {
		beforeValue = before
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO source_admin_audit(id,organization_id,actor_id,actor_role,action,entity_type,entity_id,before_value,after_value,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, auditID, e.Organization, c.Actor.Subject, c.Actor.Role, c.Action, entityType, entityID, beforeValue, after, now); err != nil {
		return Result{}, err
	}
	payload, err := json.Marshal(map[string]any{"actor": c.Actor, "organization_id": e.Organization, "action": c.Action, "entity": map[string]string{"type": entityType, "id": entityID}, "before": beforeValue, "after": json.RawMessage(after), "occurred_at": now})
	if err != nil {
		return Result{}, err
	}
	event := messaging.Envelope{MessageID: auditID, MessageType: "audit.event.recorded", SchemaVersion: 1, OccurredAt: now, Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
	if err = messaging.EnqueueOutbox(ctx, tx, event, "audit.event.recorded.v1"); err != nil {
		return Result{}, err
	}
	if c.Action == "robots.path_exception.create" {
		result.AuditID = auditID
	}
	return result, nil
}
