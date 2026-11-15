package results

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"price-crawler/services/crawler/internal/messaging"
)

type CandidateRequest struct {
	OfferID        string `json:"offer_id"`
	CrawlRequestID string `json:"crawl_request_id"`
	CandidateID    string `json:"candidate_id"`
}
type CandidateResult struct {
	CandidateRequest
	Status    string     `json:"status"`
	Candidate *candidate `json:"candidate,omitempty"`
}
type candidate struct {
	CandidateRequest
	NormalizationVersion string                    `json:"normalization_version"`
	Fields               map[string]map[string]any `json:"fields"`
	Attributes           []map[string]any          `json:"attributes"`
	EvidenceExpired      bool                      `json:"evidence_expired"`
}

func missingField() map[string]any { return map[string]any{"status": "missing", "confidence": 0} }
func cleanField(f map[string]any, expired bool) map[string]any {
	if f == nil {
		return missingField()
	}
	if f["status"] != "valid" {
		f["normalized_value"] = nil
	}
	if expired {
		delete(f, "raw_value")
	}
	return f
}

// ReadCandidate resolves the complete identity tuple in one database snapshot.
func ReadCandidate(ctx context.Context, db *sql.DB, org string, req CandidateRequest, now time.Time) CandidateResult {
	result := CandidateResult{CandidateRequest: req, Status: "pending"}
	if db == nil {
		return result
	}
	var fieldsJSON, attrsJSON []byte
	var version string
	var expired bool
	err := db.QueryRowContext(ctx, `
		SELECT c.normalization_version,
       c.field_provenance,
       c.attributes,
       (p.raw_evidence_expired_at IS NOT NULL OR p.raw_evidence_deadline <= $5)
		FROM product_information_candidate c
		JOIN crawl_request r ON r.organization_id = c.organization_id
			AND r.id = c.crawl_request_id
		JOIN page_fetch_outcome p ON p.organization_id = c.organization_id
			AND p.id = c.page_fetch_outcome_id
		WHERE c.organization_id = $1
			AND r.offer_id = $2
			AND r.id = $3
			AND c.id = $4`,
		org, req.OfferID, req.CrawlRequestID, req.CandidateID, now).Scan(&version, &fieldsJSON, &attrsJSON, &expired)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM product_information_candidate WHERE id = $1
		) OR EXISTS (
			SELECT 1 FROM crawl_request WHERE id = $2
				AND (organization_id <> $3 OR (offer_id IS NOT NULL AND offer_id <> $4))
		) OR EXISTS (
			SELECT 1 FROM offer WHERE id = $4 AND organization_id <> $3)`,
			req.CandidateID, req.CrawlRequestID, org, req.OfferID).Scan(&exists)
		if err == nil {
			if exists {
				result.Status = "forbidden"
			} else {
				result.Status = "not_found"
			}
		}
		return result
	}
	if err != nil {
		return result
	}
	var fields map[string]map[string]any
	var attrs []map[string]any
	if json.Unmarshal(fieldsJSON, &fields) != nil || json.Unmarshal(attrsJSON, &attrs) != nil || len(attrs) > 100 || version == "" {
		return result
	}
	selected := map[string]map[string]any{}
	for _, name := range []string{"title", "description", "brand", "manufacturer_model", "gtin"} {
		selected[name] = cleanField(fields[name], expired)
	}
	if attrs == nil {
		attrs = []map[string]any{}
	}
	for _, a := range attrs {
		f, _ := a["field"].(map[string]any)
		a["field"] = cleanField(f, expired)
		if _, ok := a["unit"]; !ok {
			a["unit"] = nil
		}
	}
	result.Status = "found"
	result.Candidate = &candidate{CandidateRequest: req, NormalizationVersion: version, Fields: selected, Attributes: attrs, EvidenceExpired: expired}
	return result
}

// RunCandidateConsumer authenticates the broker user and its explicitly configured tenant scopes.
func RunCandidateConsumer(ctx context.Context, db *sql.DB, brokerURL string, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := consumeCandidates(ctx, db, brokerURL, logger); err != nil && ctx.Err() == nil {
			logger.Warn("candidate reader disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeCandidates(ctx context.Context, db *sql.DB, brokerURL string, logger *slog.Logger) error {
	consumer, err := messaging.NewConsumer(brokerURL, "candidate.read.requested.v1", "crawler-candidate-reader", 1)
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
		case delivery, ok := <-consumer.Deliveries():
			if !ok {
				return errors.New("candidate read queue closed")
			}
			e, err := messaging.DecodeDelivery(delivery)
			var req CandidateRequest
			decoder := json.NewDecoder(bytes.NewReader(e.Payload))
			decoder.DisallowUnknownFields()
			if err != nil || e.MessageType != "candidate.read.requested" || delivery.ReplyTo == "" || len(delivery.ReplyTo) > 255 || delivery.CorrelationId != e.CorrelationID || decoder.Decode(&req) != nil || decoder.Decode(new(any)) != io.EOF || !validUUID(req.OfferID) || !validUUID(req.CrawlRequestID) || !validUUID(req.CandidateID) {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			var scopes map[string][]string
			if err := json.Unmarshal([]byte(os.Getenv("CANDIDATE_READ_IDENTITIES_JSON")), &scopes); err != nil {
				scopes = nil
			}
			authorized := false
			for _, org := range scopes[delivery.UserId] {
				if org == e.Organization && delivery.UserId != "" {
					authorized = true
					break
				}
			}
			result := CandidateResult{CandidateRequest: req, Status: "forbidden"}
			if authorized {
				bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
				result = ReadCandidate(bounded, db, e.Organization, req, time.Now().UTC())
				cancel()
				if result.Status == "pending" {
					logger.Warn("candidate read pending", "organization_id", e.Organization, "candidate_id", req.CandidateID)
				}
			}
			payload, err := json.Marshal(result)
			if err != nil {
				return err
			}
			id, err := newID()
			if err != nil {
				return err
			}
			reply := messaging.Envelope{MessageID: id, MessageType: "candidate.read.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = publisher.Publish(bounded, delivery.ReplyTo, reply)
			cancel()
			if err != nil && !errors.Is(err, messaging.ErrUnroutable) {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
