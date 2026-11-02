package collection

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"price-crawler/services/crawler/internal/messaging"
)

var ErrInvalidFinderResult = errors.New("invalid finder result")

type finderField struct {
	RawValue        any     `json:"raw_value"`
	NormalizedValue any     `json:"normalized_value"`
	EvidenceRef     *string `json:"evidence_ref"`
	Status          string  `json:"status"`
	Confidence      float64 `json:"confidence"`
	Reason          *string `json:"reason"`
}
type finderOffer struct {
	PriceCents    *int64       `json:"price_cents"`
	Currency      string       `json:"currency"`
	PaymentMethod *string      `json:"payment_method"`
	Availability  string       `json:"availability"`
	PriceStatus   string       `json:"price_status"`
	PriceField    *finderField `json:"price_field"`
}
type finderPayload struct {
	CrawlRequestID       string                 `json:"crawl_request_id"`
	NormalizationVersion string                 `json:"normalization_version"`
	Fields               map[string]finderField `json:"fields"`
	Attributes           []json.RawMessage      `json:"attributes"`
	Offer                finderOffer            `json:"offer"`
}

func validField(f finderField) bool {
	return (f.Status == "valid" || f.Status == "missing" || f.Status == "invalid" || f.Status == "uncertain") && f.Confidence >= 0 && f.Confidence <= 1 && (f.EvidenceRef == nil || len(*f.EvidenceRef) <= 500) && (f.Reason == nil || len(*f.Reason) <= 500)
}
func fieldString(fields map[string]finderField, name string) any {
	f, ok := fields[name]
	if !ok || f.Status != "valid" {
		return nil
	}
	s, ok := f.NormalizedValue.(string)
	if !ok || s == "" || len(s) > 5000 {
		return nil
	}
	return s
}

// ApplyFinder atomically persists candidate, immutable observation, inbox and
// outbox; duplicate or late results cannot produce another observation.
func ApplyFinder(ctx context.Context, db *sql.DB, e messaging.Envelope) error {
	if db == nil || e.Validate() != nil || e.MessageType != "product.information.found" || e.CausationID == "" {
		return ErrInvalidFinderResult
	}
	d := json.NewDecoder(bytes.NewReader(e.Payload))
	d.DisallowUnknownFields()
	var p finderPayload
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || p.CrawlRequestID == "" || p.NormalizationVersion == "" || len(p.NormalizationVersion) > 100 || p.Fields == nil || len(p.Fields) > 500 || p.Attributes == nil || len(p.Attributes) > 500 {
		return ErrInvalidFinderResult
	}
	for _, f := range p.Fields {
		if !validField(f) {
			return ErrInvalidFinderResult
		}
	}
	o := p.Offer
	if o.Availability != "available" && o.Availability != "unavailable" && o.Availability != "unknown" {
		return ErrInvalidFinderResult
	}
	if o.Currency != "" && o.Currency != "BRL" {
		return ErrInvalidFinderResult
	}
	if o.PriceStatus != "valid" && o.PriceStatus != "missing" && o.PriceStatus != "invalid" && o.PriceStatus != "uncertain" {
		return ErrInvalidFinderResult
	}
	if o.PriceField != nil && !validField(*o.PriceField) {
		return ErrInvalidFinderResult
	}
	if o.PriceStatus == "valid" && (o.PriceCents == nil || *o.PriceCents < 0 || o.PaymentMethod == nil || *o.PaymentMethod == "") {
		return ErrInvalidFinderResult
	}
	if o.PriceStatus != "valid" && o.PriceCents != nil {
		return ErrInvalidFinderResult
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	fresh, err := messaging.BeginInbox(ctx, tx, e.MessageID, "crawler-finder-result")
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit()
	}
	var pageID string
	err = tx.QueryRowContext(ctx, `SELECT payload->'payload'->>'page_parsed_message_id' FROM message_outbox WHERE organization_id=$1 AND message_id::text=$2 AND destination='product.find.requested.v1' AND correlation_id::text=$3 AND payload->'payload'->>'crawl_request_id'=$4`, e.Organization, e.CausationID, e.CorrelationID, p.CrawlRequestID).Scan(&pageID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidFinderResult
	}
	if err != nil {
		return err
	}
	var state, sourceID, submittedURL string
	err = tx.QueryRowContext(ctx, `SELECT state,source_domain_id::text,submitted_url FROM crawl_request WHERE organization_id=$1 AND id::text=$2 FOR UPDATE`, e.Organization, p.CrawlRequestID).Scan(&state, &sourceID, &submittedURL)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidFinderResult
	}
	if err != nil {
		return err
	}
	if state == "completed" {
		return tx.Commit()
	}
	if state != "finding" {
		return ErrInvalidFinderResult
	}
	var observed time.Time
	var adapterVersion string
	if err = tx.QueryRowContext(ctx, `SELECT observed_at,COALESCE(adapter_version,renderer_version,'') FROM page_fetch_outcome WHERE organization_id=$1 AND id::text=$2 AND crawl_request_id::text=$3`, e.Organization, pageID, p.CrawlRequestID).Scan(&observed, &adapterVersion); err != nil {
		return err
	}
	candidateID, err := newID()
	if err != nil {
		return err
	}
	offerID, err := newID()
	if err != nil {
		return err
	}
	observationID, err := newID()
	if err != nil {
		return err
	}
	eventID, err := newID()
	if err != nil {
		return err
	}
	fields, err := json.Marshal(p.Fields)
	if err != nil {
		return err
	}
	attributes, err := json.Marshal(p.Attributes)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO product_information_candidate(id,organization_id,crawl_request_id,page_fetch_outcome_id,title,description,brand,manufacturer_model,gtin,attributes,field_provenance,candidate_price_cents,payment_method,availability,normalization_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, candidateID, e.Organization, p.CrawlRequestID, pageID, fieldString(p.Fields, "title"), fieldString(p.Fields, "description"), fieldString(p.Fields, "brand"), fieldString(p.Fields, "manufacturer_model"), fieldString(p.Fields, "gtin"), attributes, fields, o.PriceCents, o.PaymentMethod, o.Availability, p.NormalizationVersion)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO offer(id,organization_id,source_domain_id,canonical_url,product_information_candidate_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT(organization_id,source_domain_id,canonical_url) DO NOTHING`, offerID, e.Organization, sourceID, submittedURL, candidateID)
	if err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, `SELECT id::text FROM offer WHERE organization_id=$1 AND source_domain_id::text=$2 AND canonical_url=$3 FOR UPDATE`, e.Organization, sourceID, submittedURL).Scan(&offerID); err != nil {
		return err
	}
	var verifiedPrice *int64
	if o.PriceStatus == "valid" && o.PaymentMethod != nil && *o.PaymentMethod == "cash" {
		verifiedPrice = o.PriceCents
	}
	_, err = tx.ExecContext(ctx, `UPDATE offer SET current_price_cents=COALESCE($3,current_price_cents),payment_method=CASE WHEN $3::bigint IS NOT NULL THEN 'cash' ELSE payment_method END,last_valid_price_at=CASE WHEN $3::bigint IS NOT NULL THEN $5 ELSE last_valid_price_at END,availability=$4,last_checked_at=$5,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, offerID, verifiedPrice, o.Availability, observed)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO price_availability_observation(id,organization_id,offer_id,price_cents,payment_method,availability,observed_at,idempotency_key,crawl_request_id,outcome,provenance,adapter_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8::text,$8::uuid,'partial',$9,$10)`, observationID, e.Organization, offerID, o.PriceCents, o.PaymentMethod, o.Availability, observed, p.CrawlRequestID, fields, adapterVersion)
	if err != nil {
		return err
	}
	freshness := "unknown"
	if verifiedPrice != nil {
		freshness = "current"
	}
	payload, err := json.Marshal(map[string]any{"crawl_request_id": p.CrawlRequestID, "offer_id": offerID, "candidate_id": candidateID, "outcome": "partial", "price_cents": verifiedPrice, "currency": "BRL", "availability": o.Availability, "source_domain_id": sourceID, "observed_at": observed, "freshness_status": freshness, "provenance_refs": []string{}})
	if err != nil {
		return err
	}
	result := messaging.Envelope{MessageID: eventID, MessageType: "collection.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
	if err = messaging.EnqueueOutbox(ctx, tx, result, "collection.result.v1"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='completed',offer_id=$3,completed_at=now(),updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, p.CrawlRequestID, offerID); err != nil {
		return err
	}
	return tx.Commit()
}

// RunFinderResults consumes verified finder messages and commits before ack.
func RunFinderResults(ctx context.Context, db *sql.DB, brokerURL, publisher string, logger *slog.Logger) {
	if publisher == "" {
		logger.Error("finder results disabled: trusted publisher is not configured")
		return
	}
	for ctx.Err() == nil {
		if err := consumeFinder(ctx, db, brokerURL, publisher); err != nil && ctx.Err() == nil {
			logger.Warn("finder consumer disconnected", "error", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func consumeFinder(ctx context.Context, db *sql.DB, brokerURL, publisher string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "product.information.found.v1", "crawler-finder", 1)
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-consumer.Deliveries():
			if !ok {
				return errors.New("finder result queue closed")
			}
			if delivery.UserId != publisher {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			envelope, err := messaging.DecodeDelivery(delivery)
			if err == nil {
				bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = ApplyFinder(bounded, db, envelope)
				cancel()
			}
			if errors.Is(err, ErrInvalidFinderResult) {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
