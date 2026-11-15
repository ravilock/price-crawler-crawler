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

// RankingRequest is the bounded ranking-facts read request. Organization,
// caller identity and correlation come from the authenticated envelope.
type RankingRequest struct {
	OfferID string `json:"offer_id"`
}

// RankingCollection is the latest terminal collection outcome for the offer.
type RankingCollection struct {
	CrawlRequestID string    `json:"crawl_request_id"`
	Outcome        string    `json:"outcome"`
	CompletedAt    time.Time `json:"completed_at"`
}

// RankingObservation is the newest stored observation, including invalid or
// missing prices, so ordering stays independent from price validity.
type RankingObservation struct {
	ObservationID  string    `json:"observation_id"`
	Position       int64     `json:"position"`
	CrawlRequestID string    `json:"crawl_request_id"`
	Availability   string    `json:"availability"`
	ObservedAt     time.Time `json:"observed_at"`
}

// RankingPrice is the newest verified BRL cash observation.
type RankingPrice struct {
	ObservationID  string    `json:"observation_id"`
	Position       int64     `json:"position"`
	CrawlRequestID string    `json:"crawl_request_id"`
	PriceCents     int64     `json:"price_cents"`
	Currency       string    `json:"currency"`
	ObservedAt     time.Time `json:"observed_at"`
}

// RankingFacts is the authorized, consistent snapshot returned for one offer.
type RankingFacts struct {
	OfferID                string
	SourceID               string
	OfferRevision          int64
	SourceRevision         int64
	FreshnessWindowSeconds int
	LatestCollection       *RankingCollection
	LatestObservation      *RankingObservation
	LastVerifiedCashPrice  *RankingPrice
}

// RankingRead reports either ok facts or a bounded non-ok status.
type RankingRead struct {
	Status string
	Facts  *RankingFacts
}

func rankingTimeEqual(a, b time.Time) bool { return a.UTC().Equal(b.UTC()) }

// validateRankingFacts applies the cross-field rules that JSON Schema cannot
// express. An inconsistent snapshot is never served as current facts.
func validateRankingFacts(f RankingFacts) error {
	if !validUUID(f.OfferID) || !validUUID(f.SourceID) || f.OfferRevision < 1 || f.SourceRevision < 1 || f.FreshnessWindowSeconds < 1 {
		return errors.New("invalid ranking revision or identity")
	}
	if f.LatestCollection != nil {
		c := f.LatestCollection
		if !validUUID(c.CrawlRequestID) || c.CompletedAt.IsZero() {
			return errors.New("invalid latest collection")
		}
		switch c.Outcome {
		case "complete", "partial", "failed", "paused":
		default:
			return errors.New("invalid latest collection outcome")
		}
	}
	o := f.LatestObservation
	if o != nil {
		if !validUUID(o.ObservationID) || !validUUID(o.CrawlRequestID) || o.Position < 1 || o.ObservedAt.IsZero() {
			return errors.New("invalid latest observation")
		}
		switch o.Availability {
		case "available", "unavailable", "unknown":
		default:
			return errors.New("invalid latest observation availability")
		}
	}
	p := f.LastVerifiedCashPrice
	if p != nil {
		if !validUUID(p.ObservationID) || !validUUID(p.CrawlRequestID) || p.Position < 1 || p.PriceCents < 0 || p.Currency != "BRL" || p.ObservedAt.IsZero() {
			return errors.New("invalid verified cash price")
		}
		if o == nil || p.Position > o.Position {
			return errors.New("verified price is newer than the latest observation")
		}
		if p.Position == o.Position && (p.ObservationID != o.ObservationID || p.CrawlRequestID != o.CrawlRequestID || !rankingTimeEqual(p.ObservedAt, o.ObservedAt)) {
			return errors.New("verified price identity contradicts the latest observation")
		}
	}
	return nil
}

// ReadRankingFacts reads one offer, its source policy, the latest terminal
// collection and the newest observations in a single statement, so all
// components share one database snapshot.
func ReadRankingFacts(ctx context.Context, db *sql.DB, org, offerID string) RankingRead {
	if db == nil || !validUUID(org) || !validUUID(offerID) {
		return RankingRead{Status: "invalid"}
	}
	var sameOrg bool
	var sourceID sql.NullString
	var offerRevision, sourceRevision sql.NullInt64
	var freshness sql.NullInt64
	var collectionID, collectionOutcome sql.NullString
	var collectionAt sql.NullTime
	var observationID, observationRequest, availability sql.NullString
	var observationPosition sql.NullInt64
	var observationAt sql.NullTime
	var priceID, priceRequest sql.NullString
	var pricePosition sql.NullInt64
	var priceCents sql.NullInt64
	var priceAt sql.NullTime
	err := db.QueryRowContext(ctx, `
		SELECT o.organization_id::text = $1,
		       o.source_domain_id::text,
		       o.offer_revision,
		       s.source_revision,
		       s.freshness_window_seconds,
		       lc.id::text, lc.terminal_outcome, lc.completed_at,
		       lo.id::text, lo.position, lo.crawl_request_id::text, lo.availability, lo.observed_at,
		       lp.id::text, lp.position, lp.crawl_request_id::text, lp.price_cents, lp.observed_at
		FROM offer o
		LEFT JOIN source_domain_config s
		  ON s.organization_id = o.organization_id AND s.id = o.source_domain_id
		LEFT JOIN LATERAL (
		  SELECT r.id, r.terminal_outcome, r.completed_at
		  FROM crawl_request r
		  WHERE r.organization_id = o.organization_id AND r.offer_id = o.id
		    AND r.terminal_outcome IS NOT NULL
		  ORDER BY r.completed_at DESC, r.id DESC
		  LIMIT 1
		) lc ON true
		LEFT JOIN LATERAL (
		  SELECT x.id, x.position, x.crawl_request_id, x.availability, x.observed_at
		  FROM price_availability_observation x
		  WHERE x.organization_id = o.organization_id AND x.offer_id = o.id
		  ORDER BY x.position DESC
		  LIMIT 1
		) lo ON true
		LEFT JOIN LATERAL (
		  SELECT x.id, x.position, x.crawl_request_id, x.price_cents, x.observed_at
		  FROM price_availability_observation x
		  WHERE x.organization_id = o.organization_id AND x.offer_id = o.id
		    AND x.price_status = 'valid' AND x.payment_eligible AND x.price_cents IS NOT NULL
		  ORDER BY x.position DESC
		  LIMIT 1
		) lp ON true
		WHERE o.id = $2`, org, offerID).Scan(
		&sameOrg, &sourceID, &offerRevision, &sourceRevision, &freshness,
		&collectionID, &collectionOutcome, &collectionAt,
		&observationID, &observationPosition, &observationRequest, &availability, &observationAt,
		&priceID, &pricePosition, &priceRequest, &priceCents, &priceAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RankingRead{Status: "not_found"}
	}
	if err != nil {
		return RankingRead{Status: "not_ready"}
	}
	if !sameOrg {
		return RankingRead{Status: "forbidden"}
	}
	if !sourceID.Valid || !offerRevision.Valid || !sourceRevision.Valid || !freshness.Valid {
		return RankingRead{Status: "not_ready"}
	}
	facts := RankingFacts{
		OfferID:                offerID,
		SourceID:               sourceID.String,
		OfferRevision:          offerRevision.Int64,
		SourceRevision:         sourceRevision.Int64,
		FreshnessWindowSeconds: int(freshness.Int64),
	}
	if collectionID.Valid && collectionOutcome.Valid && collectionAt.Valid {
		facts.LatestCollection = &RankingCollection{CrawlRequestID: collectionID.String, Outcome: collectionOutcome.String, CompletedAt: collectionAt.Time.UTC()}
	} else if collectionID.Valid || collectionOutcome.Valid || collectionAt.Valid {
		return RankingRead{Status: "not_ready"}
	}
	if observationID.Valid && observationPosition.Valid && observationRequest.Valid && availability.Valid && observationAt.Valid {
		facts.LatestObservation = &RankingObservation{ObservationID: observationID.String, Position: observationPosition.Int64, CrawlRequestID: observationRequest.String, Availability: availability.String, ObservedAt: observationAt.Time.UTC()}
	} else if observationID.Valid || observationPosition.Valid || observationRequest.Valid || availability.Valid || observationAt.Valid {
		return RankingRead{Status: "not_ready"}
	}
	if priceID.Valid && pricePosition.Valid && priceRequest.Valid && priceCents.Valid && priceAt.Valid {
		facts.LastVerifiedCashPrice = &RankingPrice{ObservationID: priceID.String, Position: pricePosition.Int64, CrawlRequestID: priceRequest.String, PriceCents: priceCents.Int64, Currency: "BRL", ObservedAt: priceAt.Time.UTC()}
	} else if priceID.Valid || pricePosition.Valid || priceRequest.Valid || priceCents.Valid || priceAt.Valid {
		return RankingRead{Status: "not_ready"}
	}
	if err = validateRankingFacts(facts); err != nil {
		return RankingRead{Status: "not_ready"}
	}
	return RankingRead{Status: "ok", Facts: &facts}
}

// Payload renders the exact ok-reply shape, including explicit nulls for facts
// that do not exist.
func (f RankingFacts) Payload() map[string]any {
	var collection any
	if f.LatestCollection != nil {
		collection = map[string]any{"crawl_request_id": f.LatestCollection.CrawlRequestID, "outcome": f.LatestCollection.Outcome, "completed_at": f.LatestCollection.CompletedAt}
	}
	var observation any
	if f.LatestObservation != nil {
		observation = map[string]any{"observation_id": f.LatestObservation.ObservationID, "position": f.LatestObservation.Position, "crawl_request_id": f.LatestObservation.CrawlRequestID, "availability": f.LatestObservation.Availability, "observed_at": f.LatestObservation.ObservedAt}
	}
	var price any
	if f.LastVerifiedCashPrice != nil {
		price = map[string]any{"observation_id": f.LastVerifiedCashPrice.ObservationID, "position": f.LastVerifiedCashPrice.Position, "crawl_request_id": f.LastVerifiedCashPrice.CrawlRequestID, "price_cents": f.LastVerifiedCashPrice.PriceCents, "currency": f.LastVerifiedCashPrice.Currency, "observed_at": f.LastVerifiedCashPrice.ObservedAt}
	}
	return map[string]any{
		"status":                   "ok",
		"offer_id":                 f.OfferID,
		"source_id":                f.SourceID,
		"offer_revision":           f.OfferRevision,
		"source_revision":          f.SourceRevision,
		"freshness_window_seconds": f.FreshnessWindowSeconds,
		"latest_collection":        collection,
		"latest_observation":       observation,
		"last_verified_cash_price": price,
	}
}

// RunRankingFactsConsumer authenticates the trusted broker user and its
// explicitly configured organization scopes.
func RunRankingFactsConsumer(ctx context.Context, db *sql.DB, brokerURL string, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := consumeRankingFacts(ctx, db, brokerURL, logger); err != nil && ctx.Err() == nil {
			logger.Warn("ranking facts reader disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func rankingScopes() map[string][]string {
	var scopes map[string][]string
	if json.Unmarshal([]byte(os.Getenv("RANKING_FACTS_IDENTITIES_JSON")), &scopes) != nil {
		return nil
	}
	return scopes
}

func consumeRankingFacts(ctx context.Context, db *sql.DB, brokerURL string, logger *slog.Logger) error {
	consumer, err := messaging.NewConsumer(brokerURL, "ranking.facts.read.requested.v1", "crawler-ranking-facts", 1)
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
				return errors.New("ranking facts queue closed")
			}
			e, err := messaging.DecodeDelivery(delivery)
			if err != nil || e.MessageType != "ranking.facts.read.requested" || delivery.ReplyTo == "" || len(delivery.ReplyTo) > 255 || delivery.CorrelationId != e.CorrelationID {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			req, valid := decodeRankingRequest(e.Payload)
			if !valid {
				var loose RankingRequest
				if json.Unmarshal(e.Payload, &loose) == nil && validUUID(loose.OfferID) {
					if err = publishRankingReply(ctx, publisher, delivery.ReplyTo, e, map[string]any{"status": "invalid", "offer_id": loose.OfferID}); err != nil {
						return err
					}
					if err = delivery.Ack(false); err != nil {
						return err
					}
					continue
				}
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			scopes := rankingScopes()
			authorized := false
			for _, org := range scopes[delivery.UserId] {
				if delivery.UserId != "" && org == e.Organization {
					authorized = true
					break
				}
			}
			var payload any = map[string]any{"status": "forbidden", "offer_id": req.OfferID}
			if authorized {
				bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
				read := ReadRankingFacts(bounded, db, e.Organization, req.OfferID)
				cancel()
				if read.Status == "ok" {
					payload = read.Facts.Payload()
				} else {
					if read.Status == "not_ready" {
						logger.Warn("ranking facts not ready", "organization_id", e.Organization, "offer_id", req.OfferID)
					}
					payload = map[string]any{"status": read.Status, "offer_id": req.OfferID}
				}
			}
			if err = publishRankingReply(ctx, publisher, delivery.ReplyTo, e, payload); err != nil && !errors.Is(err, messaging.ErrUnroutable) {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}

func decodeRankingRequest(raw []byte) (RankingRequest, bool) {
	var req RankingRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&req); err != nil || d.Decode(new(any)) != io.EOF || !validUUID(req.OfferID) {
		return RankingRequest{}, false
	}
	return req, true
}

func publishRankingReply(ctx context.Context, publisher *messaging.Publisher, replyTo string, request messaging.Envelope, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	reply := messaging.Envelope{MessageID: id, MessageType: "ranking.facts.read.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: body}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return publisher.Publish(bounded, replyTo, reply)
}
