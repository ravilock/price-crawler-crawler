package results

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"price-crawler/services/crawler/internal/messaging"
)

var ErrInvalidRead = errors.New("invalid collection results read")

type Actor struct {
	Subject string `json:"subject"`
	Role    string `json:"role"`
}
type Item struct {
	RequestID    string     `json:"request_id"`
	State        string     `json:"state"`
	SourceID     string     `json:"source_id"`
	SourceDomain string     `json:"source_domain"`
	PriceStatus  string     `json:"price_status"`
	PriceCents   *int64     `json:"price_cents"`
	Availability string     `json:"availability"`
	ObservedAt   *time.Time `json:"observed_at"`
}
type Page struct {
	Status     string  `json:"status"`
	Items      []Item  `json:"items"`
	NextCursor *string `json:"next_cursor"`
	Error      *struct {
		Code string `json:"code"`
	} `json:"error,omitempty"`
}
type request struct {
	Actor     Actor   `json:"actor"`
	RequestID string  `json:"request_id,omitempty"`
	Cursor    *string `json:"cursor,omitempty"`
	Limit     int     `json:"limit"`
}
type cursor struct {
	Organization string    `json:"organization_id"`
	Subject      string    `json:"subject"`
	Role         string    `json:"role"`
	RequestID    string    `json:"request_id,omitempty"`
	WatermarkAt  time.Time `json:"watermark_at"`
	WatermarkID  string    `json:"watermark_id"`
	AfterAt      time.Time `json:"after_at"`
	AfterID      string    `json:"after_id"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func failure(status, code string) Page {
	return Page{Status: status, Error: &struct {
		Code string `json:"code"`
	}{Code: code}, Items: []Item{}}
}

func validActor(actor Actor) bool {
	if strings.TrimSpace(actor.Subject) == "" || len(actor.Subject) > 200 {
		return false
	}
	switch actor.Role {
	case "administrator", "manager", "auditor", "end_user":
		return true
	default:
		return false
	}
}

// Read returns one stable tenant-bound page. A continuation cursor captures a
// high-watermark and the prior page key, so later inserts cannot shift pages.
func Read(ctx context.Context, db *sql.DB, organization string, actor Actor, requestID string, token *string, limit int, now time.Time) Page {
	if db == nil || organization == "" || !validActor(actor) || limit < 1 || limit > 100 {
		return failure("invalid", "invalid_request")
	}
	if requestID != "" && !uuidPattern.MatchString(requestID) {
		return failure("invalid", "invalid_request")
	}
	var pageCursor cursor
	if token != nil && *token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(*token)
		if err != nil || len(raw) > 2048 {
			return failure("invalid", "invalid_cursor")
		}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&pageCursor) != nil || pageCursor.Organization != organization || pageCursor.Subject != actor.Subject || pageCursor.Role != actor.Role || pageCursor.RequestID != requestID {
			return failure("forbidden", "cursor_scope")
		}
		if !pageCursor.ExpiresAt.After(now) {
			return failure("expired", "cursor_expired")
		}
	} else {
		pageCursor.Organization, pageCursor.Subject, pageCursor.Role, pageCursor.RequestID = organization, actor.Subject, actor.Role, requestID
		pageCursor.ExpiresAt = now.Add(30 * 24 * time.Hour)
		query := `SELECT r.created_at,r.id::text FROM crawl_request r WHERE r.organization_id=$1 AND ($2='' OR r.id::text=$2) AND ($3<>'end_user' OR EXISTS (SELECT 1 FROM crawl_intent_key k WHERE k.organization_id=r.organization_id AND k.crawl_request_id=r.id AND k.requester_id=$4)) ORDER BY r.created_at DESC,r.id DESC LIMIT 1`
		err := db.QueryRowContext(ctx, query, organization, requestID, actor.Role, actor.Subject).Scan(&pageCursor.WatermarkAt, &pageCursor.WatermarkID)
		if errors.Is(err, sql.ErrNoRows) {
			if requestID != "" {
				return failure("not_found", "request_not_found")
			}
			return Page{Status: "ok", Items: []Item{}, NextCursor: nil}
		}
		if err != nil {
			return failure("not_ready", "read_unavailable")
		}
	}
	query := `SELECT r.id::text,r.state,r.source_domain_id::text,s.domain,
		obs.price_cents,COALESCE(obs.payment_method,''),COALESCE(obs.availability,o.availability,'unknown'),obs.observed_at
		FROM crawl_request r
		JOIN source_domain_config s ON s.organization_id=r.organization_id AND s.id=r.source_domain_id
		LEFT JOIN offer o ON o.organization_id=r.organization_id AND o.id=r.offer_id
		LEFT JOIN LATERAL (SELECT x.price_cents,x.payment_method,x.availability,x.observed_at FROM price_availability_observation x WHERE x.organization_id=r.organization_id AND x.offer_id=r.offer_id ORDER BY x.observed_at DESC,x.id DESC LIMIT 1) obs ON true
		WHERE r.organization_id=$1 AND ($2='' OR r.id::text=$2)
		AND ($3<>'end_user' OR EXISTS (SELECT 1 FROM crawl_intent_key k WHERE k.organization_id=r.organization_id AND k.crawl_request_id=r.id AND k.requester_id=$4))
		AND (r.created_at,r.id) <= ($5,$6::uuid) AND (r.created_at,r.id) > ($7,$8::uuid)
		ORDER BY r.created_at,r.id LIMIT $9`
	if pageCursor.AfterID == "" {
		pageCursor.AfterID = zeroUUID
	}
	rows, err := db.QueryContext(ctx, query, organization, requestID, actor.Role, actor.Subject, pageCursor.WatermarkAt, pageCursor.WatermarkID, pageCursor.AfterAt, pageCursor.AfterID, limit+1)
	if err != nil {
		return failure("not_ready", "read_unavailable")
	}
	defer func() { _ = rows.Close() }()
	items := make([]Item, 0, limit)
	for rows.Next() {
		var item Item
		var price sql.NullInt64
		var payment, availability string
		var observed sql.NullTime
		if err = rows.Scan(&item.RequestID, &item.State, &item.SourceID, &item.SourceDomain, &price, &payment, &availability, &observed); err != nil {
			return failure("not_ready", "read_unavailable")
		}
		item.PriceStatus = "missing"
		if price.Valid && payment == "cash" {
			value := price.Int64
			item.PriceCents = &value
			item.PriceStatus = "verified"
		} else {
			item.PriceCents = nil
		}
		item.Availability = availability
		if observed.Valid {
			value := observed.Time.UTC()
			item.ObservedAt = &value
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return failure("not_ready", "read_unavailable")
	}
	page := Page{Status: "ok", Items: items, NextCursor: nil}
	if len(items) > limit {
		items = items[:limit]
		page.Items = items
		last := items[len(items)-1]
		var lastAt time.Time
		if err = db.QueryRowContext(ctx, `SELECT created_at FROM crawl_request WHERE organization_id=$1 AND id::text=$2`, organization, last.RequestID).Scan(&lastAt); err != nil {
			return failure("not_ready", "read_unavailable")
		}
		pageCursor.AfterAt, pageCursor.AfterID = lastAt, last.RequestID
		raw, _ := json.Marshal(pageCursor)
		next := base64.RawURLEncoding.EncodeToString(raw)
		page.NextCursor = &next
	}
	return page
}

var uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const zeroUUID = "00000000-0000-0000-0000-000000000000"

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:]), nil
}

func Handle(ctx context.Context, db *sql.DB, organization string, raw []byte) (Page, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var command request
	if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF {
		return failure("invalid", "invalid_request"), nil
	}
	return Read(ctx, db, organization, command.Actor, command.RequestID, command.Cursor, command.Limit, time.Now().UTC()), nil
}

func replyEnvelope(request messaging.Envelope, page Page) (messaging.Envelope, error) {
	id, err := newID()
	if err != nil {
		return messaging.Envelope{}, err
	}
	payload, err := json.Marshal(page)
	if err != nil {
		return messaging.Envelope{}, err
	}
	return messaging.Envelope{MessageID: id, MessageType: "collection.results.read.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: payload}, nil
}

// RunReadConsumer serves tenant-scoped result polling requests over RabbitMQ.
func RunReadConsumer(ctx context.Context, db *sql.DB, brokerURL, publisherName string, logger *slog.Logger) {
	if publisherName == "" {
		logger.Error("result reader disabled: trusted publisher is not configured")
		return
	}
	for ctx.Err() == nil {
		if err := consumeReads(ctx, db, brokerURL, publisherName); err != nil && ctx.Err() == nil {
			logger.Warn("result reader disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeReads(ctx context.Context, db *sql.DB, brokerURL, trustedPublisher string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "collection.results.read.requested.v1", "crawler-result-reader", 1)
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
				return errors.New("result read queue closed")
			}
			if delivery.UserId != trustedPublisher || delivery.ReplyTo == "" {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			e, decodeErr := messaging.DecodeDelivery(delivery)
			if decodeErr != nil || e.MessageType != "collection.results.read.requested" {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			page, handleErr := Handle(bounded, db, e.Organization, e.Payload)
			cancel()
			if handleErr != nil {
				return handleErr
			}
			reply, makeErr := replyEnvelope(e, page)
			if makeErr != nil {
				return makeErr
			}
			bounded, cancel = context.WithTimeout(ctx, 5*time.Second)
			err = publisher.Publish(bounded, delivery.ReplyTo, reply)
			cancel()
			if err != nil {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
