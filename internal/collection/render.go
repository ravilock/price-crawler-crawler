package collection

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net/url"
	"strings"
	"time"

	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/scheduler"
)

func requestRender(ctx context.Context, db *sql.DB, j job) error {
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
	id, err := newID()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"crawl_request_id": j.ID, "url": j.URL, "source_domain_id": j.SourceID, "deadline_ms": 30000, "max_network_requests": 50, "adapter_version": "http-v1"})
	if err != nil {
		return err
	}
	event := messaging.Envelope{MessageID: id, MessageType: "render.page.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: j.Organization, CorrelationID: j.Correlation, Payload: payload}
	if err = messaging.EnqueueOutbox(ctx, tx, event, "render.page.requested.v1"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='rendering',lease_token=NULL,lease_until=NULL,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, j.Organization, j.ID); err != nil {
		return err
	}
	return tx.Commit()
}

type renderedField struct {
	Name        string  `json:"name"`
	RawValue    *string `json:"raw_value"`
	EvidenceRef string  `json:"evidence_ref"`
	SourceKind  string  `json:"source_kind,omitempty"`
}
type renderedPage struct {
	CrawlRequestID  string            `json:"crawl_request_id"`
	RequestedURL    string            `json:"requested_url"`
	FinalURL        string            `json:"final_url"`
	Outcome         string            `json:"outcome"`
	ObservedAt      time.Time         `json:"observed_at"`
	ParserVersion   string            `json:"parser_version"`
	HTTPStatus      *int              `json:"http_status,omitempty"`
	ContentType     *string           `json:"content_type,omitempty"`
	PageTitle       *string           `json:"page_title,omitempty"`
	PageDescription *string           `json:"page_description,omitempty"`
	StructuredData  []map[string]any  `json:"structured_data,omitempty"`
	Metadata        []json.RawMessage `json:"metadata,omitempty"`
	Fields          []renderedField   `json:"fields"`
}

var ErrInvalidRender = errors.New("invalid rendered page")

func ApplyRendered(ctx context.Context, db *sql.DB, e messaging.Envelope) error {
	if db == nil || e.Validate() != nil || e.MessageType != "page.parsed" || e.CausationID == "" {
		return ErrInvalidRender
	}
	decoder := json.NewDecoder(bytes.NewReader(e.Payload))
	decoder.DisallowUnknownFields()
	var page renderedPage
	if decoder.Decode(&page) != nil || decoder.Decode(new(any)) != io.EOF || page.CrawlRequestID == "" || page.RequestedURL == "" || page.FinalURL == "" || page.ObservedAt.IsZero() || page.ParserVersion == "" || len(page.ParserVersion) > 100 || page.Fields == nil || len(page.Fields) > 500 || len(page.StructuredData) > 100 || len(page.Metadata) > 200 || (page.Outcome != "complete" && page.Outcome != "partial" && page.Outcome != "blocked" && page.Outcome != "unsupported" && page.Outcome != "failed") {
		return ErrInvalidRender
	}
	for _, f := range page.Fields {
		if len(f.Name) > 200 || len(f.EvidenceRef) > 500 || (f.RawValue != nil && len(*f.RawValue) > 10000) {
			return ErrInvalidRender
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	fresh, err := messaging.BeginInbox(ctx, tx, e.MessageID, "crawler-render-result")
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit()
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM message_outbox WHERE organization_id=$1 AND message_id::text=$2 AND correlation_id::text=$3 AND destination='render.page.requested.v1' AND payload->'payload'->>'crawl_request_id'=$4 AND payload->'payload'->>'url'=$5)`, e.Organization, e.CausationID, e.CorrelationID, page.CrawlRequestID, page.RequestedURL).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrInvalidRender
	}
	var state, source, domain string
	var attempts int
	if err = tx.QueryRowContext(ctx, `SELECT r.state,r.source_domain_id::text,s.domain,r.attempt_count FROM crawl_request r JOIN source_domain_config s ON s.organization_id=r.organization_id AND s.id=r.source_domain_id WHERE r.organization_id=$1 AND r.id::text=$2 FOR UPDATE OF r`, e.Organization, page.CrawlRequestID).Scan(&state, &source, &domain, &attempts); err != nil {
		return err
	}
	target, parseErr := url.Parse(page.FinalURL)
	if parseErr != nil || target.User != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") || !strings.EqualFold(target.Hostname(), domain) || target.Fragment != "" {
		return ErrInvalidRender
	}
	if state == "finding" || state == "completed" {
		return tx.Commit()
	}
	if state != "rendering" {
		return ErrInvalidRender
	}
	if page.Outcome == "failed" && page.HTTPStatus != nil && (*page.HTTPStatus == 429 || *page.HTTPStatus >= 500) {
		reason := "server_error"
		if *page.HTTPStatus == 429 {
			reason = "rate_limited"
		}
		next, retry := scheduler.RetryDecision(reason, attempts, "", time.Now().UTC(), mathrand.Float64())
		if retry {
			_, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='retry_wait',attempt_count=attempt_count+1,next_attempt_at=GREATEST($3,COALESCE((SELECT cooldown_until FROM source_domain_budget WHERE domain=$4),$3)),last_error_code=$5,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, page.CrawlRequestID, next, domain, reason)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='failed',last_error_code=$3,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, page.CrawlRequestID, reason)
		}
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if page.Outcome != "complete" && page.Outcome != "partial" {
		if _, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='failed',last_error_code=$3,updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, page.CrawlRequestID, page.Outcome); err != nil {
			return err
		}
		return tx.Commit()
	}
	pageID, err := newID()
	if err != nil {
		return err
	}
	messageID, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO page_fetch_outcome(id,organization_id,crawl_request_id,source_domain_id,requested_url,final_url,mode,http_status,content_type,observed_at,outcome,renderer_version,parser_version) VALUES($1,$2,$3,$4,$5,$6,'rendered',$7,$8,$9,'partial','playwright-v1',$10)`, pageID, e.Organization, page.CrawlRequestID, source, page.RequestedURL, page.FinalURL, page.HTTPStatus, page.ContentType, page.ObservedAt, page.ParserVersion)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"crawl_request_id": page.CrawlRequestID, "page_parsed_message_id": pageID, "normalization_profile": "1", "page": json.RawMessage(e.Payload)})
	if err != nil {
		return err
	}
	request := messaging.Envelope{MessageID: messageID, MessageType: "product.find.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
	if err = messaging.EnqueueOutbox(ctx, tx, request, "product.find.requested.v1"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE crawl_request SET state='finding',updated_at=now() WHERE organization_id=$1 AND id::text=$2`, e.Organization, page.CrawlRequestID); err != nil {
		return err
	}
	return tx.Commit()
}
func RunRenderResults(ctx context.Context, db *sql.DB, brokerURL, publisher string, logger *slog.Logger) {
	if publisher == "" {
		logger.Error("renderer results disabled: trusted publisher is not configured")
		return
	}
	for ctx.Err() == nil {
		if err := consumeRender(ctx, db, brokerURL, publisher); err != nil && ctx.Err() == nil {
			logger.Warn("renderer consumer disconnected", "error", err)
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
func consumeRender(ctx context.Context, db *sql.DB, brokerURL, publisher string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "page.parsed.v1", "crawler-render", 1)
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
				return errors.New("render result queue closed")
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
				err = ApplyRendered(bounded, db, envelope)
				cancel()
			}
			if errors.Is(err, ErrInvalidRender) {
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
