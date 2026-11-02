package scheduler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"price-crawler/services/crawler/internal/discovery"
	"price-crawler/services/crawler/internal/messaging"
)

var ErrInvalidCommand = errors.New("invalid crawl command")
var commandUUID = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type command struct {
	CrawlRequestID string `json:"crawl_request_id"`
	OfferID        string `json:"offer_id,omitempty"`
	URL            string `json:"url,omitempty"`
	Reason         string `json:"reason"`
	IdempotencyKey string `json:"idempotency_key"`
	RequesterID    string `json:"requester_id,omitempty"`
	SourceID       string `json:"source_domain_id,omitempty"`
	Category       string `json:"category,omitempty"`
}

// HandleCommand persists an authenticated crawl intent; it never initiates a
// network request. Source/category/robots enforcement is required at execution.
func HandleCommand(ctx context.Context, db *sql.DB, raw []byte, publisher, allowedPublisher string) (Receipt, error) {
	if allowedPublisher == "" || publisher != allowedPublisher {
		return Receipt{}, ErrInvalidCommand
	}
	e, err := messaging.Decode(raw)
	if err != nil || (e.MessageType != "crawl.requested" && e.MessageType != "crawl.scoped.requested") {
		return Receipt{}, ErrInvalidCommand
	}
	d := json.NewDecoder(bytes.NewReader(e.Payload))
	d.DisallowUnknownFields()
	var c command
	if err = d.Decode(&c); err != nil {
		return Receipt{}, ErrInvalidCommand
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Receipt{}, ErrInvalidCommand
	}
	if !commandUUID.MatchString(c.CrawlRequestID) || (c.OfferID != "" && !commandUUID.MatchString(c.OfferID)) || (c.RequesterID != "" && !commandUUID.MatchString(c.RequesterID)) || len(c.IdempotencyKey) < 1 || len(c.IdempotencyKey) > 200 || (c.Reason != "manual" && c.Reason != "scheduled" && c.Reason != "discovery") || (c.URL == "" && c.OfferID == "") {
		return Receipt{}, ErrInvalidCommand
	}
	if e.MessageType == "crawl.requested" && (c.SourceID != "" || c.Category != "") {
		return Receipt{}, ErrInvalidCommand
	}
	if e.MessageType == "crawl.scoped.requested" && (!commandUUID.MatchString(c.SourceID) || c.Category == "" || len(c.Category) > 100 || c.Category != strings.TrimSpace(c.Category)) {
		return Receipt{}, ErrInvalidCommand
	}
	rawURL := c.URL
	if c.OfferID != "" {
		var ownedURL string
		if err = db.QueryRowContext(ctx, `SELECT canonical_url FROM offer WHERE organization_id=$1 AND id::text=$2`, e.Organization, c.OfferID).Scan(&ownedURL); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return Receipt{}, ErrInvalidCommand
			}
			return Receipt{}, err
		}
		if rawURL != "" {
			normalized, err := discovery.NormalizeURL(rawURL)
			if err != nil || normalized != ownedURL {
				return Receipt{}, ErrInvalidCommand
			}
		}
		rawURL = ownedURL
	}
	normalized, err := discovery.NormalizeURL(rawURL)
	if err != nil {
		return Receipt{}, ErrInvalidCommand
	}
	parsed, _ := url.Parse(normalized)
	var approved bool
	if err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_domain_config s JOIN source_admin_review r ON r.organization_id=s.organization_id AND r.source_domain_id=s.id WHERE s.organization_id=$1 AND s.domain=$2 AND s.is_active AND r.state='approved' AND jsonb_array_length(s.approved_categories)>0)`, e.Organization, parsed.Hostname()).Scan(&approved); err != nil {
		return Receipt{}, err
	}
	if !approved {
		return Receipt{}, ErrInvalidCommand
	}
	if c.SourceID != "" {
		if err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_domain_config s JOIN source_admin_review r ON r.organization_id=s.organization_id AND r.source_domain_id=s.id WHERE s.organization_id=$1 AND s.id::text=$2 AND s.domain=$3 AND s.is_active AND r.state='approved' AND s.approved_categories ? $4)`, e.Organization, c.SourceID, parsed.Hostname(), c.Category).Scan(&approved); err != nil {
			return Receipt{}, err
		}
		if !approved {
			return Receipt{}, ErrInvalidCommand
		}
	}
	return Submit(ctx, db, Intent{ID: c.CrawlRequestID, Organization: e.Organization, URL: normalized, Reason: c.Reason, IdempotencyKey: c.IdempotencyKey, CorrelationID: e.CorrelationID, SourceID: c.SourceID, Category: c.Category, RequesterID: c.RequesterID})
}

// RunCommandConsumer reconnects after broker failures; invalid commands are
// dead-lettered and storage errors are retried by closing without ack.
func RunCommandConsumer(ctx context.Context, db *sql.DB, brokerURL, publisher string, logger *slog.Logger) {
	var wg sync.WaitGroup
	for _, queue := range []string{"crawl.requested.v1", "crawl.scoped.requested.v1"} {
		wg.Add(1)
		go func(queue string) { defer wg.Done(); runCommandQueue(ctx, db, brokerURL, publisher, logger, queue) }(queue)
	}
	wg.Wait()
}
func runCommandQueue(ctx context.Context, db *sql.DB, brokerURL, publisher string, logger *slog.Logger, queue string) {
	if publisher == "" {
		logger.Error("crawl command consumer disabled: trusted publisher is not configured")
		return
	}
	for ctx.Err() == nil {
		err := consumeCommands(ctx, db, brokerURL, publisher, queue)
		if ctx.Err() != nil {
			return
		}
		logger.Warn("crawl command consumer disconnected", "error", err)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func consumeCommands(ctx context.Context, db *sql.DB, brokerURL, publisher, queue string) error {
	consumer, err := messaging.NewConsumer(brokerURL, queue, "crawler-scheduler", 1)
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
				return errors.New("crawl queue closed")
			}
			if delivery.UserId != publisher {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err = HandleCommand(bounded, db, delivery.Body, delivery.UserId, publisher)
			cancel()
			if err != nil {
				if !errors.Is(err, ErrInvalidCommand) && !errors.Is(err, ErrConflict) {
					return err
				}
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
