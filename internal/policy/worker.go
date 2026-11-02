package policy

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"price-crawler/services/crawler/internal/messaging"
	"price-crawler/services/crawler/internal/scheduler"
)

var robotsCache = struct {
	sync.Mutex
	values map[string]Snapshot
}{values: make(map[string]Snapshot)}

func cachedRobots(key string, now time.Time) (Snapshot, bool) {
	robotsCache.Lock()
	defer robotsCache.Unlock()
	value, ok := robotsCache.values[key]
	if ok && !value.ExpiresAt.After(now) {
		delete(robotsCache.values, key)
		return Snapshot{}, false
	}
	return value, ok
}
func cacheRobots(key string, snapshot Snapshot) {
	robotsCache.Lock()
	defer robotsCache.Unlock()
	if len(robotsCache.values) >= 1000 {
		robotsCache.values = make(map[string]Snapshot)
	}
	robotsCache.values[key] = snapshot
}

type fetchCommand struct {
	CrawlRequestID string `json:"crawl_request_id"`
	SourceID       string `json:"source_domain_id"`
	URL            string `json:"url"`
	MaxBytes       int    `json:"max_bytes"`
	DeadlineMS     int    `json:"deadline_ms"`
}
type fetchReply struct {
	Status      string  `json:"status"`
	HTTPStatus  int     `json:"http_status,omitempty"`
	ContentType *string `json:"content_type,omitempty"`
	Location    *string `json:"location,omitempty"`
	Body        *string `json:"body_base64,omitempty"`
}

func fetchID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]), nil
}

// ExecuteFetch only serves active, tenant-owned render jobs with persisted
// source/category scope. Unreadable robots creates an administrator notification
// and applies the configured slow request budget.
func ExecuteFetch(ctx context.Context, db *sql.DB, local *scheduler.Budget, e messaging.Envelope) (fetchReply, error) {
	if e.Validate() != nil || e.MessageType != "network.fetch.requested" || e.CausationID == "" {
		return fetchReply{Status: "blocked"}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(e.Payload))
	decoder.DisallowUnknownFields()
	var c fetchCommand
	if decoder.Decode(&c) != nil || decoder.Decode(new(any)) != io.EOF || c.CrawlRequestID == "" || c.SourceID == "" || c.URL == "" || len(c.URL) > 2048 || c.MaxBytes < 1 || c.MaxBytes > 512*1024 || c.DeadlineMS < 1 || c.DeadlineMS > 30000 {
		return fetchReply{Status: "blocked"}, nil
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(c.DeadlineMS)*time.Millisecond)
	defer cancel()
	var category string
	err := db.QueryRowContext(bounded, `SELECT r.category FROM crawl_request r WHERE r.organization_id=$1 AND r.id::text=$2 AND r.source_domain_id::text=$3 AND r.state='rendering' AND r.category IS NOT NULL AND r.correlation_id::text=$4 AND EXISTS (SELECT 1 FROM message_outbox o WHERE o.organization_id=r.organization_id AND o.message_id::text=$5 AND o.correlation_id=r.correlation_id AND o.destination='render.page.requested.v1' AND o.payload->'payload'->>'crawl_request_id'=r.id::text AND o.payload->'payload'->>'source_domain_id'=r.source_domain_id::text)`, e.Organization, c.CrawlRequestID, c.SourceID, e.CorrelationID, e.CausationID).Scan(&category)
	if errors.Is(err, sql.ErrNoRows) {
		return fetchReply{Status: "blocked"}, nil
	}
	if err != nil {
		return fetchReply{}, err
	}
	fetcher := AuthorizedFetcher{DB: db, Local: local, Organization: e.Organization, SourceID: c.SourceID, Category: category}
	result, err := fetcher.Fetch(bounded, c.URL, c.MaxBytes)
	if errors.Is(err, ErrDenied) {
		return fetchReply{Status: "blocked"}, nil
	}
	if errors.Is(err, ErrNotReady) {
		return fetchReply{Status: "not_ready"}, nil
	}
	if err != nil {
		return fetchReply{Status: "failed"}, nil
	}
	contentType, location, body := result.ContentType, result.Location, base64.StdEncoding.EncodeToString(result.Body)
	return fetchReply{Status: "ok", HTTPStatus: result.Status, ContentType: &contentType, Location: &location, Body: &body}, nil
}

var ErrNotReady = errors.New("source policy not ready")

// AuthorizedFetcher applies source, category, robots and shared-budget policy
// to every HTTP hop. The caller must supply a deadline within two minutes.
type AuthorizedFetcher struct {
	DB                               *sql.DB
	Local                            *scheduler.Budget
	Organization, SourceID, Category string
}

func (f AuthorizedFetcher) Fetch(ctx context.Context, rawURL string, maxBytes int) (FetchResult, error) {
	if f.DB == nil || f.Local == nil || f.Organization == "" || f.SourceID == "" || f.Category == "" {
		return FetchResult{}, ErrDenied
	}
	slow := 1
	if raw := os.Getenv("ROBOTS_SLOW_REQUESTS_PER_MINUTE"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 1 || value > 60000 {
			return FetchResult{}, ErrNotReady
		}
		slow = value
	}
	source, overrides, err := LoadSource(ctx, f.DB, f.Organization, f.SourceID, slow)
	if errors.Is(err, ErrDenied) {
		return FetchResult{}, ErrDenied
	}
	if err != nil {
		return FetchResult{}, err
	}
	interval := (time.Minute + time.Duration(source.RequestsPerMinute) - 1) / time.Duration(source.RequestsPerMinute)
	if source.CrawlDelay > interval {
		interval = source.CrawlDelay
	}
	slowInterval := (time.Minute + time.Duration(slow) - 1) / time.Duration(slow)
	if interval < slowInterval {
		interval = slowInterval
	}
	budget := scheduler.PersistentBudget{DB: f.DB, Local: f.Local, Domain: source.Domain, Interval: interval}
	networkPolicy := NetworkPolicy{Domain: source.Domain, MockOrigin: os.Getenv("MOCK_SOURCE_ORIGIN"), LocalOrTest: os.Getenv("MOCK_ENV") == "local" || os.Getenv("MOCK_ENV") == "test"}
	if _, err = networkPolicy.ResolveDestination(ctx, rawURL); err != nil {
		return FetchResult{}, ErrDenied
	}
	target, err := url.Parse(rawURL)
	if err != nil {
		return FetchResult{}, ErrDenied
	}
	origin := target.Scheme + "://" + target.Host
	fetcher := HTTPRobotsFetcher{Origin: origin, MockOrigin: os.Getenv("MOCK_SOURCE_ORIGIN"), LocalOrTest: os.Getenv("MOCK_ENV") == "local" || os.Getenv("MOCK_ENV") == "test", Budget: budget}
	now := time.Now().UTC()
	if _, err = Check(source, f.Organization, f.Category, rawURL, Snapshot{}, nil, now); err != nil {
		return FetchResult{}, ErrDenied
	}
	cacheKey := f.Organization + "/" + f.SourceID + "/" + origin
	snapshot, ok := cachedRobots(cacheKey, now)
	if !ok {
		snapshot, err = RefreshRobots(ctx, fetcher, SQLRecorder{DB: f.DB, WriteAlert: WriteRobotsNotification}, f.Organization, f.SourceID, origin, "price-crawler", now, time.Hour)
		if err != nil {
			return FetchResult{}, ErrNotReady
		}
		if snapshot.Readable {
			cacheRobots(cacheKey, snapshot)
		}
	}
	decision, err := Check(source, f.Organization, f.Category, rawURL, snapshot, overrides, time.Now().UTC())
	if errors.Is(err, ErrDenied) {
		return FetchResult{}, ErrDenied
	}
	if err != nil {
		return FetchResult{}, ErrNotReady
	}
	budget.Interval = decision.Interval
	client := NetworkClient{Policy: networkPolicy, Budget: budget, Authorize: func(context.Context, string) error { return nil }}
	result, err := client.Fetch(ctx, rawURL, maxBytes)
	if errors.Is(err, ErrDenied) {
		return FetchResult{}, ErrDenied
	}
	if err != nil {
		return FetchResult{}, err
	}
	return result, nil

}

// RunFetchConsumer serves renderer fetch RPC only for an authenticated broker
// publisher. Invalid requests are dead-lettered; transient failures requeue.
func RunFetchConsumer(ctx context.Context, db *sql.DB, local *scheduler.Budget, brokerURL, publisherName string, logger *slog.Logger) {
	if publisherName == "" {
		logger.Error("network fetch consumer disabled: trusted publisher is not configured")
		return
	}
	for ctx.Err() == nil {
		if err := consumeFetch(ctx, db, local, brokerURL, publisherName); err != nil && ctx.Err() == nil {
			logger.Warn("network fetch consumer disconnected", "error", err)
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
func consumeFetch(ctx context.Context, db *sql.DB, local *scheduler.Budget, brokerURL, publisherName string) error {
	consumer, err := messaging.NewConsumer(brokerURL, "network.fetch.requested.v1", "crawler-network", 1)
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
				return errors.New("network queue closed")
			}
			e, err := messaging.DecodeDelivery(delivery)
			if err != nil || e.MessageType != "network.fetch.requested" || delivery.UserId != publisherName || delivery.ReplyTo == "" || len(delivery.ReplyTo) > 255 || delivery.CorrelationId != e.CorrelationID {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			reply, err := ExecuteFetch(ctx, db, local, e)
			if err != nil {
				return err
			}
			raw, err := json.Marshal(reply)
			if err != nil {
				return err
			}
			id, err := fetchID()
			if err != nil {
				return err
			}
			result := messaging.Envelope{MessageID: id, MessageType: "network.fetch.result", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: raw}
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = publisher.Publish(bounded, delivery.ReplyTo, result)
			cancel()
			if errors.Is(err, messaging.ErrUnroutable) {
				if err = delivery.Ack(false); err != nil {
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
