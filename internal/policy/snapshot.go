package policy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"time"
)

// RobotsFetcher must enforce network admission, per-source pacing and redirect
// validation before making any connection. It returns a bounded response.
type RobotsFetcher interface {
	FetchRobots(context.Context, string) (int, []byte, error)
}

// SnapshotRecorder commits metadata and, for unreadable robots, reliably alerts
// an administrator. The caller must not proceed if recording fails.
type SnapshotRecorder interface {
	RecordRobots(context.Context, string, string, RobotsRecord) error
}
type RobotsRecord struct {
	Status               string
	CheckedAt, ExpiresAt time.Time
	ContentHash          string
	AlertAdministrator   bool
}

// RefreshRobots records every retrieval outcome, including malformed responses.
// Unreadable responses return a non-readable snapshot, which Check handles using
// the configured slow rate. A recorder error blocks collection.
func RefreshRobots(ctx context.Context, fetcher RobotsFetcher, recorder SnapshotRecorder, organization, sourceID, origin, agent string, now time.Time, ttl time.Duration) (Snapshot, error) {
	if fetcher == nil || recorder == nil || organization == "" || sourceID == "" || ttl <= 0 || ttl > 24*time.Hour || now.IsZero() {
		return Snapshot{}, errors.New("invalid robots refresh configuration")
	}
	status, body, fetchErr := fetcher.FetchRobots(ctx, origin)
	if ctx.Err() != nil {
		return Snapshot{}, ctx.Err()
	}
	record := RobotsRecord{Status: "unreadable", CheckedAt: now.UTC(), ExpiresAt: now.Add(ttl).UTC(), AlertAdministrator: true}
	snapshot := Snapshot{ExpiresAt: record.ExpiresAt}
	if fetchErr == nil && status == 200 && len(body) <= maxRobotsBytes {
		parsed, err := ParseRobots(bytes.NewReader(body), agent, record.ExpiresAt)
		if err == nil {
			sum := sha256.Sum256(body)
			record.Status = "readable"
			record.ContentHash = hex.EncodeToString(sum[:])
			record.AlertAdministrator = false
			snapshot = parsed
		}
	}
	if err := recorder.RecordRobots(ctx, organization, sourceID, record); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// Evaluate checks approval before contacting robots and rechecks it for the
// destination using the freshly recorded snapshot. Each redirect must call
// Evaluate again for the new destination and its own approved source.
func Evaluate(ctx context.Context, source Source, overrides []Override, fetcher RobotsFetcher, recorder SnapshotRecorder, organization, sourceID, category, destination, agent string, now time.Time, ttl time.Duration) (Decision, error) {
	if _, err := Check(source, organization, category, destination, Snapshot{}, nil, now); err != nil {
		return Decision{}, err
	}
	u, err := url.Parse(destination)
	if err != nil {
		return Decision{}, ErrDenied
	}
	origin := (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
	snapshot, err := RefreshRobots(ctx, fetcher, recorder, organization, sourceID, origin, agent, now, ttl)
	if err != nil {
		return Decision{}, err
	}
	return Check(source, organization, category, destination, snapshot, overrides, now)
}
