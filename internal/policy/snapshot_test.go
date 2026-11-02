package policy

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeFetch struct {
	status int
	body   []byte
	err    error
}

func (f fakeFetch) FetchRobots(context.Context, string) (int, []byte, error) {
	return f.status, f.body, f.err
}

type fakeRecord struct {
	record RobotsRecord
	err    error
}

func (f *fakeRecord) RecordRobots(_ context.Context, _, _ string, r RobotsRecord) error {
	f.record = r
	return f.err
}
func TestRefreshRobots(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		f        fakeFetch
		readable bool
	}{{"ok", fakeFetch{status: 200, body: []byte("User-agent: *\nDisallow: /private")}, true}, {"failure", fakeFetch{status: 500}, false}, {"malformed", fakeFetch{status: 200, body: []byte("broken")}, false}, {"transport", fakeFetch{err: errors.New("offline")}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeRecord{}
			snap, err := RefreshRobots(context.Background(), tc.f, rec, "org", "source", "https://example.com", "crawler", now, time.Hour)
			if err != nil || snap.Readable != tc.readable || rec.record.AlertAdministrator == tc.readable || (rec.record.ContentHash != "") != tc.readable {
				t.Fatalf("snapshot=%+v record=%+v err=%v", snap, rec.record, err)
			}
		})
	}
	rec := &fakeRecord{err: errors.New("storage unavailable")}
	if _, err := RefreshRobots(context.Background(), fakeFetch{status: 500}, rec, "org", "source", "https://example.com", "crawler", now, time.Hour); err == nil {
		t.Fatal("recording failure must block collection")
	}
}

func TestEvaluateChecksApprovalBeforeFetch(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	source := Source{Organization: "org", Domain: "example.com", Active: true, AccessReviewed: true, TermsReviewed: true, IntegrationSupported: true, Categories: []string{"parts"}, RequestsPerMinute: 60, SlowRequestsPerMinute: 1}
	recorder := &fakeRecord{}
	fetch := fakeFetch{status: 200, body: []byte("User-agent: *\nDisallow: /private")}
	if _, err := Evaluate(context.Background(), source, nil, fetch, recorder, "other", "source", "parts", "https://example.com/private", "crawler", now, time.Hour); err == nil {
		t.Fatal("foreign tenant accepted")
	}
	if !recorder.record.CheckedAt.IsZero() {
		t.Fatal("unapproved request fetched robots")
	}
	if _, err := Evaluate(context.Background(), source, nil, fetch, recorder, "org", "source", "parts", "https://example.com/private", "crawler", now, time.Hour); err == nil {
		t.Fatal("robots disallow ignored")
	}
	if recorder.record.Status != "readable" {
		t.Fatal("robots outcome not recorded")
	}
}
