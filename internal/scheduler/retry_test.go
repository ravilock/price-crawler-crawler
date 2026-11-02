package scheduler

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryDecision(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		outcome  string
		attempts int
		header   string
		retry    bool
		delay    time.Duration
	}{{"timeout", 0, "", true, time.Second}, {"server_error", 2, "", true, 4 * time.Second}, {"rate_limited", 1, "12", true, 12 * time.Second}, {"rate_limited", 0, "7200", true, 2 * time.Hour}, {"rate_limited", 0, now.Add(2 * time.Hour).Format(http.TimeFormat), true, 2 * time.Hour}, {"rate_limited", 0, now.Add(10 * time.Second).Format(http.TimeFormat), true, 10 * time.Second}, {"not_found", 0, "", false, 0}, {"invalid_url", 0, "", false, 0}, {"unsupported_page", 0, "", false, 0}, {"timeout", 3, "", false, 0}} {
		at, ok := RetryDecision(tc.outcome, tc.attempts, tc.header, now, 0)
		if ok != tc.retry || (ok && at.Sub(now) != tc.delay) {
			t.Errorf("%+v: %v %v", tc, at, ok)
		}
	}
}
