package policy

import (
	"testing"
	"time"
)

func TestPolicyRestrictionsAndExceptions(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	source := Source{Organization: "org", Domain: "store.example", Active: true, AccessReviewed: true, TermsReviewed: true, IntegrationSupported: true, Categories: []string{"parts"}, RequestsPerMinute: 60, CrawlDelay: 2 * time.Second, SlowRequestsPerMinute: 2}
	robots := Snapshot{Readable: true, ExpiresAt: now.Add(time.Hour), Rules: []Rule{{Path: "/private", Allow: false}, {Path: "/private/open", Allow: true}}}
	override := []Override{{Path: "/private/item", Actor: "admin", Reason: "reviewed"}}
	for _, tc := range []struct {
		name, org, category, url string
		snapshot                 Snapshot
		overrides                []Override
		deny, alert              bool
		interval                 time.Duration
	}{
		{"allow", "org", "parts", "https://store.example/private/open", robots, nil, false, false, 2 * time.Second},
		{"blocked", "org", "parts", "https://store.example/private/item", robots, nil, true, false, 0},
		{"exception", "org", "parts", "https://store.example/private/item", robots, override, false, false, 2 * time.Second},
		{"exception exact", "org", "parts", "https://store.example/private/item/other", robots, override, true, false, 0},
		{"foreign tenant", "other", "parts", "https://store.example/private/item", robots, override, true, false, 0},
		{"redirect host", "org", "parts", "https://other.example/private/item", robots, override, true, false, 0},
		{"category", "org", "other", "https://store.example/", robots, nil, true, false, 0},
		{"unreadable", "org", "parts", "https://store.example/", Snapshot{}, nil, false, true, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Check(source, tc.org, tc.category, tc.url, tc.snapshot, tc.overrides, now)
			if (err != nil) != tc.deny || got.AlertAdministrator != tc.alert || got.Interval != tc.interval {
				t.Fatalf("decision=%+v err=%v", got, err)
			}
		})
	}
	source.TermsReviewed = false
	if _, err := Check(source, "org", "parts", "https://store.example/private/item", robots, override, now); err == nil {
		t.Fatal("override bypassed terms")
	}
}
