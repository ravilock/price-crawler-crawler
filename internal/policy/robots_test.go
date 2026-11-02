package policy

import (
	"strings"
	"testing"
	"time"
)

func TestRobotsGroupsAndMatching(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	input := `User-agent: *
Disallow: /all
User-agent: crawler
Disallow: /private/*
Allow: /private/public$
User-agent: crawler
Disallow: /other
`
	snap, err := ParseRobots(strings.NewReader(input), "crawler", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	source := Source{Organization: "o", Domain: "example.com", Active: true, AccessReviewed: true, TermsReviewed: true, IntegrationSupported: true, Categories: []string{"c"}, RequestsPerMinute: 1}
	for _, tt := range []struct {
		path   string
		denied bool
	}{{"/all", false}, {"/private/x", true}, {"/private/public", false}, {"/private/public/x", true}, {"/other", true}} {
		_, err := Check(source, "o", "c", "https://example.com"+tt.path, snap, nil, now)
		if (err != nil) != tt.denied {
			t.Errorf("%s: %v", tt.path, err)
		}
	}
	if _, err = ParseRobots(strings.NewReader(strings.Repeat("a", maxRobotsBytes+1)), "crawler", now); err == nil {
		t.Fatal("accepted oversized robots")
	}
	if _, err = ParseRobots(strings.NewReader("User-agent: crawler\nDisallow: bad"), "crawler", now); err == nil {
		t.Fatal("accepted malformed path")
	}
}
