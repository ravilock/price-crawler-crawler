package adapters

import (
	"context"
	"strings"
	"testing"
	"time"

	"price-crawler/services/crawler/internal/policy"
)

type fakeFetcher struct {
	responses map[string]policy.FetchResult
	denied    map[string]bool
	calls     []string
}

func (f *fakeFetcher) Fetch(_ context.Context, url string, _ int) (policy.FetchResult, error) {
	f.calls = append(f.calls, url)
	if f.denied[url] {
		return policy.FetchResult{}, policy.ErrDenied
	}
	return f.responses[url], nil
}
func TestCollectEvidenceAndRedirectValidation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	html := `<html><title>Widget</title><meta property="product:price:amount" content="100.00"><meta property="product:price:currency" content="BRL"><meta property="product:payment_method" content="cash"><script type="application/ld+json">{"name":"Widget"}</script></html>` + strings.Repeat("x", 9000)
	fetch := &fakeFetcher{responses: map[string]policy.FetchResult{"https://store.example/a": {Status: 302, Location: "/b"}, "https://store.example/b": {Status: 200, ContentType: "text/html", Body: []byte(html)}}}
	page, err := Collect(context.Background(), fetch, "https://store.example/a", now)
	if err != nil || page.FinalURL != "https://store.example/b" || page.Title != "Widget" || len(page.Fields) != 3 || len(page.StructuredData) != 1 || len(page.EvidenceExcerpt) > 8192 || page.EvidenceHash == "" || page.EvidenceExpiresAt.Sub(now) > 30*24*time.Hour {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	fetch.denied = map[string]bool{"https://store.example/b": true}
	page, err = Collect(context.Background(), fetch, "https://store.example/a", now)
	if err != nil || page.Outcome != "blocked" {
		t.Fatalf("redirect denial: %+v %v", page, err)
	}
}
func TestTerminalAndRetryableOutcomes(t *testing.T) {
	for status, want := range map[int]string{404: "not_found", 429: "rate_limited", 500: "server_error", 403: "blocked"} {
		f := &fakeFetcher{responses: map[string]policy.FetchResult{"https://store.example/": {Status: status, RetryAfter: "5"}}}
		page, err := Collect(context.Background(), f, "https://store.example/", time.Now())
		if err != nil || page.Outcome != want {
			t.Errorf("%d: %+v %v", status, page, err)
		}
	}
}
