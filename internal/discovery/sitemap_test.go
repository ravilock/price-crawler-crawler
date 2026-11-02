package discovery

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSitemapSourceScopeAndDedupe(t *testing.T) {
	xml := `<urlset><url><loc>https://store.example/a?utm_source=x&amp;color=red</loc></url><url><loc>/a?color=red</loc></url><url><loc>https://other.example/b</loc></url><url><loc>https://store.example/blocked</loc></url></urlset>`
	got, err := SitemapURLs(context.Background(), strings.NewReader(xml), "store.example", func(_ context.Context, url string) error {
		if strings.Contains(url, "blocked") {
			return errors.New("blocked")
		}
		return nil
	})
	if err != nil || len(got) != 1 || got[0] != "https://store.example/a?color=red" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if _, err = SitemapURLs(context.Background(), strings.NewReader(`<sitemapindex/>`), "store.example", func(context.Context, string) error { return nil }); err == nil {
		t.Fatal("accepted unsupported sitemap index")
	}
}
