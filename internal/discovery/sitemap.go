package discovery

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/url"
	"strings"
)

const maxSitemapBytes = 1 << 20

// SitemapURLs parses one bounded sitemap after the caller has fetched it under
// source policy. Each discovered URL is normalized, source-scoped, approved by
// the caller and deduplicated; nested sitemap indexes are deliberately rejected.
func SitemapURLs(ctx context.Context, body io.Reader, sourceHost string, authorize func(context.Context, string) error) ([]string, error) {
	if body == nil || authorize == nil || sourceHost == "" {
		return nil, errors.New("invalid discovery source")
	}
	if !strings.Contains(sourceHost, "://") {
		sourceHost = "https://" + sourceHost
	}
	normalizedBase, err := NormalizeURL(sourceHost)
	if err != nil {
		return nil, err
	}
	base, err := url.Parse(normalizedBase)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(body, maxSitemapBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxSitemapBytes {
		return nil, errors.New("sitemap exceeds size limit")
	}
	var doc struct {
		XMLName xml.Name
		URLs    []struct {
			Location string `xml:"loc"`
		} `xml:"url"`
	}
	if err = xml.Unmarshal(data, &doc); err != nil || doc.XMLName.Local != "urlset" || len(doc.URLs) > 5000 {
		return nil, errors.New("invalid or unsupported sitemap")
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(doc.URLs))
	for _, entry := range doc.URLs {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		reference, parseErr := url.Parse(strings.TrimSpace(entry.Location))
		if parseErr != nil {
			continue
		}
		normalized, err := NormalizeURL(base.ResolveReference(reference).String())
		if err != nil {
			continue
		}
		parsed, _ := url.Parse(normalized)
		if parsed.Hostname() != base.Hostname() || seen[normalized] {
			continue
		}
		if err = authorize(ctx, normalized); err != nil {
			continue
		}
		seen[normalized] = true
		result = append(result, normalized)
	}
	return result, nil
}
