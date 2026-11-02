package scheduler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"price-crawler/services/crawler/internal/discovery"
)

// SubmitSitemap validates the requested source/category against the tenant's
// reviewed source configuration, then persists one idempotent discovery intent
// per same-origin URL. The caller must fetch the sitemap through the crawler's
// network-admission path before passing its bounded response body here.
func SubmitSitemap(ctx context.Context, db *sql.DB, organization, sourceID, category, idempotencyKey, correlationID string, body io.Reader) ([]Receipt, error) {
	if db == nil || organization == "" || !commandUUID.MatchString(sourceID) || category == "" || len(category) > 100 || category != strings.TrimSpace(category) || len(idempotencyKey) == 0 || len(idempotencyKey) > 160 || body == nil {
		return nil, errors.New("invalid sitemap discovery request")
	}
	var domain string
	var approved bool
	err := db.QueryRowContext(ctx, `SELECT s.domain, (s.is_active AND r.state='approved' AND s.approved_categories ? $3)
		FROM source_domain_config s JOIN source_admin_review r ON r.organization_id=s.organization_id AND r.source_domain_id=s.id
		WHERE s.organization_id=$1 AND s.id::text=$2`, organization, sourceID, category).Scan(&domain, &approved)
	if err != nil {
		return nil, err
	}
	if !approved {
		return nil, errors.New("source category is not approved")
	}
	base, err := url.Parse("https://" + domain)
	if err != nil || base.Hostname() == "" {
		return nil, errors.New("invalid approved source domain")
	}
	urls, err := discovery.SitemapURLs(ctx, body, domain, func(_ context.Context, candidate string) error {
		u, parseErr := url.Parse(candidate)
		if parseErr != nil || !strings.EqualFold(u.Hostname(), base.Hostname()) {
			return errors.New("sitemap URL is outside the approved source")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	receipts := make([]Receipt, 0, len(urls))
	for _, candidate := range urls {
		if err = ctx.Err(); err != nil {
			return receipts, err
		}
		key := idempotencyKey + ":" + digest(candidate)
		id := deterministicUUID(organization + "\x00" + key)
		receipt, submitErr := Submit(ctx, db, Intent{
			ID: id, Organization: organization, URL: candidate, Reason: "discovery",
			IdempotencyKey: key, CorrelationID: correlationID, SourceID: sourceID, Category: category,
		})
		if submitErr != nil {
			return receipts, submitErr
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func deterministicUUID(value string) string {
	sum := sha256.Sum256([]byte(value))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
