// Package policy implements source access decisions independently of transport.
package policy

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

var ErrDenied = errors.New("source access denied")

// Source contains reviewed policy loaded from the source owner's storage.
type Source struct {
	Organization, Domain                                        string
	Active, AccessReviewed, TermsReviewed, IntegrationSupported bool
	Categories                                                  []string
	RequestsPerMinute                                           int
	CrawlDelay                                                  time.Duration
	SlowRequestsPerMinute                                       int
}

// Snapshot represents a bounded robots policy. Unreadable snapshots require
// the caller to emit an administrator alert and apply the slow rate.
type Snapshot struct {
	Readable  bool
	ExpiresAt time.Time
	Rules     []Rule
}
type Rule struct {
	Path  string
	Allow bool
}
type Override struct {
	Path, Actor, Reason  string
	ExpiresAt, RevokedAt time.Time
}
type Decision struct {
	Interval           time.Duration
	AlertAdministrator bool
}

// Check never allows a robots exception to bypass source approval or terms.
// Redirects must be checked individually with the same tenant and category.
func Check(source Source, organization, category, rawURL string, snapshot Snapshot, overrides []Override, now time.Time) (Decision, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || !strings.EqualFold(u.Hostname(), source.Domain) || source.Organization == "" || source.Organization != organization || !source.Active || !source.AccessReviewed || !source.TermsReviewed || !source.IntegrationSupported || source.RequestsPerMinute < 1 || source.CrawlDelay < 0 {
		return Decision{}, ErrDenied
	}
	allowed := false
	for _, c := range source.Categories {
		if c == category && c != "" {
			allowed = true
			break
		}
	}
	if !allowed {
		return Decision{}, ErrDenied
	}
	interval := (time.Minute + time.Duration(source.RequestsPerMinute) - 1) / time.Duration(source.RequestsPerMinute)
	if source.CrawlDelay > interval {
		interval = source.CrawlDelay
	}
	unreadable := !snapshot.Readable || !snapshot.ExpiresAt.After(now)
	if unreadable {
		if source.SlowRequestsPerMinute < 1 {
			return Decision{}, ErrDenied
		}
		slow := (time.Minute + time.Duration(source.SlowRequestsPerMinute) - 1) / time.Duration(source.SlowRequestsPerMinute)
		if slow > interval {
			interval = slow
		}
		return Decision{Interval: interval, AlertAdministrator: true}, nil
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}
	matched, permitted := -1, true
	for _, rule := range snapshot.Rules {
		specificity := robotsMatch(rule.Path, path)
		if rule.Path != "" && specificity >= 0 && (specificity > matched || (specificity == matched && rule.Allow)) {
			matched, permitted = specificity, rule.Allow
		}
	}
	if !permitted {
		permitted = false
		for _, o := range overrides {
			if o.Path == u.EscapedPath() && o.Actor != "" && o.Reason != "" && o.RevokedAt.IsZero() && (o.ExpiresAt.IsZero() || o.ExpiresAt.After(now)) {
				permitted = true
				break
			}
		}
	}
	if !permitted {
		return Decision{}, ErrDenied
	}
	return Decision{Interval: interval}, nil
}
