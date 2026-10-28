// Package admin validates source commands against an independently verified principal.
package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
)

var ErrForbidden = errors.New("source administration forbidden")
var ErrInvalid = errors.New("invalid source administration command")

type Principal struct {
	Subject      string `json:"subject"`
	Role         string `json:"role"`
	Organization string `json:"organization_id"`
}
type Actor struct {
	Subject string `json:"subject"`
	Role    string `json:"role"`
}
type Review struct {
	AccessRulesReviewed  bool   `json:"access_rules_reviewed"`
	TermsReviewed        bool   `json:"terms_reviewed"`
	SupportedIntegration string `json:"supported_integration"`
	RequestPacePerMinute int    `json:"request_pace_per_minute"`
}
type Command struct {
	Actor               Actor    `json:"actor"`
	Action              string   `json:"action"`
	SourceID            string   `json:"source_id,omitempty"`
	Domain              string   `json:"domain,omitempty"`
	RequestedCategories []string `json:"requested_categories,omitempty"`
	Decision            string   `json:"decision,omitempty"`
	ApprovedCategories  []string `json:"approved_categories,omitempty"`
	Review              *Review  `json:"review,omitempty"`
	Path                string   `json:"path,omitempty"`
	Reason              string   `json:"reason,omitempty"`
	Active              *bool    `json:"active,omitempty"`
}

var hostPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)

func bounded(s string, n int) bool { return len(s) > 0 && len(s) <= n && strings.TrimSpace(s) == s }
func supportedIntegration(name string) bool {
	for _, item := range strings.Split(os.Getenv("SOURCE_ADMIN_SUPPORTED_INTEGRATIONS"), ",") {
		if item != "" && strings.TrimSpace(item) == name {
			return true
		}
	}
	return false
}
func categories(values []string) bool {
	if len(values) < 1 || len(values) > 50 {
		return false
	}
	seen := map[string]bool{}
	for _, v := range values {
		if !bounded(v, 100) || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}

// ParseCommand requires identity verified independently of the message payload.
func ParseCommand(raw []byte, organization string, verified Principal) (Command, error) {
	var c Command
	if len(raw) == 0 || len(raw) > 65536 {
		return c, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil {
		return c, ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return c, ErrInvalid
	}
	if verified.Subject == "" || verified.Organization == "" || verified.Role != "administrator" || organization != verified.Organization || c.Actor.Subject != verified.Subject || c.Actor.Role != verified.Role {
		return c, ErrForbidden
	}
	switch c.Action {
	case "inclusion.request":
		if !bounded(c.Domain, 253) || c.Domain != strings.ToLower(c.Domain) || !hostPattern.MatchString(c.Domain) || strings.Contains(c.Domain, "..") || net.ParseIP(c.Domain) != nil || !categories(c.RequestedCategories) {
			return c, ErrInvalid
		}
	case "inclusion.review":
		if !bounded(c.SourceID, 200) || (c.Decision != "approve" && c.Decision != "reject") || c.Review == nil || !c.Review.AccessRulesReviewed || !c.Review.TermsReviewed || !bounded(c.Review.SupportedIntegration, 100) || (c.Decision == "approve" && !supportedIntegration(c.Review.SupportedIntegration)) || c.Review.RequestPacePerMinute < 1 || c.Review.RequestPacePerMinute > 1000 || (c.Decision == "approve" && !categories(c.ApprovedCategories)) {
			return c, ErrInvalid
		}
	case "robots.path_exception.create":
		if !bounded(c.SourceID, 200) || !bounded(c.Path, 2048) || !strings.HasPrefix(c.Path, "/") || strings.ContainsAny(c.Path, "*?#\r\n") || !bounded(c.Reason, 1000) {
			return c, ErrInvalid
		}
	case "source.read":
		if !bounded(c.SourceID, 200) {
			return c, ErrInvalid
		}
	default:
		return c, ErrInvalid
	}
	return c, nil
}
