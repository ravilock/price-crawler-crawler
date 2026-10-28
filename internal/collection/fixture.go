// Package collection currently provides only a loopback fixture adapter for the
// early integration tests. Production collection and policy enforcement are
// implemented by later tasks; callers must not treat this adapter as either.
package collection

import (
	"context"
	"errors"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// Request describes a local test fixture, not an authorization decision.
type Request struct {
	URL      string
	Approved bool
}
type Result struct {
	Outcome    string
	PriceValid bool
	Title      string
}

var titlePattern = regexp.MustCompile(`(?is)<title\s*>([^<]*)</title\s*>`)
var pricePattern = regexp.MustCompile(`(?is)<meta\s+property=["']product:price:amount["']\s+content=["']([^"']*)["']\s*/?>`)
var priceNumber = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,2})?$`)

// Collect reads only a literal loopback HTTP test fixture, never redirects, and
// bounds both response size and time. It is not registered in the service.
func Collect(ctx context.Context, req Request) (Result, error) {
	if !req.Approved {
		return Result{Outcome: "blocked"}, nil
	}
	u, err := url.Parse(req.URL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Hostname() == "" {
		return Result{Outcome: "blocked"}, nil
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return Result{Outcome: "blocked"}, nil
	}
	client := http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Result{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return Result{}, errors.New("fixture returned non-success status")
	}
	const maxBytes = 64 << 10
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return Result{}, err
	}
	if len(body) > maxBytes {
		return Result{}, errors.New("fixture response too large")
	}
	result := Result{Outcome: "partial"}
	if match := titlePattern.FindSubmatch(body); len(match) == 2 {
		result.Title = html.UnescapeString(string(match[1]))
	}
	if match := pricePattern.FindSubmatch(body); len(match) == 2 && priceNumber.Match(match[1]) {
		if _, err := strconv.ParseFloat(string(match[1]), 64); err == nil {
			result.PriceValid = true
			result.Outcome = "parsed"
		}
	}
	return result, nil
}
