package policy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type IPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// NetworkPolicy is derived from an approved source, never from renderer input.
// MockOrigin must be operator-configured and is accepted only in local/test.
type NetworkPolicy struct {
	Domain, MockOrigin string
	LocalOrTest        bool
	Resolver           IPResolver
}
type Destination struct {
	URL  *url.URL
	IP   net.IP
	Port string
}

// ResolveDestination rejects mixed public/private answers, userinfo, literal
// addresses and unapproved hosts. The caller MUST connect to IP directly with
// Host and TLS server name set to the URL hostname, and recheck every redirect.
func (p NetworkPolicy) ResolveDestination(ctx context.Context, raw string) (Destination, error) {
	if len(raw) == 0 || len(raw) > 2048 || p.Domain == "" {
		return Destination{}, ErrDenied
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" || net.ParseIP(u.Hostname()) != nil || !strings.EqualFold(u.Hostname(), p.Domain) {
		return Destination{}, ErrDenied
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return Destination{}, ErrDenied
	}
	// Public access uses standard ports. The only exception is an exact local
	// mock source origin, which cannot include the fixture control port.
	origin := u.Scheme + "://" + u.Host
	mock := p.LocalOrTest && p.MockOrigin != "" && origin == p.MockOrigin
	if !mock && ((u.Scheme == "http" && port != "80") || (u.Scheme == "https" && port != "443")) {
		return Destination{}, ErrDenied
	}
	resolver := p.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupIPAddr(ctx, u.Hostname())
	if err != nil || len(ips) == 0 {
		return Destination{}, errors.New("destination resolution failed")
	}
	for _, addr := range ips {
		if addr.IP == nil || (!publicDestination(addr.IP) && !mock) {
			return Destination{}, ErrDenied
		}
	}
	return Destination{URL: u, IP: append(net.IP(nil), ips[0].IP...), Port: port}, nil
}

// NetworkBudget supplies the same domain reservation used by other fetch paths.
type NetworkBudget interface {
	Acquire(context.Context) (func(), error)
}
type FetchResult struct {
	Status                            int
	ContentType, Location, RetryAfter string
	Body                              []byte
}
type NetworkClient struct {
	Policy NetworkPolicy
	Budget NetworkBudget
	// Authorize applies reviewed category, robots and source policy for each URL.
	Authorize func(context.Context, string) error
}

// Fetch performs one GET with redirects disabled. Redirects must be submitted
// separately, so each hop is reauthorized and charged to the shared budget.
// It returns no cookies or authorization headers to the renderer.
func (c NetworkClient) Fetch(ctx context.Context, raw string, maxBytes int) (FetchResult, error) {
	if c.Budget == nil || c.Authorize == nil || maxBytes < 1 || maxBytes > 512*1024 {
		return FetchResult{}, ErrDenied
	}
	if err := c.Authorize(ctx, raw); err != nil {
		return FetchResult{}, err
	}
	destination, err := c.Policy.ResolveDestination(ctx, raw)
	if err != nil {
		return FetchResult{}, err
	}
	release, err := c.Budget.Acquire(ctx)
	if err != nil {
		return FetchResult{}, err
	}
	if release == nil {
		return FetchResult{}, errors.New("missing budget release")
	}
	defer release()
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || !strings.EqualFold(host, destination.URL.Hostname()) || port != destination.Port {
			return nil, ErrDenied
		}
		var dialer net.Dialer
		dialer.Timeout = 5 * time.Second
		return dialer.DialContext(ctx, network, net.JoinHostPort(destination.IP.String(), destination.Port))
	}}
	defer transport.CloseIdleConnections()
	client := http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, destination.URL.String(), nil)
	if err != nil {
		return FetchResult{}, err
	}
	request.Header.Set("User-Agent", "price-crawler")
	response, err := client.Do(request)
	if err != nil {
		return FetchResult{}, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(maxBytes)+1))
	if err != nil {
		return FetchResult{}, err
	}
	if len(body) > maxBytes {
		return FetchResult{}, errors.New("source response exceeds limit")
	}
	contentType := response.Header.Get("Content-Type")
	location := response.Header.Get("Location")
	retryAfter := response.Header.Get("Retry-After")
	if len(contentType) > 256 || len(location) > 2048 || len(retryAfter) > 100 {
		return FetchResult{}, errors.New("source header exceeds limit")
	}
	if response.StatusCode == http.StatusTooManyRequests {
		budget, ok := c.Budget.(interface {
			Cooldown(context.Context, time.Time) error
		})
		if !ok {
			return FetchResult{}, errors.New("persistent rate-limit cooldown unavailable")
		}
		now := time.Now().UTC()
		until := now.Add(time.Second)
		if seconds, parseErr := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); parseErr == nil && seconds >= 0 {
			const maxSeconds = int64(100 * 365 * 24 * 60 * 60)
			if seconds > maxSeconds {
				seconds = maxSeconds
			}
			until = now.Add(time.Duration(seconds) * time.Second)
		} else if parsed, parseErr := http.ParseTime(retryAfter); parseErr == nil && parsed.After(until) {
			until = parsed
		}
		if until.Before(now.Add(time.Second)) {
			until = now.Add(time.Second)
		}
		if err = budget.Cooldown(ctx, until); err != nil {
			return FetchResult{}, err
		}
	}
	return FetchResult{Status: response.StatusCode, ContentType: contentType, Location: location, RetryAfter: retryAfter, Body: body}, nil
}
