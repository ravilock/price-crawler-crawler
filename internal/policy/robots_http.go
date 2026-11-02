package policy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RobotsBudget is shared with page requests by the scheduler. It must reserve
// one request under the source's active pace and concurrency restrictions.
type RobotsBudget interface {
	Acquire(context.Context) (func(), error)
}

// HTTPRobotsFetcher is restricted to one approved origin. MockOrigin must be
// explicitly configured only for a local/test deployment; it is the sole
// exception for private DNS results. A budget is required for every hop.
type HTTPRobotsFetcher struct {
	Origin      string
	MockOrigin  string
	LocalOrTest bool
	Budget      RobotsBudget
	Resolver    *net.Resolver
}

func (f HTTPRobotsFetcher) FetchRobots(ctx context.Context, origin string) (int, []byte, error) {
	approved, err := url.Parse(f.Origin)
	if err != nil || approved.User != nil || approved.Host == "" || (approved.Scheme != "http" && approved.Scheme != "https") || approved.Path != "" || approved.RawQuery != "" || approved.Fragment != "" || origin != f.Origin || f.Budget == nil {
		return 0, nil, ErrDenied
	}
	mock := f.LocalOrTest && f.MockOrigin != "" && f.MockOrigin == f.Origin
	resolver := f.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || !strings.EqualFold(host, approved.Hostname()) {
			return nil, ErrDenied
		}
		expected := approved.Port()
		if expected == "" {
			if approved.Scheme == "https" {
				expected = "443"
			} else {
				expected = "80"
			}
		}
		if port != expected {
			return nil, ErrDenied
		}
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil || len(ips) == 0 {
			return nil, ErrDenied
		}
		for _, ip := range ips {
			if !publicDestination(ip.IP) && !mock {
				return nil, ErrDenied
			}
		}
		var dialer net.Dialer
		dialer.Timeout = 5 * time.Second
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	current := f.Origin + "/robots.txt"
	for hop := 0; hop < 4; hop++ {
		target, err := url.Parse(current)
		if err != nil || target.User != nil || !strings.EqualFold(target.Scheme, approved.Scheme) || !strings.EqualFold(target.Host, approved.Host) {
			return 0, nil, ErrDenied
		}
		release, err := f.Budget.Acquire(ctx)
		if err != nil {
			return 0, nil, err
		}
		if release == nil {
			return 0, nil, errors.New("budget returned no release function")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			release()
			return 0, nil, err
		}
		req.Header.Set("User-Agent", "price-crawler")
		resp, err := client.Do(req)
		if err != nil {
			release()
			return 0, nil, err
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			location, locErr := resp.Location()
			_ = resp.Body.Close()
			release()
			if locErr != nil {
				return 0, nil, ErrDenied
			}
			current = location.String()
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRobotsBytes+1))
		_ = resp.Body.Close()
		release()
		if readErr != nil {
			return 0, nil, readErr
		}
		if len(body) > maxRobotsBytes {
			return 0, nil, errors.New("robots response exceeds limit")
		}
		return resp.StatusCode, body, nil
	}
	return 0, nil, errors.New("too many robots redirects")
}

func publicDestination(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return false
	}
	// Carrier-grade NAT and protocol-assignment ranges are not public targets.
	for _, block := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32"} {
		_, network, _ := net.ParseCIDR(block)
		if network.Contains(ip) {
			return false
		}
	}
	return true
}
