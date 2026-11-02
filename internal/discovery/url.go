// Package discovery owns normalization and bounded source discovery.
package discovery

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
)

var hostname = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)

// NormalizeURL drops only explicitly recognized tracking parameters and the
// fragment, retaining all other query values (including repeated values).
func NormalizeURL(raw string) (string, error) {
	if len(raw) == 0 || len(raw) > 4096 {
		return "", errors.New("invalid URL length")
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", errors.New("invalid URL")
	}
	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil || !hostname.MatchString(host) || strings.Contains(host, "..") {
		return "", errors.New("invalid source hostname")
	}
	port := u.Port()
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	u.Host = host
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	}
	if u.Path == "" {
		u.Path = "/"
	}
	u.Fragment = ""
	u.RawFragment = ""
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", errors.New("invalid URL query")
	}
	for key := range values {
		k := strings.ToLower(key)
		if strings.HasPrefix(k, "utm_") || k == "gclid" || k == "fbclid" {
			delete(values, key)
		}
	}
	u.RawQuery = values.Encode()
	u.ForceQuery = false
	return u.String(), nil
}
