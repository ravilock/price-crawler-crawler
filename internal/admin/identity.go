package admin

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

// IdentityConfig is trusted operator-managed tenant configuration. Broker ACLs
// must restrict the publisher credential to the user API; RabbitMQ validates a
// supplied AMQP UserId against the authenticated publishing connection.
type IdentityConfig struct {
	Publisher      string      `json:"publisher"`
	Administrators []Principal `json:"administrators"`
}

func LoadIdentity(path string) (IdentityConfig, error) {
	var cfg IdentityConfig
	var b []byte
	var err error
	if path == "" {
		raw := os.Getenv("SOURCE_ADMIN_IDENTITY_JSON")
		if raw == "" {
			return cfg, nil
		}
		b = []byte(raw)
	} else {
		b, err = os.ReadFile(path)
		if err != nil {
			return cfg, err
		}
	}
	if len(b) > 65536 {
		return cfg, errors.New("identity configuration too large")
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(&cfg); err != nil {
		return cfg, err
	}
	if d.Decode(new(any)) != io.EOF {
		return cfg, errors.New("identity configuration has trailing data")
	}
	if !bounded(cfg.Publisher, 200) || len(cfg.Administrators) == 0 || len(cfg.Administrators) > 1000 {
		return cfg, errors.New("invalid source administrator identity configuration")
	}
	seen := map[string]bool{}
	for _, a := range cfg.Administrators {
		key := a.Organization + "/" + a.Subject
		if !bounded(a.Organization, 200) || !bounded(a.Subject, 200) || a.Role != "administrator" || seen[key] {
			return cfg, errors.New("invalid or duplicate administrator identity")
		}
		seen[key] = true
	}
	return cfg, nil
}

// Verify checks broker-authenticated publisher and trusted tenant membership.
func (c IdentityConfig) Verify(publisher, org, subject string) (Principal, error) {
	if c.Publisher == "" || publisher != c.Publisher {
		return Principal{}, ErrForbidden
	}
	for _, p := range c.Administrators {
		if p.Organization == org && p.Subject == subject && p.Role == "administrator" {
			return p, nil
		}
	}
	return Principal{}, ErrForbidden
}
