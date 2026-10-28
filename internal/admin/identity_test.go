package admin

import (
	"errors"
	"testing"
)

func TestIdentityRequiresPublisherAndTrustedMembership(t *testing.T) {
	cfg := IdentityConfig{Publisher: "user-api", Administrators: []Principal{{Subject: "alice", Role: "administrator", Organization: "org-a"}}}
	for _, tc := range []struct {
		publisher, org, subject string
		allowed                 bool
	}{{"user-api", "org-a", "alice", true}, {"other", "org-a", "alice", false}, {"user-api", "org-b", "alice", false}, {"user-api", "org-a", "mallory", false}, {"", "org-a", "alice", false}} {
		_, err := cfg.Verify(tc.publisher, tc.org, tc.subject)
		if (err == nil) != tc.allowed || (!tc.allowed && !errors.Is(err, ErrForbidden)) {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}
