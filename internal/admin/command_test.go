package admin

import (
	"errors"
	"testing"
)

func TestParseAuthorizedCommand(t *testing.T) {
	trusted := Principal{Subject: "admin-1", Role: "administrator", Organization: "tenant-1"}
	tests := []struct {
		name, org, payload string
		principal          Principal
		want               error
	}{
		{"inclusion", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"inclusion.request","domain":"example.com","requested_categories":["components"]}`, trusted, nil},
		{"untrusted actor", "tenant-1", `{"actor":{"subject":"admin-2","role":"administrator"},"action":"inclusion.request","domain":"example.com","requested_categories":["components"]}`, trusted, ErrForbidden},
		{"cross tenant", "tenant-2", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"inclusion.request","domain":"example.com","requested_categories":["components"]}`, trusted, ErrForbidden},
		{"unverified role", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"source.read","source_id":"x"}`, Principal{Subject: "admin-1", Role: "end_user", Organization: "tenant-1"}, ErrForbidden},
		{"ip literal", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"inclusion.request","domain":"127.0.0.1","requested_categories":["components"]}`, trusted, ErrInvalid},
		{"unknown field", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"source.read","source_id":"x","other":true}`, trusted, ErrInvalid},
		{"missing review", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"inclusion.review","source_id":"x","decision":"approve"}`, trusted, ErrInvalid},
		{"path wildcard", "tenant-1", `{"actor":{"subject":"admin-1","role":"administrator"},"action":"robots.path_exception.create","source_id":"x","path":"/private/*","reason":"review"}`, trusted, ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseCommand([]byte(tc.payload), tc.org, tc.principal)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}
