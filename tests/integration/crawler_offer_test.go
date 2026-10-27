package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type offerFixture struct {
	Name   string `json:"name"`
	Source struct {
		RequestedURL string   `json:"requested_url"`
		Approved     []string `json:"approved_hosts"`
		Active       bool     `json:"active"`
		Robots       string   `json:"robots"`
		RedirectURL  string   `json:"redirect_url"`
	} `json:"source"`
	Page struct {
		Title        string `json:"title"`
		Price        string `json:"price"`
		Availability string `json:"availability"`
	} `json:"page"`
	Expected struct {
		Policy       string `json:"policy"`
		Title        field  `json:"title"`
		Price        field  `json:"price"`
		Availability field  `json:"availability"`
	} `json:"expected"`
}

type field struct {
	Value    any    `json:"value"`
	Status   string `json:"status"`
	Currency string `json:"currency,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// These fixtures are the contract for crawler policy and offer normalization.
// T025/T028 implement the behavior that consumes them; keeping them as input data
// makes source approval, redirect, malformed-price, and partial-field cases
// reusable by the end-to-end crawler tests.
func TestOfferBehaviorFixtures(t *testing.T) {
	want := map[string]struct {
		policy       string
		priceStatus  string
		priceValue   any
		availability string
	}{
		"approved_partial.json":               {policy: "allow", priceStatus: "valid", priceValue: float64(129990), availability: "available"},
		"unapproved_redirect.json":            {policy: "block_redirect", priceStatus: "valid", priceValue: float64(129990), availability: "available"},
		"unapproved_source.json":              {policy: "block_source", priceStatus: "valid", priceValue: float64(129990), availability: "available"},
		"malformed_price_partial_fields.json": {policy: "allow", priceStatus: "invalid", priceValue: nil, availability: "unavailable"},
	}

	for name, expected := range want {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "fixtures", "offers", name))
			if err != nil {
				t.Fatal(err)
			}
			var fixture offerFixture
			if err := json.Unmarshal(data, &fixture); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if fixture.Name == "" || fixture.Source.RequestedURL == "" || fixture.Source.RedirectURL == "" {
				t.Fatal("fixture must identify its source and requested/final URLs")
			}
			if fixture.Expected.Policy != expected.policy {
				t.Errorf("policy = %q, want %q", fixture.Expected.Policy, expected.policy)
			}
			if fixture.Expected.Price.Status != expected.priceStatus || fixture.Expected.Price.Value != expected.priceValue {
				t.Errorf("price = (%v, %q), want (%v, %q)", fixture.Expected.Price.Value, fixture.Expected.Price.Status, expected.priceValue, expected.priceStatus)
			}
			if fixture.Expected.Availability.Value != expected.availability || fixture.Expected.Availability.Status != "valid" {
				t.Errorf("availability = (%v, %q), want (%q, valid)", fixture.Expected.Availability.Value, fixture.Expected.Availability.Status, expected.availability)
			}
			if fixture.Expected.Title.Status != "valid" || fixture.Expected.Title.Value == nil {
				t.Error("valid title must survive offer normalization, including malformed-price responses")
			}
		})
	}
}
