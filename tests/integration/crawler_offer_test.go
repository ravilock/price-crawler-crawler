package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"price-crawler/services/crawler/internal/collection"
	"testing"
)

func TestSourcePolicyAndPartialOffers(t *testing.T) {
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		switch r.URL.Path {
		case "/complete":
			w.Write([]byte(`<html><title>Widget</title><meta property="product:price:amount" content="123.45"></html>`))
		case "/malformed":
			w.Write([]byte(`<html><title>Widget</title><meta property="product:price:amount" content="not a price"></html>`))
		case "/partial":
			w.Write([]byte(`<html><title>Widget</title></html>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	for _, tc := range []struct {
		path       string
		approved   bool
		outcome    string
		priceValid bool
	}{
		{"/complete", true, "parsed", true}, {"/malformed", true, "partial", false}, {"/partial", true, "partial", false}, {"/complete", false, "blocked", false},
	} {
		t.Run(tc.path+"/"+tc.outcome, func(t *testing.T) {
			before := hits
			result, err := collection.Collect(context.Background(), collection.Request{URL: server.URL + tc.path, Approved: tc.approved})
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome != tc.outcome || result.PriceValid != tc.priceValid {
				t.Fatalf("result = %+v", result)
			}
			if !tc.approved && hits != before {
				t.Fatal("unapproved source was contacted")
			}
			if tc.approved && result.Title != "Widget" {
				t.Fatalf("partial title lost: %+v", result)
			}
		})
	}
}
