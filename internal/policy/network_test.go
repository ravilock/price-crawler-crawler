package policy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type fakeResolver []net.IPAddr

func (r fakeResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) { return r, nil }
func TestResolveDestination(t *testing.T) {
	public := fakeResolver{{IP: net.ParseIP("1.1.1.1")}}
	private := fakeResolver{{IP: net.ParseIP("10.2.3.4")}}
	mixed := fakeResolver{{IP: net.ParseIP("1.1.1.1")}, {IP: net.ParseIP("10.2.3.4")}}
	for _, tc := range []struct {
		name, url string
		p         NetworkPolicy
		allow     bool
	}{
		{"public", "https://store.example/a", NetworkPolicy{Domain: "store.example", Resolver: public}, true},
		{"foreign", "https://other.example/a", NetworkPolicy{Domain: "store.example", Resolver: public}, false},
		{"private", "https://store.example/a", NetworkPolicy{Domain: "store.example", Resolver: private}, false},
		{"mixed", "https://store.example/a", NetworkPolicy{Domain: "store.example", Resolver: mixed}, false},
		{"mock", "http://mock-store:8080/a", NetworkPolicy{Domain: "mock-store", Resolver: private, LocalOrTest: true, MockOrigin: "http://mock-store:8080"}, true},
		{"control", "http://mock-store:8081/a", NetworkPolicy{Domain: "mock-store", Resolver: private, LocalOrTest: true, MockOrigin: "http://mock-store:8080"}, false},
		{"production mock", "http://mock-store:8080/a", NetworkPolicy{Domain: "mock-store", Resolver: private, MockOrigin: "http://mock-store:8080"}, false},
		{"literal", "http://127.0.0.1/a", NetworkPolicy{Domain: "127.0.0.1", Resolver: private}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest, err := tc.p.ResolveDestination(context.Background(), tc.url)
			if (err == nil) != tc.allow {
				t.Fatalf("dest=%+v err=%v", dest, err)
			}
		})
	}
}

func TestNetworkFetchPinsAddressAndDoesNotFollowRedirect(t *testing.T) {
	hits := 0
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(http.StatusOK) }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, destination.URL, http.StatusFound)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	origin := "http://mock-store:" + parsed.Port()
	budget := &countingBudget{}
	client := NetworkClient{Policy: NetworkPolicy{Domain: "mock-store", MockOrigin: origin, LocalOrTest: true, Resolver: fakeResolver{{IP: net.ParseIP("127.0.0.1")}}}, Budget: budget, Authorize: func(_ context.Context, target string) error {
		if strings.HasSuffix(target, "/denied") {
			return ErrDenied
		}
		return nil
	}}
	result, err := client.Fetch(context.Background(), origin+"/redirect", 1024)
	if err != nil || result.Status != http.StatusFound || hits != 0 || budget.acquired != 1 || budget.released != 1 {
		t.Fatalf("result=%+v hits=%d budget=%+v err=%v", result, hits, budget, err)
	}
	if _, err = client.Fetch(context.Background(), origin+"/denied", 1024); err == nil || budget.acquired != 1 {
		t.Fatal("policy denial not enforced before budget")
	}
	result, err = client.Fetch(context.Background(), origin+"/ok", 1024)
	if err != nil || string(result.Body) != "ok" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err = client.Fetch(context.Background(), origin+"/ok", 1); err == nil {
		t.Fatal("response size limit ignored")
	}
}
