package policy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type countingBudget struct{ acquired, released int }

func (b *countingBudget) Acquire(context.Context) (func(), error) {
	b.acquired++
	return func() { b.released++ }, nil
}
func TestRobotsHTTPAdmissionAndRedirects(t *testing.T) {
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			http.Redirect(w, r, "/rules", http.StatusFound)
		case "/rules":
			_, _ = w.Write([]byte("User-agent: *\nDisallow: /private"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	budget := &countingBudget{}
	fetch := HTTPRobotsFetcher{Origin: server.URL, Budget: budget}
	if _, _, err := fetch.FetchRobots(context.Background(), server.URL); err == nil {
		t.Fatal("private address permitted without mock rule")
	}
	fetch.MockOrigin = server.URL
	fetch.LocalOrTest = true
	status, body, err := fetch.FetchRobots(context.Background(), server.URL)
	if err != nil || status != 200 || len(body) == 0 || budget.acquired != 3 || budget.released != 3 {
		t.Fatalf("status=%d body=%q budget=%+v err=%v", status, body, budget, err)
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer redirect.Close()
	fetch.Origin = redirect.URL
	fetch.MockOrigin = redirect.URL
	if _, _, err = fetch.FetchRobots(context.Background(), redirect.URL); err == nil || hits != 0 {
		t.Fatalf("foreign redirect contacted: hits=%d err=%v", hits, err)
	}
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "169.254.1.1", "100.64.0.1", "::1", "2001:db8::1"} {
		if publicDestination(net.ParseIP(raw)) {
			t.Errorf("accepted %s", raw)
		}
	}
	if !publicDestination(net.ParseIP("1.1.1.1")) {
		t.Fatal("public address rejected")
	}
}
