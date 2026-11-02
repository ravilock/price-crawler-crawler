package discovery

import "testing"

func TestNormalizeURL(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
		valid     bool
	}{
		{"https://STORE.example:443/p?color=red&utm_source=a&size=m&color=blue#detail", "https://store.example/p?color=red&color=blue&size=m", true},
		{"http://store.example?fbclid=x&ref=1&gclid=2", "http://store.example/?ref=1", true},
		{"http://127.0.0.1/x", "", false}, {"ftp://store.example/x", "", false}, {"https://name:secret@store.example/x", "", false}, {"https://store.example/x?bad=%zz", "", false},
	} {
		got, err := NormalizeURL(tc.raw)
		if (err == nil) != tc.valid || tc.valid && got != tc.want {
			t.Errorf("%s got %s %v", tc.raw, got, err)
		}
	}
}
