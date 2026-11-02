package adapters

import (
	"bytes"
	"net/url"
	"strings"
)

// collectPatolocoPayment records a cash payment label only when the live page's
// primary schema.org price is explicitly presented as à vista with PIX/ boleto.
func collectPatolocoPayment(rawURL string, body []byte, page *Page) {
	u, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(u.Hostname(), "patoloco.com.br") || page == nil {
		return
	}
	for _, field := range page.Fields {
		if field.Name == "payment_method" {
			return
		}
	}
	lower := []byte(strings.ToLower(string(body)))
	price := bytes.Index(lower, []byte(`itemprop="price"`))
	if price < 0 {
		price = bytes.Index(lower, []byte(`itemprop='price'`))
	}
	if price < 0 {
		return
	}
	end := price + 1200
	if end > len(lower) {
		end = len(lower)
	}
	window := string(lower[price:end])
	if !strings.Contains(window, "à vista") && !strings.Contains(window, "a vista") {
		return
	}
	if !strings.Contains(window, "pix") && !strings.Contains(window, "boleto") {
		return
	}
	page.Fields = append(page.Fields, Field{Name: "payment_method", RawValue: "cash", EvidenceRef: "text:patoloco.primary-price-payment-label", SourceKind: "text"})
}
