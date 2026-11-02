// Package adapters extracts bounded evidence from approved HTTP responses.
package adapters

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/html"
	"price-crawler/services/crawler/internal/policy"
)

type Fetcher interface {
	Fetch(context.Context, string, int) (policy.FetchResult, error)
}
type Field struct {
	Name        string `json:"name"`
	RawValue    string `json:"raw_value"`
	EvidenceRef string `json:"evidence_ref"`
	SourceKind  string `json:"source_kind"`
}
type Page struct {
	RequestedURL, FinalURL, Outcome, ContentType, Title, Description, RetryAfter string
	HTTPStatus                                                                   int
	ObservedAt, EvidenceExpiresAt                                                time.Time
	EvidenceHash, EvidenceExcerpt                                                string
	Fields                                                                       []Field
	StructuredData                                                               []map[string]any
}

func truncate(value string, n int) string {
	if len(value) <= n {
		return value
	}
	value = value[:n]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

// Collect performs HTTP-first extraction. The fetcher must apply crawler-owned
// authorization and the shared budget separately to every redirect hop.
func Collect(ctx context.Context, fetcher Fetcher, rawURL string, now time.Time) (Page, error) {
	if fetcher == nil || now.IsZero() {
		return Page{}, errors.New("invalid adapter configuration")
	}
	result := Page{RequestedURL: rawURL, FinalURL: rawURL, ObservedAt: now.UTC(), Fields: []Field{}, StructuredData: []map[string]any{}}
	for hop := 0; hop <= 5; hop++ {
		response, err := fetcher.Fetch(ctx, result.FinalURL, 512*1024)
		if errors.Is(err, policy.ErrDenied) {
			result.Outcome = "blocked"
			return result, nil
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				result.Outcome = "timeout"
				return result, nil
			}
			return result, err
		}
		result.HTTPStatus = response.Status
		result.ContentType = response.ContentType
		result.RetryAfter = response.RetryAfter
		if response.Status >= 300 && response.Status < 400 {
			if response.Location == "" || hop == 5 {
				result.Outcome = "blocked"
				return result, nil
			}
			base, err := url.Parse(result.FinalURL)
			if err != nil {
				result.Outcome = "invalid_url"
				return result, nil
			}
			next, err := base.Parse(response.Location)
			if err != nil {
				result.Outcome = "invalid_url"
				return result, nil
			}
			result.FinalURL = next.String()
			continue
		}
		switch {
		case response.Status == 404:
			result.Outcome = "not_found"
			return result, nil
		case response.Status == 429:
			result.Outcome = "rate_limited"
			return result, nil
		case response.Status >= 500:
			result.Outcome = "server_error"
			return result, nil
		case response.Status < 200 || response.Status >= 300:
			result.Outcome = "blocked"
			return result, nil
		}
		if !strings.Contains(strings.ToLower(response.ContentType), "text/html") {
			result.Outcome = "unsupported_page"
			return result, nil
		}
		hash := sha256.Sum256(response.Body)
		result.EvidenceHash = hex.EncodeToString(hash[:])
		result.EvidenceExcerpt = truncate(string(bytes.ToValidUTF8(response.Body, []byte("�"))), 8192)
		result.EvidenceExpiresAt = result.ObservedAt.Add(7 * 24 * time.Hour)
		if err = extract(response.Body, &result); err != nil {
			return result, err
		}
		collectPatolocoPayment(rawURL, response.Body, &result)
		if result.Title == "" && len(result.Fields) == 0 && len(result.StructuredData) == 0 {
			result.Outcome = "unsupported_page"
		} else {
			result.Outcome = "partial"
		}
		return result, nil
	}
	result.Outcome = "blocked"
	return result, nil
}
func extract(body []byte, page *Page) error {
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	inTitle := false
	inJSON := false
	itemProp, itemPropTag := "", ""
	var itemPropText strings.Builder
	appendItemProp := func(name, value, ref string) {
		if len(page.Fields) < 500 && strings.TrimSpace(value) != "" {
			page.Fields = append(page.Fields, Field{Name: name, RawValue: truncate(strings.TrimSpace(value), 1024), EvidenceRef: ref, SourceKind: "dom"})
		}
	}
	for {
		tt := tokenizer.Next()
		switch tt {
		case html.ErrorToken:
			if errors.Is(tokenizer.Err(), io.EOF) {
				return nil
			}
			return tokenizer.Err()
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			name := strings.ToLower(token.Data)
			var prop, propValue, propRef string
			propTag := name
			for _, a := range token.Attr {
				switch strings.ToLower(a.Key) {
				case "itemprop":
					prop = strings.ToLower(strings.TrimSpace(a.Val))
				case "content":
					propValue = a.Val
				case "href":
					propValue = a.Val
				}
			}
			if mapped, ok := map[string]string{"name": "title", "description": "description", "mpn": "manufacturer_model", "brand": "brand", "gtin": "gtin", "gtin8": "gtin", "gtin12": "gtin", "gtin13": "gtin", "gtin14": "gtin", "price": "price", "pricecurrency": "currency", "availability": "availability"}[prop]; ok {
				propRef = "itemprop:" + prop
				if propValue != "" {
					appendItemProp(mapped, propValue, propRef)
				} else {
					itemProp, itemPropTag = mapped, propTag
					itemPropText.Reset()
				}
			}
			if name == "title" {
				inTitle = true
			}
			if name == "script" {
				for _, a := range token.Attr {
					if strings.EqualFold(a.Key, "type") && strings.EqualFold(a.Val, "application/ld+json") {
						inJSON = true
					}
				}
			}
			if name != "meta" {
				continue
			}
			var key, value string
			for _, a := range token.Attr {
				switch strings.ToLower(a.Key) {
				case "name", "property":
					key = strings.ToLower(a.Val)
				case "content":
					value = a.Val
				}
			}
			if key == "description" {
				page.Description = truncate(value, 5000)
			}
			names := map[string]string{"og:title": "title", "og:description": "description", "product:price:amount": "price", "product:price:currency": "currency", "product:payment_method": "payment_method", "product:availability": "availability"}
			if mapped, ok := names[key]; ok && len(page.Fields) < 500 {
				page.Fields = append(page.Fields, Field{Name: mapped, RawValue: truncate(value, 1024), EvidenceRef: "meta:" + truncate(key, 190), SourceKind: "meta"})
			}
		case html.TextToken:
			token := tokenizer.Token()
			if itemProp != "" {
				itemPropText.WriteString(token.Data)
			}
			if inTitle && page.Title == "" {
				page.Title = truncate(strings.TrimSpace(token.Data), 1000)
			}
			if inJSON && len(token.Data) <= 32768 && len(page.StructuredData) < 100 {
				var object map[string]any
				if json.Unmarshal([]byte(token.Data), &object) == nil && object != nil {
					page.StructuredData = append(page.StructuredData, object)
				}
			}
		case html.EndTagToken:
			token := tokenizer.Token()
			if strings.EqualFold(token.Data, "title") {
				inTitle = false
			}
			if strings.EqualFold(token.Data, "script") {
				inJSON = false
			}
			if itemProp != "" && strings.EqualFold(token.Data, itemPropTag) {
				appendItemProp(itemProp, itemPropText.String(), "itemprop:"+itemPropTag)
				itemProp, itemPropTag = "", ""
				itemPropText.Reset()
			}
		}
	}
}
