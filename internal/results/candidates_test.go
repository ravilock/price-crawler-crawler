package results

import (
	"testing"
)

func TestCandidateFieldRedaction(t *testing.T) {
	f := map[string]any{"status": "valid", "confidence": 0.9, "normalized_value": "model", "raw_value": "secret", "evidence_ref": "ref"}
	cleanField(f, true)
	if _, ok := f["raw_value"]; ok {
		t.Fatal("expired raw value was retained")
	}
	if f["normalized_value"] != "model" || f["evidence_ref"] != "ref" {
		t.Fatalf("identity or provenance lost: %v", f)
	}
	rejected := cleanField(map[string]any{"status": "uncertain", "confidence": 0.5, "normalized_value": "guess"}, false)
	if rejected["normalized_value"] != nil {
		t.Fatal("uncertain field exposed a normalized value")
	}
	if missing := cleanField(nil, false); missing["status"] != "missing" {
		t.Fatalf("missing field: %v", missing)
	}
}
