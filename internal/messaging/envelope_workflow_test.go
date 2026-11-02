package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSourceAdministrationEnvelopeUsesUnversionedMessageType(t *testing.T) {
	e := Envelope{MessageID: "00000000-0000-4000-8000-000000000001", MessageType: "source.admin.command", SchemaVersion: 1, OccurredAt: time.Now(), Organization: "00000000-0000-4000-8000-000000000001", CorrelationID: "00000000-0000-4000-8000-000000000001", Payload: json.RawMessage(`{"action":"source.read"}`)}
	if err := e.Validate(); err != nil {
		t.Fatalf("documented workflow envelope rejected: %v", err)
	}
}

func TestNetworkFetchMessageTypes(t *testing.T) {
	for _, kind := range []string{"network.fetch.requested", "network.fetch.result"} {
		envelope := Envelope{MessageID: "00000000-0000-4000-8000-000000000001", MessageType: kind, SchemaVersion: 1, OccurredAt: time.Now(), Organization: "00000000-0000-4000-8000-000000000001", CorrelationID: "00000000-0000-4000-8000-000000000001", Payload: json.RawMessage(`{}`)}
		if err := envelope.Validate(); err != nil {
			t.Errorf("%s: %v", kind, err)
		}
	}
}
