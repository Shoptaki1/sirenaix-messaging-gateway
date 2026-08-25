package eventcontract_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/eventcontract"
)

func TestMarshalProducesCanonicalVersionedTenantEnvelope(t *testing.T) {
	input := eventcontract.Envelope{
		EventID: "evt-a", Type: "message.received", OccurredAt: time.Date(2026, 8, 25, 12, 34, 56, 123456000, time.FixedZone("offset", -4*60*60)),
		IngestedAt: time.Date(2026, 8, 25, 16, 35, 0, 0, time.UTC),
		TenantID:   "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
		MessageID: "message-a", ProviderMessageID: "provider-a", Direction: "inbound", Provenance: "provider_live",
		ProviderStatus: "INCOMING_COMPLETE", Actionable: true,
		Sender: "+1 (202) 555-0100", Recipients: []string{"+12025550102", "+12025550101", "+12025550101"}, Text: "hello", Status: "received", State: "received",
		MediaID: "media-a",
		Media: []eventcontract.Media{
			{ID: "media-b", Position: 1, Status: "pending", MIMEType: "image/png", Size: 20, DisplayFilename: "b.png"},
			{ID: "media-a", Position: 0, Status: "ready", MIMEType: "image/jpeg", Size: 10, DisplayFilename: "a.jpg", ContentPath: "/v1/media/media-a/content"},
		},
		Data: map[string]any{"provider_message_id": "provider-a", "conversation_id": "conversation-a"},
	}

	first, err := eventcontract.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eventcontract.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("canonical bytes differ:\n%s\n%s", first, second)
	}
	var decoded struct {
		EventID           string                `json:"event_id"`
		Type              string                `json:"type"`
		Version           int                   `json:"version"`
		OccurredAt        string                `json:"occurred_at"`
		IngestedAt        string                `json:"ingested_at"`
		TenantID          string                `json:"tenant_id"`
		ConnectionID      string                `json:"connection_id"`
		ConversationID    string                `json:"conversation_id"`
		MessageID         string                `json:"message_id"`
		ProviderMessageID string                `json:"provider_message_id"`
		Direction         string                `json:"direction"`
		Provenance        string                `json:"provenance"`
		ProviderStatus    string                `json:"provider_status"`
		Actionable        bool                  `json:"actionable"`
		Sender            string                `json:"sender"`
		Recipients        []string              `json:"recipients"`
		Text              string                `json:"text"`
		Status            string                `json:"status"`
		State             string                `json:"state"`
		MediaID           string                `json:"media_id"`
		Media             []eventcontract.Media `json:"media"`
		Data              map[string]any        `json:"data"`
	}
	if err = json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.EventID != "evt-a" || decoded.Type != "message.received" || decoded.Version != 1 ||
		decoded.OccurredAt != "2026-08-25T16:34:56.123456Z" || decoded.IngestedAt != "2026-08-25T16:35:00Z" || decoded.TenantID != "tenant-a" ||
		decoded.ConnectionID != "connection-a" || decoded.ConversationID != "conversation-a" ||
		decoded.MessageID != "message-a" || decoded.ProviderMessageID != "provider-a" || decoded.Direction != "inbound" ||
		decoded.Provenance != "provider_live" || decoded.ProviderStatus != "INCOMING_COMPLETE" || !decoded.Actionable ||
		decoded.Sender != "+12025550100" || decoded.Text != "hello" || decoded.Status != "received" || decoded.State != "received" || decoded.MediaID != "media-a" {
		t.Fatalf("decoded envelope = %#v", decoded)
	}
	if len(decoded.Recipients) != 2 || decoded.Recipients[0] != "+12025550101" || decoded.Recipients[1] != "+12025550102" {
		t.Fatalf("canonical recipients = %#v", decoded.Recipients)
	}
	if len(decoded.Media) != 2 || decoded.Media[0].ID != "media-a" || decoded.Media[1].ID != "media-b" {
		t.Fatalf("canonical media = %#v", decoded.Media)
	}
	if decoded.Data["provider_message_id"] != "provider-a" || decoded.Data["conversation_id"] != "conversation-a" {
		t.Fatalf("legacy data = %#v", decoded.Data)
	}
}

func TestMarshalRejectsCrossTenantOrUnsafeEventFields(t *testing.T) {
	valid := eventcontract.Envelope{
		EventID: "evt-a", Type: "message.received", OccurredAt: time.Now(), IngestedAt: time.Now(), TenantID: "tenant-a",
		ConnectionID: "connection-a", ConversationID: "conversation-a", MessageID: "message-a", ProviderMessageID: "provider-a",
		Direction: "inbound", Provenance: "provider_live", ProviderStatus: "INCOMING_COMPLETE", Actionable: true,
	}
	for name, mutate := range map[string]func(*eventcontract.Envelope){
		"missing tenant":      func(event *eventcontract.Envelope) { event.TenantID = "" },
		"missing event id":    func(event *eventcontract.Envelope) { event.EventID = "" },
		"missing occurred at": func(event *eventcontract.Envelope) { event.OccurredAt = time.Time{} },
		"invalid direction":   func(event *eventcontract.Envelope) { event.Direction = "sideways" },
		"invalid sender":      func(event *eventcontract.Envelope) { event.Sender = "202-555-0100" },
		"invalid recipient":   func(event *eventcontract.Envelope) { event.Recipients = []string{"+12025550101", "local"} },
		"unsafe text":         func(event *eventcontract.Envelope) { event.Text = "hello\x00secret" },
		"unsafe reason":       func(event *eventcontract.Envelope) { event.Reason = "secret\nvalue" },
		"oversized reason":    func(event *eventcontract.Envelope) { event.Reason = string(bytes.Repeat([]byte{'x'}, 129)) },
		"received not live":   func(event *eventcontract.Envelope) { event.Provenance = "provider_history" },
		"received outgoing":   func(event *eventcontract.Envelope) { event.Direction = "outbound" },
		"received inert":      func(event *eventcontract.Envelope) { event.Actionable = false },
		"received no ingest":  func(event *eventcontract.Envelope) { event.IngestedAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := eventcontract.Marshal(candidate); err == nil {
				t.Fatal("invalid event unexpectedly marshaled")
			}
		})
	}
}
