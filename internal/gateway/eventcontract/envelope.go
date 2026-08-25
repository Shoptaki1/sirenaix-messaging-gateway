// Package eventcontract defines the public event body shared by the durable
// outbox, webhooks, and Kafka. The JSON representation is versioned and
// deterministic so event_id remains the consumer deduplication key.
package eventcontract

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const Version = 1

var ErrInvalidEnvelope = errors.New("invalid event envelope")

type Media struct {
	ID              string `json:"media_id"`
	Position        int    `json:"position"`
	Status          string `json:"status"`
	MIMEType        string `json:"mime_type,omitempty"`
	Size            int64  `json:"size,omitempty"`
	DisplayFilename string `json:"display_filename,omitempty"`
	MetadataPath    string `json:"metadata_path,omitempty"`
	ContentPath     string `json:"content_path,omitempty"`
}

type Envelope struct {
	EventID           string    `json:"event_id"`
	Type              string    `json:"type"`
	Version           int       `json:"version"`
	OccurredAt        time.Time `json:"-"`
	OccurredAtText    string    `json:"occurred_at"`
	IngestedAt        time.Time `json:"-"`
	IngestedAtText    string    `json:"ingested_at,omitempty"`
	TenantID          string    `json:"tenant_id"`
	ConnectionID      string    `json:"connection_id,omitempty"`
	ConversationID    string    `json:"conversation_id,omitempty"`
	MessageID         string    `json:"message_id,omitempty"`
	ProviderMessageID string    `json:"provider_message_id,omitempty"`
	ProviderTmpID     string    `json:"provider_tmp_id,omitempty"`
	Direction         string    `json:"direction,omitempty"`
	Provenance        string    `json:"provenance,omitempty"`
	ProviderStatus    string    `json:"provider_status,omitempty"`
	Actionable        bool      `json:"actionable,omitempty"`
	Sender            string    `json:"sender,omitempty"`
	Recipients        []string  `json:"recipients,omitempty"`
	Text              string    `json:"text,omitempty"`
	Transport         string    `json:"transport,omitempty"`
	Status            string    `json:"status,omitempty"`
	// State and MediaID preserve the pre-v1 top-level fields used by existing
	// consumers while Status and Media carry the normalized contract.
	State              string         `json:"state,omitempty"`
	MediaID            string         `json:"media_id,omitempty"`
	MetadataPath       string         `json:"metadata_path,omitempty"`
	ContentPath        string         `json:"content_path,omitempty"`
	Media              []Media        `json:"media,omitempty"`
	ProviderResponseID string         `json:"provider_response_id,omitempty"`
	Reason             string         `json:"reason,omitempty"`
	Data               map[string]any `json:"data,omitempty"`
}

func Marshal(input Envelope) ([]byte, error) {
	if input.Version != 0 && input.Version != Version {
		return nil, ErrInvalidEnvelope
	}
	if !validToken(input.EventID, 256) || !validToken(input.Type, 128) || input.OccurredAt.IsZero() ||
		!validToken(input.TenantID, 256) || !validOptionalToken(input.ConnectionID, 256) ||
		!validOptionalToken(input.ConversationID, domain.MaxProviderConversationIDBytes) ||
		!validOptionalToken(input.MessageID, 256) || !validOptionalToken(input.ProviderMessageID, domain.MaxProviderConversationIDBytes) ||
		!validOptionalToken(input.ProviderTmpID, domain.MaxProviderConversationIDBytes) || !validOptionalToken(input.MediaID, 256) ||
		!validOptionalText(input.MetadataPath, 1024) || !validOptionalText(input.ContentPath, 1024) ||
		!validOptionalToken(input.Reason, 128) ||
		!validOptionalText(input.Text, 4<<20) {
		return nil, ErrInvalidEnvelope
	}
	if input.Direction != "" && input.Direction != "inbound" && input.Direction != "outbound" && input.Direction != "unknown" {
		return nil, ErrInvalidEnvelope
	}
	if !validOptionalToken(input.Provenance, 128) || !validOptionalToken(input.ProviderStatus, 128) {
		return nil, ErrInvalidEnvelope
	}
	if input.Actionable && input.Type != "message.received" {
		return nil, ErrInvalidEnvelope
	}
	if input.Type == "message.received" && (!input.Actionable || input.IngestedAt.IsZero() ||
		input.ConnectionID == "" || input.ConversationID == "" || input.ProviderMessageID == "" ||
		input.Direction != "inbound" || input.Provenance != "provider_live") {
		return nil, ErrInvalidEnvelope
	}
	if input.Sender != "" {
		phone, err := domain.ParseE164(input.Sender)
		if err != nil {
			return nil, ErrInvalidEnvelope
		}
		input.Sender = phone.String()
	}
	recipients := make([]string, 0, len(input.Recipients))
	seenRecipients := make(map[string]struct{}, len(input.Recipients))
	for _, raw := range input.Recipients {
		phone, err := domain.ParseE164(raw)
		if err != nil {
			return nil, ErrInvalidEnvelope
		}
		canonical := phone.String()
		if _, exists := seenRecipients[canonical]; !exists {
			seenRecipients[canonical] = struct{}{}
			recipients = append(recipients, canonical)
		}
	}
	sort.Strings(recipients)
	media := append([]Media(nil), input.Media...)
	for _, item := range media {
		if !validToken(item.ID, 256) || item.Position < 0 || item.Position > 255 ||
			(item.Status != "pending" && item.Status != "ready" && item.Status != "failed") || item.Size < 0 ||
			!validOptionalText(item.MIMEType, 255) || !validOptionalText(item.DisplayFilename, 255) ||
			!validOptionalText(item.MetadataPath, 1024) || !validOptionalText(item.ContentPath, 1024) {
			return nil, ErrInvalidEnvelope
		}
	}
	sort.Slice(media, func(i, j int) bool {
		if media[i].Position == media[j].Position {
			return media[i].ID < media[j].ID
		}
		return media[i].Position < media[j].Position
	})
	input.Version = Version
	input.OccurredAtText = input.OccurredAt.UTC().Format(time.RFC3339Nano)
	if !input.IngestedAt.IsZero() {
		input.IngestedAtText = input.IngestedAt.UTC().Format(time.RFC3339Nano)
	}
	input.Recipients = recipients
	input.Media = media
	return json.Marshal(input)
}

func validOptionalToken(value string, limit int) bool {
	return value == "" || validToken(value, limit)
}

func validToken(value string, limit int) bool {
	return len(value) > 0 && len(value) <= limit && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func validOptionalText(value string, limit int) bool {
	return len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}
