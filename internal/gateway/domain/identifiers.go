package domain

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// ProviderPageCursorID is the internal conversation-row sentinel used for
	// the parent backfill checkpoint. It is never a provider conversation ID.
	ProviderPageCursorID = "_provider_page"
	// MaxProviderConversationIDBytes bounds provider-controlled identifiers
	// before they reach SQL, logs, or provider requests.
	MaxProviderConversationIDBytes = 512
	// MaxProviderResponseIDBytes is shared by the raw durable inbox and the
	// provider ACK protocol. An ID that cannot be ACKed must never be admitted
	// as a durable inbox key.
	MaxProviderResponseIDBytes = 256
)

type TenantID string
type ConnectionID string
type LineID string
type ContactID string
type LabelID string
type MessageID string
type MediaID string
type EventID string
type WebhookEndpointID string

// ValidProviderIdentifier is the single provider-ID trust boundary used by
// inbound projection, outbound commands, provider dispatch, cursor handling,
// and persistence.
func ValidProviderIdentifier(value string) bool {
	if value == "" || value == ProviderPageCursorID || len(value) > MaxProviderConversationIDBytes ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unsafeProviderIdentifierRune(character) {
			return false
		}
	}
	return true
}

func ValidProviderConversationID(value string) bool {
	return ValidProviderIdentifier(value)
}

// ValidProviderResponseID is the canonical trust boundary for provider frame,
// inbox, pagination, and ACK response identifiers.
func ValidProviderResponseID(value string) bool {
	if value == "" || len(value) > MaxProviderResponseIDBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unsafeProviderIdentifierRune(character) {
			return false
		}
	}
	return true
}

func unsafeProviderIdentifierRune(character rune) bool {
	// Opaque provider identifiers are protocol keys, never display strings.
	// Exclude whitespace and invisible/directional formatting, private-use
	// characters, and permanently reserved Unicode noncharacters so their SQL,
	// ACK, audit, and log representations cannot become ambiguous.
	return unicode.IsControl(character) || unicode.IsSpace(character) ||
		unicode.Is(unicode.Cf, character) || unicode.Is(unicode.Co, character) ||
		(character >= 0xFDD0 && character <= 0xFDEF) ||
		(character >= 0 && character <= utf8.MaxRune && character&0xFFFE == 0xFFFE)
}
