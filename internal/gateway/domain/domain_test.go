package domain_test

import (
	"errors"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestProviderConversationIDValidatorReservesControlPlaneNamespace(t *testing.T) {
	if !domain.ValidProviderConversationID("conversation-a") {
		t.Fatal("ordinary bounded provider conversation ID was rejected")
	}
	for name, value := range map[string]string{
		"empty":           "",
		"reserved parent": domain.ProviderPageCursorID,
		"nul":             "conversation\x00a",
		"invalid UTF-8":   string([]byte{0xff}),
		"excessive":       strings.Repeat("a", domain.MaxProviderConversationIDBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if domain.ValidProviderConversationID(value) {
				t.Fatalf("provider conversation ID %q was accepted", value)
			}
		})
	}
}

func TestProviderResponseIDValidatorMatchesDurableACKAndDatabaseBoundary(t *testing.T) {
	if !domain.ValidProviderResponseID(strings.Repeat("r", 256)) {
		t.Fatal("256-byte provider response ID was rejected")
	}
	for name, value := range map[string]string{
		"empty":                  "",
		"leading space":          " response-a",
		"trailing space":         "response-a ",
		"interior unicode space": "response\u2003a",
		"nul":                    "response\x00a",
		"control":                "response\na",
		"bidi format":            "response\u202ea",
		"zero width format":      "response\u200ba",
		"private use":            "response\ue000a",
		"noncharacter":           "response\ufdd0a",
		"invalid UTF-8":          string([]byte{0xff}),
		"257 bytes":              strings.Repeat("r", 257),
	} {
		t.Run(name, func(t *testing.T) {
			if domain.ValidProviderResponseID(value) {
				t.Fatalf("invalid provider response ID %q was accepted", value)
			}
		})
	}
}

func TestParseE164NormalizesInternationalFormatting(t *testing.T) {
	phone, err := domain.ParseE164(" +1 (202) 555-0123 ")
	if err != nil {
		t.Fatalf("ParseE164() error = %v", err)
	}
	if got, want := phone.String(), "+12025550123"; got != want {
		t.Fatalf("phone = %q, want %q", got, want)
	}
}

func TestParseE164RejectsLocalAndMalformedNumbers(t *testing.T) {
	for _, input := range []string{"", "2025550123", "+01234567", "+1 202 CALL-NOW", "+1234567890123456"} {
		t.Run(input, func(t *testing.T) {
			_, err := domain.ParseE164(input)
			if !errors.Is(err, domain.ErrInvalidPhoneNumber) {
				t.Fatalf("ParseE164(%q) error = %v, want ErrInvalidPhoneNumber", input, err)
			}
		})
	}
}

func TestContactEffectiveDisplayNameUsesAliasThenProviderNameThenPhone(t *testing.T) {
	phone, err := domain.ParseE164("+12025550123")
	if err != nil {
		t.Fatal(err)
	}

	contact := domain.Contact{Phone: phone}
	if got, want := contact.EffectiveDisplayName(), "+12025550123"; got != want {
		t.Fatalf("phone fallback = %q, want %q", got, want)
	}
	contact.ProviderDisplayName = "Phone Name"
	if got, want := contact.EffectiveDisplayName(), "Phone Name"; got != want {
		t.Fatalf("provider fallback = %q, want %q", got, want)
	}
	contact.Alias = "SirenaIX Alias"
	if got, want := contact.EffectiveDisplayName(), "SirenaIX Alias"; got != want {
		t.Fatalf("alias precedence = %q, want %q", got, want)
	}
}

func TestNewLabelNormalizesSlugAndRequiresOwnership(t *testing.T) {
	label, err := domain.NewLabel("label-1", "tenant-1", "  Potential CLIENT  ")
	if err != nil {
		t.Fatalf("NewLabel() error = %v", err)
	}
	if got, want := label.Slug, "potential-client"; got != want {
		t.Fatalf("slug = %q, want %q", got, want)
	}

	_, err = domain.NewLabel("label-1", "", "Potential Client")
	if !errors.Is(err, domain.ErrInvalidTenantID) {
		t.Fatalf("NewLabel() error = %v, want ErrInvalidTenantID", err)
	}
}

func TestConnectionStateValidationCoversGatewayLifecycle(t *testing.T) {
	states := []domain.ConnectionState{
		domain.ConnectionStateUnpaired,
		domain.ConnectionStatePairing,
		domain.ConnectionStateConnected,
		domain.ConnectionStateDegraded,
		domain.ConnectionStateReauthorizationRequired,
		domain.ConnectionStateSuspended,
		domain.ConnectionStateDisconnected,
	}
	for _, state := range states {
		if err := state.Validate(); err != nil {
			t.Errorf("state %q rejected: %v", state, err)
		}
	}
	if err := domain.ConnectionState("surprise").Validate(); !errors.Is(err, domain.ErrInvalidConnectionState) {
		t.Fatalf("unknown state error = %v, want ErrInvalidConnectionState", err)
	}
}

func TestLineValidationAllowsManyConnectionsAndLinesWithinTenant(t *testing.T) {
	connections := []domain.Connection{
		{ID: "connection-1", TenantID: "tenant-1"},
		{ID: "connection-2", TenantID: "tenant-1"},
	}
	for _, connection := range connections {
		for _, lineID := range []domain.LineID{"line-a", "line-b"} {
			line := domain.Line{
				ID:                    lineID,
				TenantID:              "tenant-1",
				ConnectionID:          connection.ID,
				ProviderParticipantID: "participant-7",
				ProviderOutgoingID:    "outgoing-9",
			}
			if err := line.ValidateFor(connection); err != nil {
				t.Fatalf("ValidateFor(%q, %q) error = %v", connection.ID, lineID, err)
			}
		}
	}
}

func TestLineValidationRejectsCrossTenantOwnership(t *testing.T) {
	connection := domain.Connection{ID: "connection-1", TenantID: "tenant-1"}
	line := domain.Line{ID: "line-1", TenantID: "tenant-2", ConnectionID: "connection-1"}
	if err := line.ValidateFor(connection); !errors.Is(err, domain.ErrTenantBoundary) {
		t.Fatalf("ValidateFor() error = %v, want ErrTenantBoundary", err)
	}
}
