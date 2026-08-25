// Package gmessages adapts the Matrix-independent libgm protocol types to the
// provider-neutral gateway contracts.
package gmessages

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

var (
	ErrInvalidClient            = errors.New("invalid Google Messages client")
	ErrInvalidConnection        = errors.New("invalid Google Messages connection")
	ErrLineSelectionUnsupported = errors.New("line selection unsupported")
	ErrLineMismatch             = errors.New("requested line does not match provider route")
	ErrScopeMismatch            = errors.New("requested line is outside adapter scope")
)

// ContactClient is the minimum libgm surface needed for contact sync.
type ContactClient interface {
	ListContacts(ctx context.Context) (*gmproto.ListContactsResponse, error)
}

// Adapter is bound to exactly one paired physical phone connection.
type Adapter struct {
	connection domain.Connection
	client     ContactClient
}

func New(connection domain.Connection, client ContactClient) (*Adapter, error) {
	if connection.ID == "" || connection.TenantID == "" {
		return nil, ErrInvalidConnection
	}
	if client == nil {
		return nil, ErrInvalidClient
	}
	return &Adapter{connection: connection, client: client}, nil
}

func (adapter *Adapter) Connection() domain.Connection {
	return adapter.connection
}

// ListContacts implements contactsync.Provider. The incoming connection must
// match the adapter binding before libgm is called.
func (adapter *Adapter) ListContacts(ctx context.Context, connection domain.Connection) ([]contactsync.ProviderContact, error) {
	if connection.ID != adapter.connection.ID || connection.TenantID != adapter.connection.TenantID {
		return nil, contactsync.ErrConnectionAccessDenied
	}
	response, err := adapter.client.ListContacts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list Google Messages contacts: %w", err)
	}
	contacts := make([]contactsync.ProviderContact, 0, len(response.GetContacts()))
	for _, contact := range response.GetContacts() {
		contacts = append(contacts, contactsync.ProviderContact{
			ID:          providerContactID(contact),
			PhoneNumber: contact.GetNumber().GetNumber(),
			DisplayName: contact.GetName(),
		})
	}
	return contacts, nil
}

func providerContactID(contact *gmproto.Contact) string {
	if contactID := strings.TrimSpace(contact.GetContactID()); contactID != "" {
		return "contact:" + contactID
	}
	if participantID := strings.TrimSpace(contact.GetParticipantID()); participantID != "" {
		return "participant:" + participantID
	}
	// contactsync.Service deterministically quarantines a blank provider ID.
	return ""
}

type LineRejectionReason string

const (
	LineRejectionMissingProviderIdentity   LineRejectionReason = "missing_provider_identity"
	LineRejectionDuplicateProviderIdentity LineRejectionReason = "duplicate_provider_identity"
	LineRejectionInvalidPhoneNumber        LineRejectionReason = "invalid_phone_number"
)

// DiscoveredLine carries provider metadata that is not part of the core
// domain.Line routing identity.
type DiscoveredLine struct {
	Line                   domain.Line
	Phone                  domain.E164Phone
	CarrierName            string
	ColorHex               string
	RCSEnabled             bool
	ProviderSIMNumber      int32
	ProviderSIMPayloadType int32
}

type RejectedLine struct {
	ProviderParticipantID string
	PhoneNumber           string
	Reason                LineRejectionReason
}

type LineMapResult struct {
	Lines    []DiscoveredLine
	Rejected []RejectedLine
}

// MapLines maps the latest settings event for this adapter's connection. It
// prefers the protocol's international number and only uses the formatted
// number when the international field is absent; it never guesses a country.
func (adapter *Adapter) MapLines(settings *gmproto.Settings) LineMapResult {
	return MapSettingsLines(adapter.connection, settings)
}

// MapSettingsLines maps provider facts without requiring the contact RPC
// client. Runtime discovery uses it only for libgm.AuthenticatedSettings.
func MapSettingsLines(connection domain.Connection, settings *gmproto.Settings) LineMapResult {
	var result LineMapResult
	if connection.ID == "" || connection.TenantID == "" || settings == nil {
		return result
	}
	identityCounts := make(map[string]int)
	for _, sim := range settings.GetSIMCards() {
		if participantID := strings.TrimSpace(sim.GetSIMParticipant().GetID()); domain.ValidProviderIdentifier(participantID) {
			identityCounts[participantID]++
		}
	}
	for _, sim := range settings.GetSIMCards() {
		participantID := strings.TrimSpace(sim.GetSIMParticipant().GetID())
		phoneNumber := strings.TrimSpace(sim.GetSIMData().GetInternationalPhoneNumber())
		if phoneNumber == "" {
			phoneNumber = strings.TrimSpace(sim.GetSIMData().GetFormattedPhoneNumber())
		}
		if !domain.ValidProviderIdentifier(participantID) {
			result.Rejected = append(result.Rejected, RejectedLine{
				PhoneNumber: phoneNumber,
				Reason:      LineRejectionMissingProviderIdentity,
			})
			continue
		}
		if identityCounts[participantID] > 1 {
			result.Rejected = append(result.Rejected, RejectedLine{
				ProviderParticipantID: participantID,
				PhoneNumber:           phoneNumber,
				Reason:                LineRejectionDuplicateProviderIdentity,
			})
			continue
		}
		phone, err := domain.ParseE164(phoneNumber)
		if err != nil {
			result.Rejected = append(result.Rejected, RejectedLine{
				ProviderParticipantID: participantID,
				PhoneNumber:           phoneNumber,
				Reason:                LineRejectionInvalidPhoneNumber,
			})
			continue
		}
		carrierName := safeProviderLineMetadata(sim.GetSIMData().GetCarrierName(), 255)
		displayName := carrierName
		if displayName == "" {
			displayName = phone.String()
		}
		result.Lines = append(result.Lines, DiscoveredLine{
			Line: domain.Line{
				ID:                    lineID(connection, participantID),
				TenantID:              connection.TenantID,
				ConnectionID:          connection.ID,
				ProviderParticipantID: participantID,
				ProviderOutgoingID:    participantID,
				DisplayName:           displayName,
			},
			Phone:                  phone,
			CarrierName:            carrierName,
			ColorHex:               safeProviderLineMetadata(sim.GetSIMData().GetColorHex(), 64),
			RCSEnabled:             sim.GetRCSChats().GetEnabled(),
			ProviderSIMNumber:      sim.GetSIMData().GetSIMPayload().GetSIMNumber(),
			ProviderSIMPayloadType: sim.GetSIMData().GetSIMPayload().GetTwo(),
		})
	}
	return result
}

func lineID(connection domain.Connection, participantID string) domain.LineID {
	digest := sha256.New()
	for _, value := range []string{string(connection.TenantID), string(connection.ID), participantID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	return domain.LineID("gmessages:" + base64.RawURLEncoding.EncodeToString(digest.Sum(nil)))
}

func safeProviderLineMetadata(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) > limit || strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	return value
}

type RouteLimitation string

const (
	RouteLimitationNone         RouteLimitation = ""
	RouteLimitationPhoneDefault RouteLimitation = "phone_default_only"
)

type Route struct {
	ProviderOutgoingID string
	UsesPhoneDefault   bool
	Limitation         RouteLimitation
}

// LineRoutingError is returned before provider dispatch whenever the caller's
// requested line cannot be proven and honored by the Google protocol.
type LineRoutingError struct {
	Kind                        error
	ConversationOutgoingID      string
	RequestedProviderOutgoingID string
}

func (err *LineRoutingError) Error() string {
	if err.RequestedProviderOutgoingID == "" {
		return err.Kind.Error()
	}
	return fmt.Sprintf("%v: conversation uses %q, requested %q", err.Kind, err.ConversationOutgoingID, err.RequestedProviderOutgoingID)
}

func (err *LineRoutingError) Unwrap() error {
	return err.Kind
}

// RouteExistingConversation preserves the outgoing identity already selected
// by Google. An explicit gateway line is accepted only when it matches.
func (adapter *Adapter) RouteExistingConversation(conversation *gmproto.Conversation, requested *domain.Line) (Route, error) {
	if requested != nil && (requested.TenantID != adapter.connection.TenantID || requested.ConnectionID != adapter.connection.ID) {
		return Route{}, &LineRoutingError{Kind: ErrScopeMismatch}
	}
	providerOutgoingID := strings.TrimSpace(conversation.GetDefaultOutgoingID())
	if providerOutgoingID == "" {
		return Route{}, &LineRoutingError{Kind: ErrLineSelectionUnsupported}
	}
	if requested != nil && strings.TrimSpace(requested.ProviderOutgoingID) != providerOutgoingID {
		return Route{}, &LineRoutingError{
			Kind:                        ErrLineMismatch,
			ConversationOutgoingID:      providerOutgoingID,
			RequestedProviderOutgoingID: strings.TrimSpace(requested.ProviderOutgoingID),
		}
	}
	return Route{ProviderOutgoingID: providerOutgoingID}, nil
}

// RouteNewConversation documents the protocol limitation: the create request
// has no originating SIM field. Without an explicit line Google may use the
// phone default; an explicit line request is rejected before dispatch.
func (adapter *Adapter) RouteNewConversation(requested *domain.Line) (Route, error) {
	if requested != nil && (requested.TenantID != adapter.connection.TenantID || requested.ConnectionID != adapter.connection.ID) {
		return Route{}, &LineRoutingError{Kind: ErrScopeMismatch}
	}
	if requested != nil {
		return Route{}, &LineRoutingError{
			Kind:                        ErrLineSelectionUnsupported,
			RequestedProviderOutgoingID: strings.TrimSpace(requested.ProviderOutgoingID),
		}
	}
	return Route{UsesPhoneDefault: true, Limitation: RouteLimitationPhoneDefault}, nil
}
