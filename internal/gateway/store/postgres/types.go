package postgres

import (
	"errors"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

var (
	ErrInvalidEncryptedSession  = errors.New("invalid encrypted session envelope")
	ErrInvalidFingerprint       = errors.New("invalid provider device fingerprint")
	ErrInvalidCursor            = errors.New("invalid contact cursor")
	ErrContactNotFound          = errors.New("contact not found")
	ErrLabelNotFound            = errors.New("label not found")
	ErrContactLabelLinkNotFound = errors.New("contact-label link not found")
	ErrEncryptedSessionNotFound = errors.New("encrypted session not found")
	ErrContactSyncRunNotFound   = errors.New("contact sync run not found")
	ErrConnectionLeaseLost      = errors.New("connection lease fence lost")
)

type ConnectionRecord struct {
	Connection                domain.Connection
	ProviderDeviceFingerprint []byte
}

type ConnectionLease struct {
	OwnerID      string
	FencingToken uint64
	ExpiresAt    time.Time
}

type ConnectionActorHealth struct {
	ActorState              string
	LeaseState              string
	ConnectionState         domain.ConnectionState
	ConnectedAt             *time.Time
	LastFrameAt             *time.Time
	LastPhoneResponseAt     *time.Time
	ReconnectCount          uint64
	CurrentBackoff          time.Duration
	LastSafeReason          string
	RequiresReauthorization bool
	FencingToken            uint64
	UpdatedAt               time.Time
}

type LineRecord struct {
	Line                   domain.Line
	Phone                  domain.E164Phone
	CarrierName            string
	ColorHex               string
	RCSEnabled             bool
	ProviderSIMNumber      int32
	ProviderSIMPayloadType int32
	DiscoverySource        LineDiscoverySource
}

type LineDiscoverySource string

const (
	LineDiscoveryLegacyUnknown               LineDiscoverySource = "legacy_unknown"
	LineDiscoveryAuthenticatedGoogleSettings LineDiscoverySource = "authenticated_google_settings"
)

type EncryptedSession = session.Envelope

type ContactListOptions struct {
	After domain.ContactID
	Limit int
}

type ContactPage struct {
	Contacts   []domain.Contact
	NextCursor domain.ContactID
}

type ContactSyncStatus string

const (
	ContactSyncRunning   ContactSyncStatus = "running"
	ContactSyncSucceeded ContactSyncStatus = "succeeded"
	ContactSyncFailed    ContactSyncStatus = "failed"
)

type ContactSyncRun struct {
	ID            string
	TenantID      domain.TenantID
	ConnectionID  domain.ConnectionID
	Status        ContactSyncStatus
	ImportedCount int
	RejectedCount int
	ErrorSummary  string
	StartedAt     time.Time
	FinishedAt    *time.Time
}
