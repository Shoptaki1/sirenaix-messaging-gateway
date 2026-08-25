package domain

import "fmt"

type ConnectionState string

const (
	ConnectionStateUnpaired                ConnectionState = "unpaired"
	ConnectionStatePairing                 ConnectionState = "pairing"
	ConnectionStateConnected               ConnectionState = "connected"
	ConnectionStateDegraded                ConnectionState = "degraded"
	ConnectionStateReauthorizationRequired ConnectionState = "reauthorization-required"
	ConnectionStateSuspended               ConnectionState = "suspended"
	ConnectionStateDisconnected            ConnectionState = "disconnected"
)

func (state ConnectionState) Validate() error {
	switch state {
	case ConnectionStateUnpaired,
		ConnectionStatePairing,
		ConnectionStateConnected,
		ConnectionStateDegraded,
		ConnectionStateReauthorizationRequired,
		ConnectionStateSuspended,
		ConnectionStateDisconnected:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidConnectionState, state)
	}
}

type Tenant struct {
	ID   TenantID
	Name string
}

type Connection struct {
	ID       ConnectionID
	TenantID TenantID
	Name     string
	State    ConnectionState
}

// Line is a provider-discovered SIM route on exactly one physical connection.
// The provider IDs are opaque and must be preserved for explicit routing.
type Line struct {
	ID                    LineID
	TenantID              TenantID
	ConnectionID          ConnectionID
	ProviderParticipantID string
	ProviderOutgoingID    string
	DisplayName           string
}

func (line Line) ValidateFor(connection Connection) error {
	if line.TenantID == "" || connection.TenantID == "" || line.TenantID != connection.TenantID {
		return fmt.Errorf("%w: line %q does not belong to connection tenant", ErrTenantBoundary, line.ID)
	}
	if line.ConnectionID == "" || connection.ID == "" || line.ConnectionID != connection.ID {
		return fmt.Errorf("%w: line %q does not belong to connection %q", ErrConnectionBoundary, line.ID, connection.ID)
	}
	return nil
}
