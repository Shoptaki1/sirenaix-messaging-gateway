package contactsync

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var (
	ErrConnectionNotFound       = errors.New("connection not found")
	ErrConnectionAccessDenied   = errors.New("connection access denied")
	ErrContactAccessDenied      = errors.New("contact access denied")
	ErrInvalidDependency        = errors.New("invalid service dependency")
	ErrProviderIdentityConflict = errors.New("provider contact identity conflict")
)

type ProviderContact struct {
	ID          string
	PhoneNumber string
	DisplayName string
}

type ProviderContactUpsert struct {
	TenantID            domain.TenantID
	ConnectionID        domain.ConnectionID
	ProviderContactID   string
	Phone               domain.E164Phone
	ProviderDisplayName string
}

// ConnectionRepository performs a tenant-scoped lookup. Service.Sync also
// validates the returned tenant before any provider operation.
type ConnectionRepository interface {
	GetConnection(ctx context.Context, tenantID domain.TenantID, id domain.ConnectionID) (domain.Connection, error)
}

type Provider interface {
	ListContacts(ctx context.Context, connection domain.Connection) ([]ProviderContact, error)
}

// ContactRepository atomically associates the provider source with the
// tenant's canonical phone contact. A source key (tenant, connection, provider
// contact ID) is immutable: if it already maps to another phone, implementations
// must make no changes and return ErrProviderIdentityConflict. Implementations
// must update only provider metadata; Alias and LabelIDs are server-owned and
// must be preserved.
type ContactRepository interface {
	UpsertProviderContact(ctx context.Context, update ProviderContactUpsert) (domain.Contact, error)
}

type RejectionReason string

const (
	RejectionInvalidPhoneNumber       RejectionReason = "invalid_phone_number"
	RejectionInvalidProviderContactID RejectionReason = "invalid_provider_contact_id"
	RejectionProviderIdentityConflict RejectionReason = "provider_identity_conflict"
)

type RejectedContact struct {
	ProviderContactID string
	PhoneNumber       string
	Reason            RejectionReason
}

type SyncResult struct {
	Contacts []domain.Contact
	Rejected []RejectedContact
}

type Service struct {
	connections ConnectionRepository
	provider    Provider
	contacts    ContactRepository
}

func NewService(connections ConnectionRepository, provider Provider, contacts ContactRepository) (*Service, error) {
	if isNilDependency(connections) {
		return nil, fmt.Errorf("%w: connection repository", ErrInvalidDependency)
	}
	if isNilDependency(provider) {
		return nil, fmt.Errorf("%w: provider", ErrInvalidDependency)
	}
	if isNilDependency(contacts) {
		return nil, fmt.Errorf("%w: contact repository", ErrInvalidDependency)
	}
	return &Service{connections: connections, provider: provider, contacts: contacts}, nil
}

func isNilDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (service *Service) Sync(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (SyncResult, error) {
	var result SyncResult
	if tenantID == "" || connectionID == "" {
		return result, ErrConnectionAccessDenied
	}

	connection, err := service.connections.GetConnection(ctx, tenantID, connectionID)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			return result, ErrConnectionAccessDenied
		}
		return result, fmt.Errorf("get connection: %w", err)
	}
	if connection.ID != connectionID || connection.TenantID != tenantID {
		return result, ErrConnectionAccessDenied
	}

	providerContacts, err := service.provider.ListContacts(ctx, connection)
	if err != nil {
		return result, fmt.Errorf("list provider contacts: %w", err)
	}

	contactIndexes := make(map[string]int)
	providerIdentities := make(map[string]domain.E164Phone)
	for _, providerContact := range providerContacts {
		if strings.TrimSpace(providerContact.ID) == "" {
			result.Rejected = append(result.Rejected, RejectedContact{
				ProviderContactID: providerContact.ID,
				PhoneNumber:       providerContact.PhoneNumber,
				Reason:            RejectionInvalidProviderContactID,
			})
			continue
		}

		phone, parseErr := domain.ParseE164(providerContact.PhoneNumber)
		if parseErr != nil {
			result.Rejected = append(result.Rejected, RejectedContact{
				ProviderContactID: providerContact.ID,
				PhoneNumber:       providerContact.PhoneNumber,
				Reason:            RejectionInvalidPhoneNumber,
			})
			continue
		}
		if existingPhone, exists := providerIdentities[providerContact.ID]; exists {
			if existingPhone != phone {
				result.Rejected = append(result.Rejected, RejectedContact{
					ProviderContactID: providerContact.ID,
					PhoneNumber:       providerContact.PhoneNumber,
					Reason:            RejectionProviderIdentityConflict,
				})
			}
			continue
		}
		providerIdentities[providerContact.ID] = phone

		contact, upsertErr := service.contacts.UpsertProviderContact(ctx, ProviderContactUpsert{
			TenantID:            tenantID,
			ConnectionID:        connectionID,
			ProviderContactID:   providerContact.ID,
			Phone:               phone,
			ProviderDisplayName: strings.TrimSpace(providerContact.DisplayName),
		})
		if upsertErr != nil {
			if errors.Is(upsertErr, ErrProviderIdentityConflict) {
				result.Rejected = append(result.Rejected, RejectedContact{
					ProviderContactID: providerContact.ID,
					PhoneNumber:       providerContact.PhoneNumber,
					Reason:            RejectionProviderIdentityConflict,
				})
				continue
			}
			return result, fmt.Errorf("upsert provider contact %q: %w", providerContact.ID, upsertErr)
		}
		if contact.TenantID != tenantID || contact.Phone != phone {
			return SyncResult{Rejected: result.Rejected}, ErrContactAccessDenied
		}

		key := phone.String()
		if index, exists := contactIndexes[key]; exists {
			result.Contacts[index] = contact
		} else {
			contactIndexes[key] = len(result.Contacts)
			result.Contacts = append(result.Contacts, contact)
		}
	}

	return result, nil
}
