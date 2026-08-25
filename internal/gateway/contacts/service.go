// Package contacts manages tenant-owned contact data independently from the
// provider address-book import. Server aliases and labels never write back to
// the paired phone.
package contacts

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const MaxServerAliasBytes = 256

var ErrInvalidContact = errors.New("invalid server contact")

type Store interface {
	UpsertServerContact(context.Context, domain.TenantID, domain.ContactID, domain.E164Phone, *string) (domain.Contact, error)
}

type UpsertInput struct {
	Phone       string
	ServerAlias *string
}

type Service struct {
	store Store
	newID func() string
}

func NewService(store Store, newID func() string) (*Service, error) {
	if store == nil || newID == nil {
		return nil, ErrInvalidContact
	}
	return &Service{store: store, newID: newID}, nil
}

func (service *Service) Upsert(ctx context.Context, tenantID domain.TenantID, input UpsertInput) (domain.Contact, error) {
	if tenantID == "" {
		return domain.Contact{}, ErrInvalidContact
	}
	phone, err := domain.ParseE164(input.Phone)
	if err != nil || input.Phone != phone.String() {
		return domain.Contact{}, ErrInvalidContact
	}
	alias := input.ServerAlias
	if alias != nil {
		canonical := strings.TrimSpace(*alias)
		if len(canonical) > MaxServerAliasBytes || !utf8.ValidString(canonical) || strings.ContainsRune(canonical, '\x00') {
			return domain.Contact{}, ErrInvalidContact
		}
		alias = &canonical
	}
	createdID := domain.ContactID(service.newID())
	if createdID == "" || len(createdID) > 256 || !utf8.ValidString(string(createdID)) ||
		strings.TrimSpace(string(createdID)) != string(createdID) || strings.ContainsAny(string(createdID), "\x00\r\n") {
		return domain.Contact{}, ErrInvalidContact
	}
	contact, err := service.store.UpsertServerContact(ctx, tenantID, createdID, phone, alias)
	if err != nil {
		return domain.Contact{}, err
	}
	if contact.TenantID != tenantID {
		return domain.Contact{}, domain.ErrTenantBoundary
	}
	if contact.ID == "" || contact.Phone != phone {
		return domain.Contact{}, ErrInvalidContact
	}
	return contact, nil
}
