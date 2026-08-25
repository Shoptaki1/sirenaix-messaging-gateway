package contacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contacts"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type serverContactStore struct {
	contact   domain.Contact
	tenant    domain.TenantID
	createdID domain.ContactID
	phone     domain.E164Phone
	alias     *string
}

func (store *serverContactStore) UpsertServerContact(_ context.Context, tenantID domain.TenantID, createdID domain.ContactID, phone domain.E164Phone, alias *string) (domain.Contact, error) {
	store.tenant, store.createdID, store.phone, store.alias = tenantID, createdID, phone, alias
	if store.contact.ID == "" {
		store.contact = domain.Contact{ID: createdID, TenantID: tenantID, Phone: phone}
		if alias != nil {
			store.contact.Alias = *alias
		}
	}
	return store.contact, nil
}

func TestServiceUpsertsCanonicalPhoneAndOptionalAlias(t *testing.T) {
	store := &serverContactStore{}
	service, err := contacts.NewService(store, func() string { return "contact-a" })
	if err != nil {
		t.Fatal(err)
	}
	alias := "  Potential Client  "
	contact, err := service.Upsert(context.Background(), "tenant-a", contacts.UpsertInput{Phone: "+12025550100", ServerAlias: &alias})
	if err != nil {
		t.Fatal(err)
	}
	if contact.ID != "contact-a" || contact.Phone.String() != "+12025550100" || contact.Alias != "Potential Client" {
		t.Fatalf("contact = %#v", contact)
	}
	if store.tenant != "tenant-a" || store.createdID != "contact-a" || store.phone.String() != "+12025550100" || store.alias == nil || *store.alias != "Potential Client" {
		t.Fatalf("store call = tenant=%q id=%q phone=%q alias=%v", store.tenant, store.createdID, store.phone.String(), store.alias)
	}
}

func TestServicePreservesExistingServerValuesWhenAliasIsOmitted(t *testing.T) {
	phone, _ := domain.ParseE164("+12025550100")
	store := &serverContactStore{contact: domain.Contact{ID: "existing", TenantID: "tenant-a", Phone: phone, Alias: "AI lead", ProviderDisplayName: "Phone Name", LabelIDs: []domain.LabelID{"label-a"}}}
	service, _ := contacts.NewService(store, func() string { return "unused" })
	contact, err := service.Upsert(context.Background(), "tenant-a", contacts.UpsertInput{Phone: "+12025550100"})
	if err != nil {
		t.Fatal(err)
	}
	if store.alias != nil || contact.Alias != "AI lead" || contact.ProviderDisplayName != "Phone Name" || len(contact.LabelIDs) != 1 {
		t.Fatalf("server/provider values were not preserved: %#v", contact)
	}
}

func TestServiceRejectsInvalidPhoneAliasAndCrossTenantResult(t *testing.T) {
	store := &serverContactStore{}
	service, _ := contacts.NewService(store, func() string { return "contact-a" })
	tooLong := strings.Repeat("a", contacts.MaxServerAliasBytes+1)
	nul := "lead\x00private"
	for name, input := range map[string]contacts.UpsertInput{
		"local phone":     {Phone: "202-555-0100"},
		"formatted phone": {Phone: "+1 (202) 555-0100"},
		"empty phone":     {},
		"oversized alias": {Phone: "+12025550100", ServerAlias: &tooLong},
		"nul alias":       {Phone: "+12025550100", ServerAlias: &nul},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := service.Upsert(context.Background(), "tenant-a", input); !errors.Is(err, contacts.ErrInvalidContact) {
				t.Fatalf("Upsert() error = %v", err)
			}
		})
	}
	phone, _ := domain.ParseE164("+12025550100")
	store.contact = domain.Contact{ID: "contact-a", TenantID: "tenant-b", Phone: phone}
	if _, err := service.Upsert(context.Background(), "tenant-a", contacts.UpsertInput{Phone: phone.String()}); !errors.Is(err, domain.ErrTenantBoundary) {
		t.Fatalf("cross-tenant result error = %v", err)
	}
	invalidIDService, _ := contacts.NewService(&serverContactStore{}, func() string { return string([]byte{0xff}) })
	if _, err := invalidIDService.Upsert(context.Background(), "tenant-a", contacts.UpsertInput{Phone: phone.String()}); !errors.Is(err, contacts.ErrInvalidContact) {
		t.Fatalf("invalid generated contact ID error = %v", err)
	}
}
