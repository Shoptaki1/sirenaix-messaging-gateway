package contactsync_test

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestSyncConvergesDuplicateNumbersAndPreservesServerMetadata(t *testing.T) {
	ctx := context.Background()
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{
		{ID: "provider-1", PhoneNumber: "+1 (202) 555-0123", DisplayName: "Old Phone Name"},
		{ID: "provider-2", PhoneNumber: "+12025550123", DisplayName: "Second Source"},
	}}
	repository := newContactRepository()
	repository.seed(domain.Contact{
		ID: "contact-1", TenantID: "tenant-1", Phone: mustPhone(t, "+12025550123"),
		Alias: "Server Alias", LabelIDs: []domain.LabelID{"label-1"},
	})
	service := mustService(t, connections, provider, repository)

	first, err := service.Sync(ctx, "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	if got, want := len(first.Contacts), 1; got != want {
		t.Fatalf("first contacts = %d, want %d", got, want)
	}
	if got, want := first.Contacts[0].Alias, "Server Alias"; got != want {
		t.Fatalf("alias = %q, want %q", got, want)
	}
	if got, want := first.Contacts[0].LabelIDs, []domain.LabelID{"label-1"}; !equalLabels(got, want) {
		t.Fatalf("labels = %v, want %v", got, want)
	}
	if got, want := repository.contactCount(), 1; got != want {
		t.Fatalf("stored contacts = %d, want %d", got, want)
	}
	if got, want := repository.sourceCount(), 2; got != want {
		t.Fatalf("stored sources = %d, want %d", got, want)
	}
	for _, providerID := range []string{"provider-1", "provider-2"} {
		if !repository.hasSource("tenant-1", "connection-1", providerID) {
			t.Errorf("missing tenant-scoped source for %q", providerID)
		}
	}

	provider.contacts = []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "Updated Phone Name",
	}}
	second, err := service.Sync(ctx, "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("second Sync() error = %v", err)
	}
	if got, want := len(second.Contacts), 1; got != want {
		t.Fatalf("second contacts = %d, want %d", got, want)
	}
	contact := second.Contacts[0]
	if got, want := contact.ProviderDisplayName, "Updated Phone Name"; got != want {
		t.Fatalf("provider name = %q, want %q", got, want)
	}
	if got, want := contact.Alias, "Server Alias"; got != want {
		t.Fatalf("alias after resync = %q, want %q", got, want)
	}
	if got, want := contact.LabelIDs, []domain.LabelID{"label-1"}; !equalLabels(got, want) {
		t.Fatalf("labels after resync = %v, want %v", got, want)
	}
	if got, want := repository.contactCount(), 1; got != want {
		t.Fatalf("stored contacts after resync = %d, want %d", got, want)
	}
	if got, want := repository.sourceCount(), 2; got != want {
		t.Fatalf("stored sources after resync = %d, want %d", got, want)
	}
}

func TestSyncRejectsInvalidNumbersWithoutUpsert(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{
		{ID: "empty", PhoneNumber: "", DisplayName: "Empty"},
		{ID: "local", PhoneNumber: "2025550123", DisplayName: "Local"},
		{ID: "valid", PhoneNumber: "+12025550123", DisplayName: "Valid"},
	}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	result, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got, want := len(result.Rejected), 2; got != want {
		t.Fatalf("rejected = %d, want %d", got, want)
	}
	for _, rejected := range result.Rejected {
		if rejected.Reason != contactsync.RejectionInvalidPhoneNumber {
			t.Errorf("rejection %q reason = %q, want %q", rejected.ProviderContactID, rejected.Reason, contactsync.RejectionInvalidPhoneNumber)
		}
	}
	if got, want := repository.upsertCalls, 1; got != want {
		t.Fatalf("upsert calls = %d, want %d", got, want)
	}
}

func TestSyncFailsClosedBeforeListingCrossTenantConnection(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-2"},
	}}
	provider := &contactProvider{}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	_, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if !errors.Is(err, contactsync.ErrConnectionAccessDenied) {
		t.Fatalf("Sync() error = %v, want ErrConnectionAccessDenied", err)
	}
	if provider.listCalls != 0 {
		t.Fatalf("provider list calls = %d, want 0", provider.listCalls)
	}
	if repository.upsertCalls != 0 {
		t.Fatalf("repository upsert calls = %d, want 0", repository.upsertCalls)
	}
}

func TestSyncFailsClosedIfRepositoryReturnsCrossTenantContact(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "Phone Name",
	}}}
	repository := newContactRepository()
	repository.returnTenantID = "tenant-2"
	service := mustService(t, connections, provider, repository)

	result, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if !errors.Is(err, contactsync.ErrContactAccessDenied) {
		t.Fatalf("Sync() error = %v, want ErrContactAccessDenied", err)
	}
	if len(result.Contacts) != 0 {
		t.Fatalf("returned contacts = %v, want none", result.Contacts)
	}
}

func TestSyncUsesTenantScopedConnectionLookup(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{}
	service := mustService(t, connections, provider, newContactRepository())

	_, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got, want := connections.lookupTenantID, domain.TenantID("tenant-1"); got != want {
		t.Fatalf("connection lookup tenant = %q, want %q", got, want)
	}
}

func TestSyncRejectsBlankProviderContactIDWithoutUpsert(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{
		{ID: "  ", PhoneNumber: "+12025550123", DisplayName: "Missing ID"},
		{ID: "valid", PhoneNumber: "+13125550123", DisplayName: "Valid"},
	}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	result, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got, want := repository.upsertCalls, 1; got != want {
		t.Fatalf("upsert calls = %d, want %d", got, want)
	}
	if got, want := len(result.Rejected), 1; got != want {
		t.Fatalf("rejected = %d, want %d", got, want)
	}
	if got, want := result.Rejected[0].Reason, contactsync.RejectionInvalidProviderContactID; got != want {
		t.Fatalf("rejection reason = %q, want %q", got, want)
	}
}

func TestSyncDeduplicatesRepeatedProviderIDForSameCanonicalPhone(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{
		{ID: "provider-1", PhoneNumber: "+1 (202) 555-0123", DisplayName: "First"},
		{ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "Duplicate"},
	}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	result, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got, want := repository.upsertCalls, 1; got != want {
		t.Fatalf("upsert calls = %d, want %d", got, want)
	}
	if got, want := len(result.Contacts), 1; got != want {
		t.Fatalf("contacts = %d, want %d", got, want)
	}
	if got := len(result.Rejected); got != 0 {
		t.Fatalf("rejected = %d, want 0", got)
	}
}

func TestSyncQuarantinesProviderIDConflictWithoutRemappingSource(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{
		{ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "First"},
		{ID: "provider-1", PhoneNumber: "+13125550123", DisplayName: "Conflicting"},
	}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	result, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if got, want := repository.upsertCalls, 1; got != want {
		t.Fatalf("upsert calls = %d, want %d", got, want)
	}
	if got, want := len(result.Rejected), 1; got != want {
		t.Fatalf("rejected = %d, want %d", got, want)
	}
	if got, want := result.Rejected[0].Reason, contactsync.RejectionProviderIdentityConflict; got != want {
		t.Fatalf("rejection reason = %q, want %q", got, want)
	}
	if got, want := repository.sourcePhone("tenant-1", "connection-1", "provider-1"), "+12025550123"; got != want {
		t.Fatalf("stored source phone = %q, want %q", got, want)
	}
}

func TestSyncQuarantinesProviderIdentityConflictAcrossSyncRuns(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "First",
	}}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	if _, err := service.Sync(context.Background(), "tenant-1", "connection-1"); err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	provider.contacts = []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+13125550123", DisplayName: "Conflicting",
	}}
	second, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("second Sync() error = %v", err)
	}
	if got, want := len(second.Rejected), 1; got != want {
		t.Fatalf("second rejected = %d, want %d", got, want)
	}
	if got, want := second.Rejected[0].Reason, contactsync.RejectionProviderIdentityConflict; got != want {
		t.Fatalf("rejection reason = %q, want %q", got, want)
	}
	if got := len(second.Contacts); got != 0 {
		t.Fatalf("second contacts = %d, want 0", got)
	}
	if got, want := repository.sourcePhone("tenant-1", "connection-1", "provider-1"), "+12025550123"; got != want {
		t.Fatalf("stored source phone = %q, want %q", got, want)
	}
	if got, want := repository.contactCount(), 1; got != want {
		t.Fatalf("stored contacts = %d, want %d", got, want)
	}
	if got, want := repository.successfulUpserts, 1; got != want {
		t.Fatalf("successful upserts = %d, want %d", got, want)
	}
}

func TestSyncKeepsProviderIdentityIdempotentAcrossSyncRuns(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+1 (202) 555-0123", DisplayName: "First",
	}}}
	repository := newContactRepository()
	service := mustService(t, connections, provider, repository)

	if _, err := service.Sync(context.Background(), "tenant-1", "connection-1"); err != nil {
		t.Fatalf("first Sync() error = %v", err)
	}
	provider.contacts = []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "Updated",
	}}
	second, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if err != nil {
		t.Fatalf("second Sync() error = %v", err)
	}
	if got := len(second.Rejected); got != 0 {
		t.Fatalf("second rejected = %d, want 0", got)
	}
	if got, want := len(second.Contacts), 1; got != want {
		t.Fatalf("second contacts = %d, want %d", got, want)
	}
	if got, want := second.Contacts[0].ProviderDisplayName, "Updated"; got != want {
		t.Fatalf("provider name = %q, want %q", got, want)
	}
	if got, want := repository.sourcePhone("tenant-1", "connection-1", "provider-1"), "+12025550123"; got != want {
		t.Fatalf("stored source phone = %q, want %q", got, want)
	}
	if got, want := repository.contactCount(), 1; got != want {
		t.Fatalf("stored contacts = %d, want %d", got, want)
	}
	if got, want := repository.sourceCount(), 1; got != want {
		t.Fatalf("stored sources = %d, want %d", got, want)
	}
}

func TestSyncPropagatesUnrelatedRepositoryError(t *testing.T) {
	connections := &connectionRepository{connections: map[domain.ConnectionID]domain.Connection{
		"connection-1": {ID: "connection-1", TenantID: "tenant-1"},
	}}
	provider := &contactProvider{contacts: []contactsync.ProviderContact{{
		ID: "provider-1", PhoneNumber: "+12025550123", DisplayName: "First",
	}}}
	repositoryFailure := errors.New("repository unavailable")
	repository := newContactRepository()
	repository.upsertErr = repositoryFailure
	service := mustService(t, connections, provider, repository)

	_, err := service.Sync(context.Background(), "tenant-1", "connection-1")
	if !errors.Is(err, repositoryFailure) {
		t.Fatalf("Sync() error = %v, want repository failure", err)
	}
}

func TestNewServiceRejectsNilDependencies(t *testing.T) {
	connections := &connectionRepository{}
	provider := &contactProvider{}
	repository := newContactRepository()
	var nilConnections *connectionRepository
	var nilProvider *contactProvider
	var nilRepository *contactRepository
	tests := []struct {
		name        string
		connections contactsync.ConnectionRepository
		provider    contactsync.Provider
		contacts    contactsync.ContactRepository
	}{
		{name: "connection repository", connections: nil, provider: provider, contacts: repository},
		{name: "provider", connections: connections, provider: nil, contacts: repository},
		{name: "contact repository", connections: connections, provider: provider, contacts: nil},
		{name: "typed nil connection repository", connections: nilConnections, provider: provider, contacts: repository},
		{name: "typed nil provider", connections: connections, provider: nilProvider, contacts: repository},
		{name: "typed nil contact repository", connections: connections, provider: provider, contacts: nilRepository},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := contactsync.NewService(test.connections, test.provider, test.contacts)
			if !errors.Is(err, contactsync.ErrInvalidDependency) {
				t.Fatalf("NewService() error = %v, want ErrInvalidDependency", err)
			}
			if service != nil {
				t.Fatalf("NewService() service = %v, want nil", service)
			}
		})
	}
}

type connectionRepository struct {
	connections    map[domain.ConnectionID]domain.Connection
	lookupTenantID domain.TenantID
}

func (r *connectionRepository) GetConnection(_ context.Context, tenantID domain.TenantID, id domain.ConnectionID) (domain.Connection, error) {
	r.lookupTenantID = tenantID
	connection, ok := r.connections[id]
	if !ok {
		return domain.Connection{}, contactsync.ErrConnectionNotFound
	}
	return connection, nil
}

type contactProvider struct {
	contacts  []contactsync.ProviderContact
	listCalls int
}

func (p *contactProvider) ListContacts(_ context.Context, _ domain.Connection) ([]contactsync.ProviderContact, error) {
	p.listCalls++
	return p.contacts, nil
}

type contactRepository struct {
	contacts          map[string]domain.Contact
	sources           map[string]contactsync.ProviderContactUpsert
	nextID            int
	upsertCalls       int
	successfulUpserts int
	returnTenantID    domain.TenantID
	upsertErr         error
}

func newContactRepository() *contactRepository {
	return &contactRepository{
		contacts: make(map[string]domain.Contact),
		sources:  make(map[string]contactsync.ProviderContactUpsert),
	}
}

func (r *contactRepository) seed(contact domain.Contact) {
	r.contacts[string(contact.TenantID)+"\x00"+contact.Phone.String()] = contact
}

func (r *contactRepository) UpsertProviderContact(_ context.Context, update contactsync.ProviderContactUpsert) (domain.Contact, error) {
	r.upsertCalls++
	if r.upsertErr != nil {
		return domain.Contact{}, r.upsertErr
	}
	sourceKey := string(update.TenantID) + "\x00" + string(update.ConnectionID) + "\x00" + update.ProviderContactID
	if source, exists := r.sources[sourceKey]; exists && source.Phone != update.Phone {
		return domain.Contact{}, contactsync.ErrProviderIdentityConflict
	}
	contactKey := string(update.TenantID) + "\x00" + update.Phone.String()
	contact, ok := r.contacts[contactKey]
	if !ok {
		r.nextID++
		contact = domain.Contact{ID: domain.ContactID("generated-contact"), TenantID: update.TenantID, Phone: update.Phone}
	}
	contact.ProviderDisplayName = update.ProviderDisplayName
	if r.returnTenantID != "" {
		contact.TenantID = r.returnTenantID
	}
	r.contacts[contactKey] = contact
	r.sources[sourceKey] = update
	r.successfulUpserts++
	return contact, nil
}

func (r *contactRepository) contactCount() int { return len(r.contacts) }
func (r *contactRepository) sourceCount() int  { return len(r.sources) }
func (r *contactRepository) hasSource(tenantID domain.TenantID, connectionID domain.ConnectionID, providerID string) bool {
	key := string(tenantID) + "\x00" + string(connectionID) + "\x00" + providerID
	_, ok := r.sources[key]
	return ok
}

func (r *contactRepository) sourcePhone(tenantID domain.TenantID, connectionID domain.ConnectionID, providerID string) string {
	key := string(tenantID) + "\x00" + string(connectionID) + "\x00" + providerID
	return r.sources[key].Phone.String()
}

func mustService(t *testing.T, connections contactsync.ConnectionRepository, provider contactsync.Provider, repository contactsync.ContactRepository) *contactsync.Service {
	t.Helper()
	service, err := contactsync.NewService(connections, provider, repository)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func mustPhone(t *testing.T, input string) domain.E164Phone {
	t.Helper()
	phone, err := domain.ParseE164(input)
	if err != nil {
		t.Fatal(err)
	}
	return phone
}

func equalLabels(a, b []domain.LabelID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
