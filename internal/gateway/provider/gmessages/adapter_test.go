package gmessages

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

var _ contactsync.Provider = (*Adapter)(nil)

type contactClientStub struct {
	response *gmproto.ListContactsResponse
	err      error
}

func (stub *contactClientStub) ListContacts(context.Context) (*gmproto.ListContactsResponse, error) {
	return stub.response, stub.err
}

type singleConnectionRepository struct {
	connection domain.Connection
}

func (repository *singleConnectionRepository) GetConnection(context.Context, domain.TenantID, domain.ConnectionID) (domain.Connection, error) {
	return repository.connection, nil
}

type recordingContactRepository struct {
	upsertCalls int
}

func (repository *recordingContactRepository) UpsertProviderContact(_ context.Context, update contactsync.ProviderContactUpsert) (domain.Contact, error) {
	repository.upsertCalls++
	return domain.Contact{ID: "unexpected-contact", TenantID: update.TenantID, Phone: update.Phone}, nil
}

func TestListContactsMapsCurrentNameAndCanonicalNumber(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{response: &gmproto.ListContactsResponse{
		Contacts: []*gmproto.Contact{{
			ContactID:     "google-contact-17",
			ParticipantID: "participant-17",
			Name:          "Alex Example",
			Number:        &gmproto.ContactNumber{Number: "+12025550117", Number2: "not-canonical", FormattedNumber: stringPointer("(202) 555-0117")},
		}},
	}})

	contacts, err := adapter.ListContacts(context.Background(), adapter.Connection())
	if err != nil {
		t.Fatalf("ListContacts() error = %v", err)
	}
	if len(contacts) != 1 {
		t.Fatalf("ListContacts() count = %d, want 1", len(contacts))
	}
	want := contactsync.ProviderContact{
		ID:          "contact:google-contact-17",
		PhoneNumber: "+12025550117",
		DisplayName: "Alex Example",
	}
	if contacts[0] != want {
		t.Fatalf("ListContacts()[0] = %#v, want %#v", contacts[0], want)
	}
}

func TestListContactsUsesNamespacedParticipantFallbackAndLeavesMissingIDForQuarantine(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{response: &gmproto.ListContactsResponse{
		Contacts: []*gmproto.Contact{
			{ContactID: " shared-id ", ParticipantID: "participant-other", Number: &gmproto.ContactNumber{Number: "+12025550120"}},
			{ParticipantID: " shared-id ", Number: &gmproto.ContactNumber{Number: "+12025550121"}},
			{Number: &gmproto.ContactNumber{Number: "+12025550122"}},
		},
	}})

	contacts, err := adapter.ListContacts(context.Background(), adapter.Connection())
	if err != nil {
		t.Fatalf("ListContacts() error = %v", err)
	}
	if len(contacts) != 3 {
		t.Fatalf("ListContacts() count = %d, want 3", len(contacts))
	}
	if contacts[0].ID != "contact:shared-id" {
		t.Errorf("contact ID = %q, want namespaced Google contact ID", contacts[0].ID)
	}
	if contacts[1].ID != "participant:shared-id" {
		t.Errorf("participant fallback ID = %q, want namespaced participant ID", contacts[1].ID)
	}
	if contacts[2].ID != "" {
		t.Errorf("missing provider identity mapped to %q, want blank ID for deterministic sync quarantine", contacts[2].ID)
	}
}

func TestContactSyncQuarantinesAdapterContactMissingBothProviderIDs(t *testing.T) {
	client := &contactClientStub{response: &gmproto.ListContactsResponse{Contacts: []*gmproto.Contact{{
		Name:   "Missing Identity Example",
		Number: &gmproto.ContactNumber{Number: "+12025550123"},
	}}}}
	adapter := newTestAdapter(t, "connection-a", client)
	connections := &singleConnectionRepository{connection: adapter.Connection()}
	contacts := &recordingContactRepository{}
	service, err := contactsync.NewService(connections, adapter, contacts)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result, err := service.Sync(context.Background(), "tenant-a", "connection-a")
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if len(result.Contacts) != 0 {
		t.Fatalf("Sync() contacts = %#v, want none", result.Contacts)
	}
	if len(result.Rejected) != 1 || result.Rejected[0].Reason != contactsync.RejectionInvalidProviderContactID {
		t.Fatalf("Sync() rejected = %#v, want invalid provider ID quarantine", result.Rejected)
	}
	if contacts.upsertCalls != 0 {
		t.Fatalf("contact repository upsert calls = %d, want 0", contacts.upsertCalls)
	}
}

func TestMapLinesPreservesDualSIMIdentityAndMetadata(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{
		{
			RCSChats:       &gmproto.RCSChats{Enabled: true},
			SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-alpha"},
			SIMData: &gmproto.SIMData{
				SIMPayload:               &gmproto.SIMPayload{SIMNumber: 1, Two: 2},
				CarrierName:              "Example Wireless A",
				ColorHex:                 "#112233",
				FormattedPhoneNumber:     "+1 (202) 555-0101",
				InternationalPhoneNumber: "+12025550101",
			},
		},
		{
			RCSChats:       &gmproto.RCSChats{Enabled: false},
			SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-beta"},
			SIMData: &gmproto.SIMData{
				SIMPayload:               &gmproto.SIMPayload{SIMNumber: 2, Two: 2},
				CarrierName:              "Example Wireless B",
				ColorHex:                 "#445566",
				FormattedPhoneNumber:     "+1 (202) 555-0102",
				InternationalPhoneNumber: "+12025550102",
			},
		},
	}}

	result := adapter.MapLines(settings)
	if len(result.Rejected) != 0 {
		t.Fatalf("MapLines() rejected = %#v, want none", result.Rejected)
	}
	if len(result.Lines) != 2 {
		t.Fatalf("MapLines() lines = %d, want 2", len(result.Lines))
	}
	first, second := result.Lines[0], result.Lines[1]
	if first.Line.ID == second.Line.ID {
		t.Fatalf("dual SIM line IDs collide: %q", first.Line.ID)
	}
	if first.Line.ProviderParticipantID != "outgoing-alpha" || first.Line.ProviderOutgoingID != "outgoing-alpha" {
		t.Errorf("first line identities = (%q, %q), want outgoing-alpha", first.Line.ProviderParticipantID, first.Line.ProviderOutgoingID)
	}
	if first.Phone.String() != "+12025550101" || first.CarrierName != "Example Wireless A" || first.ColorHex != "#112233" || !first.RCSEnabled {
		t.Errorf("first line metadata = %#v", first)
	}
	if first.ProviderSIMNumber != 1 || first.ProviderSIMPayloadType != 2 {
		t.Errorf("first line SIM payload = (%d, %d), want (1, 2)", first.ProviderSIMNumber, first.ProviderSIMPayloadType)
	}
	if second.Phone.String() != "+12025550102" || second.CarrierName != "Example Wireless B" || second.ColorHex != "#445566" || second.RCSEnabled {
		t.Errorf("second line metadata = %#v", second)
	}
}

func TestMapLinesKeepsConnectionsDistinct(t *testing.T) {
	settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{{
		SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-1"},
		SIMData:        &gmproto.SIMData{InternationalPhoneNumber: "+12025550103"},
	}}}
	first := newTestAdapter(t, "connection-a", &contactClientStub{}).MapLines(settings).Lines[0]
	second := newTestAdapter(t, "connection-b", &contactClientStub{}).MapLines(settings).Lines[0]

	if first.Line.ConnectionID != "connection-a" || second.Line.ConnectionID != "connection-b" {
		t.Fatalf("connection scopes = (%q, %q)", first.Line.ConnectionID, second.Line.ConnectionID)
	}
	if first.Line.ID == second.Line.ID {
		t.Fatalf("same provider SIM on separate connections has colliding line ID %q", first.Line.ID)
	}
}

func TestMapLinesKeepsTenantLineIDsDistinct(t *testing.T) {
	settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{{
		SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-1"},
		SIMData:        &gmproto.SIMData{InternationalPhoneNumber: "+12025550107"},
	}}}
	first := newTestAdapterForTenant(t, "tenant-a", "connection-shared", &contactClientStub{}).MapLines(settings).Lines[0]
	second := newTestAdapterForTenant(t, "tenant-b", "connection-shared", &contactClientStub{}).MapLines(settings).Lines[0]

	if first.Line.ID == second.Line.ID {
		t.Fatalf("same connection/provider identity across tenants has colliding line ID %q", first.Line.ID)
	}
}

func TestMapLinesQuarantinesMissingIdentityAndInvalidCanonicalPhone(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{
		{SIMData: &gmproto.SIMData{InternationalPhoneNumber: "+12025550104"}},
		{SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-2"}, SIMData: &gmproto.SIMData{InternationalPhoneNumber: "202-555-0105", FormattedPhoneNumber: "+12025550105"}},
		{SIMParticipant: &gmproto.SIMParticipant{ID: "outgoing-3"}, SIMData: &gmproto.SIMData{FormattedPhoneNumber: "+1 (202) 555-0106"}},
	}}

	result := adapter.MapLines(settings)
	if len(result.Lines) != 1 || result.Lines[0].Phone.String() != "+12025550106" {
		t.Fatalf("MapLines() valid lines = %#v, want formatted fallback line", result.Lines)
	}
	if len(result.Rejected) != 2 {
		t.Fatalf("MapLines() rejected = %#v, want two records", result.Rejected)
	}
	if result.Rejected[0].Reason != LineRejectionMissingProviderIdentity {
		t.Errorf("missing identity reason = %q", result.Rejected[0].Reason)
	}
	if result.Rejected[1].Reason != LineRejectionInvalidPhoneNumber {
		t.Errorf("invalid canonical phone reason = %q", result.Rejected[1].Reason)
	}
}

func TestMapLinesQuarantinesEveryDuplicateIdentityRegardlessOfOrder(t *testing.T) {
	tests := []struct {
		name   string
		phones []string
	}{
		{name: "invalid then valid", phones: []string{"202-555-0108", "+12025550108"}},
		{name: "valid then invalid", phones: []string{"+12025550109", "202-555-0109"}},
		{name: "two valid", phones: []string{"+12025550110", "+12025550111"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
			settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{
				{SIMParticipant: &gmproto.SIMParticipant{ID: "duplicate-outgoing"}, SIMData: &gmproto.SIMData{InternationalPhoneNumber: test.phones[0]}},
				{SIMParticipant: &gmproto.SIMParticipant{ID: " duplicate-outgoing "}, SIMData: &gmproto.SIMData{InternationalPhoneNumber: test.phones[1]}},
			}}

			result := adapter.MapLines(settings)
			if len(result.Lines) != 0 {
				t.Fatalf("MapLines() routable lines = %#v, want none", result.Lines)
			}
			if len(result.Rejected) != 2 {
				t.Fatalf("MapLines() rejected = %#v, want both duplicate records", result.Rejected)
			}
			for index, rejected := range result.Rejected {
				if rejected.Reason != LineRejectionDuplicateProviderIdentity {
					t.Errorf("rejected[%d] reason = %q, want %q", index, rejected.Reason, LineRejectionDuplicateProviderIdentity)
				}
			}
		})
	}
}

func TestExistingConversationRouteMatchesRequestedLine(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	requested := &domain.Line{
		TenantID: "tenant-a", ConnectionID: "connection-a",
		ProviderOutgoingID: "outgoing-alpha",
	}

	route, err := adapter.RouteExistingConversation(&gmproto.Conversation{DefaultOutgoingID: "outgoing-alpha"}, requested)
	if err != nil {
		t.Fatalf("RouteExistingConversation() error = %v", err)
	}
	if route.ProviderOutgoingID != "outgoing-alpha" || route.UsesPhoneDefault || route.Limitation != RouteLimitationNone {
		t.Fatalf("RouteExistingConversation() = %#v", route)
	}
}

func TestExistingConversationRouteRejectsRequestedLineMismatch(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	requested := &domain.Line{
		TenantID: "tenant-a", ConnectionID: "connection-a",
		ProviderOutgoingID: "outgoing-beta",
	}

	_, err := adapter.RouteExistingConversation(&gmproto.Conversation{DefaultOutgoingID: "outgoing-alpha"}, requested)
	if !errors.Is(err, ErrLineMismatch) {
		t.Fatalf("RouteExistingConversation() error = %v, want ErrLineMismatch", err)
	}
	var routingError *LineRoutingError
	if !errors.As(err, &routingError) {
		t.Fatalf("RouteExistingConversation() error type = %T, want *LineRoutingError", err)
	}
}

func TestExistingConversationRouteRejectsRequestedLineScopeMismatch(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	tests := []struct {
		name         string
		tenantID     domain.TenantID
		connectionID domain.ConnectionID
	}{
		{name: "tenant", tenantID: "tenant-other", connectionID: "connection-a"},
		{name: "connection", tenantID: "tenant-a", connectionID: "connection-other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requested := &domain.Line{
				TenantID: test.tenantID, ConnectionID: test.connectionID,
				ProviderOutgoingID: "requested-secret",
			}

			_, err := adapter.RouteExistingConversation(&gmproto.Conversation{DefaultOutgoingID: "conversation-secret"}, requested)
			assertScopeMismatch(t, err)
		})
	}
}

func TestNewConversationRouteRejectsRequestedLineScopeMismatch(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	tests := []struct {
		name         string
		tenantID     domain.TenantID
		connectionID domain.ConnectionID
	}{
		{name: "tenant", tenantID: "tenant-other", connectionID: "connection-a"},
		{name: "connection", tenantID: "tenant-a", connectionID: "connection-other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requested := &domain.Line{
				TenantID: test.tenantID, ConnectionID: test.connectionID,
				ProviderOutgoingID: "requested-secret",
			}

			_, err := adapter.RouteNewConversation(requested)
			assertScopeMismatch(t, err)
		})
	}
}

func TestNewConversationRejectsExplicitLineAndDocumentsPhoneDefault(t *testing.T) {
	adapter := newTestAdapter(t, "connection-a", &contactClientStub{})
	requested := &domain.Line{
		TenantID: "tenant-a", ConnectionID: "connection-a",
		ProviderOutgoingID: "outgoing-beta",
	}

	if _, err := adapter.RouteNewConversation(requested); !errors.Is(err, ErrLineSelectionUnsupported) {
		t.Fatalf("RouteNewConversation(explicit line) error = %v, want ErrLineSelectionUnsupported", err)
	}
	route, err := adapter.RouteNewConversation(nil)
	if err != nil {
		t.Fatalf("RouteNewConversation(nil) error = %v", err)
	}
	if !route.UsesPhoneDefault || route.ProviderOutgoingID != "" || route.Limitation != RouteLimitationPhoneDefault {
		t.Fatalf("RouteNewConversation(nil) = %#v", route)
	}
}

func newTestAdapter(t *testing.T, connectionID domain.ConnectionID, client ContactClient) *Adapter {
	t.Helper()
	return newTestAdapterForTenant(t, "tenant-a", connectionID, client)
}

func newTestAdapterForTenant(t *testing.T, tenantID domain.TenantID, connectionID domain.ConnectionID, client ContactClient) *Adapter {
	t.Helper()
	adapter, err := New(domain.Connection{
		ID: connectionID, TenantID: tenantID, State: domain.ConnectionStateConnected,
	}, client)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func stringPointer(value string) *string {
	return &value
}

func assertScopeMismatch(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("routing error = %v, want ErrScopeMismatch", err)
	}
	if errors.Is(err, ErrLineMismatch) {
		t.Fatalf("routing error = %v, must not report ErrLineMismatch", err)
	}
	var routingError *LineRoutingError
	if !errors.As(err, &routingError) {
		t.Fatalf("routing error type = %T, want *LineRoutingError", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("scope error exposes provider route details: %q", err)
	}
}
