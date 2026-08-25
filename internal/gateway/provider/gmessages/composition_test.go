package gmessages

import (
	"context"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type messagingRuntimeStoreFake struct{ routeRecorder }

func (*messagingRuntimeStoreFake) GetLine(context.Context, domain.TenantID, domain.ConnectionID, domain.LineID) (domain.Line, error) {
	return domain.Line{}, nil
}
func (*messagingRuntimeStoreFake) ClaimNext(context.Context, messaging.LaneKey, string) (messaging.DispatchClaim, bool, error) {
	return messaging.DispatchClaim{}, false, nil
}
func (*messagingRuntimeStoreFake) BeginProviderIO(context.Context, messaging.DispatchClaim, string) (bool, error) {
	return false, nil
}
func (*messagingRuntimeStoreFake) RenewProviderIO(context.Context, messaging.DispatchClaim, string) (bool, error) {
	return false, nil
}
func (*messagingRuntimeStoreFake) CompleteDispatch(context.Context, messaging.DispatchClaim, []domain.MessageState, string) error {
	return nil
}
func (*messagingRuntimeStoreFake) ReleaseBeforeDispatch(context.Context, messaging.DispatchClaim, string) error {
	return nil
}
func (*messagingRuntimeStoreFake) LoadCommittedCursor(context.Context, domain.TenantID, domain.ConnectionID, string) ([]byte, error) {
	return nil, nil
}
func (*messagingRuntimeStoreFake) LoadBackfillCheckpoint(context.Context, domain.TenantID, domain.ConnectionID) (*messaging.BackfillCheckpoint, error) {
	return nil, nil
}
func (*messagingRuntimeStoreFake) StageBackfillPageFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, messaging.BackfillPage) error {
	return nil
}
func (*messagingRuntimeStoreFake) MarkBackfillItemFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, string, int, messaging.BackfillItemState, string) error {
	return nil
}
func (*messagingRuntimeStoreFake) CompleteBackfillPageFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, string) error {
	return nil
}

func TestNewMessagingServicesUsesOneActorExecutorForSendAndMedia(t *testing.T) {
	sequence := []string{}
	store := &messagingRuntimeStoreFake{routeRecorder: routeRecorder{sequence: &sequence}}
	services, err := NewMessagingServices(MessagingServicesConfig{
		Executor: inlineExecutor{provider: &messagingProviderFake{client: &messagingClientFake{}}},
		Store:    store, Media: mediaSourceFake{}, Keys: keyOpenerFake{key: make([]byte, 32)},
		OwnerID: "owner-a", MaxMediaBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if services.Sender == nil || services.Dispatcher == nil || services.MediaFetcher == nil || services.Backfill == nil {
		t.Fatalf("composition = %+v", services)
	}
	if services.Sender.executor != services.MediaFetcher.executor || services.Sender.executor != services.Backfill.executor {
		t.Fatal("send, media fetch, and backfill did not share the actor-owned provider executor")
	}
}

type contactRuntimeClient struct{}

func (contactRuntimeClient) ListContacts(context.Context) (*gmproto.ListContactsResponse, error) {
	return &gmproto.ListContactsResponse{Contacts: []*gmproto.Contact{{ContactID: "provider-a", Name: "Ada", Number: &gmproto.ContactNumber{Number: "+12025550123"}}}}, nil
}

type contactRuntimeProvider struct{ client ContactClient }

func (provider contactRuntimeProvider) gatewayContactClient() ContactClient { return provider.client }
func (contactRuntimeProvider) Connect(context.Context) error                { return nil }
func (contactRuntimeProvider) Disconnect(context.Context) error             { return nil }

func TestActorContactProviderUsesFencedConnectionExecutor(t *testing.T) {
	connection := domain.Connection{ID: "connection-a", TenantID: "tenant-a", State: domain.ConnectionStateConnected}
	provider, err := NewActorContactProvider(inlineExecutor{provider: contactRuntimeProvider{client: contactRuntimeClient{}}})
	if err != nil {
		t.Fatal(err)
	}
	contacts, err := provider.ListContacts(context.Background(), connection)
	if err != nil || len(contacts) != 1 || contacts[0].ID != "contact:provider-a" || contacts[0].PhoneNumber != "+12025550123" {
		t.Fatalf("ListContacts() = (%+v, %v)", contacts, err)
	}
}
