package messaging_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

type dispatchStore struct {
	claim          messaging.DispatchClaim
	claimOK        bool
	fenceOK        bool
	begun          bool
	released       bool
	completed      []domain.MessageState
	completedClaim messaging.DispatchClaim
	completeErr    error
	renewOK        bool
	renewCalls     int
	renewMu        sync.Mutex
}

func (store *dispatchStore) ClaimNext(context.Context, messaging.LaneKey, string) (messaging.DispatchClaim, bool, error) {
	return store.claim, store.claimOK, nil
}

func (store *dispatchStore) BeginProviderIO(context.Context, messaging.DispatchClaim, string) (bool, error) {
	store.begun = true
	return store.fenceOK, nil
}

func (store *dispatchStore) RenewProviderIO(_ context.Context, _ messaging.DispatchClaim, _ string) (bool, error) {
	store.renewMu.Lock()
	defer store.renewMu.Unlock()
	store.renewCalls++
	return store.renewOK, nil
}

func (store *dispatchStore) CompleteDispatch(_ context.Context, claim messaging.DispatchClaim, states []domain.MessageState, _ string) error {
	store.completedClaim = claim
	store.completed = append([]domain.MessageState(nil), states...)
	return store.completeErr
}

func (store *dispatchStore) ReleaseBeforeDispatch(context.Context, messaging.DispatchClaim, string) error {
	store.released = true
	return nil
}

type onceSender struct {
	calls  int
	result messaging.ProviderSendResult
	err    error
}

func (sender *onceSender) SendOnce(context.Context, messaging.ProviderSendCommand) (messaging.ProviderSendResult, error) {
	sender.calls++
	return sender.result, sender.err
}

func TestDispatcherRecordsRelayAcceptanceWithoutClaimingSent(t *testing.T) {
	store := &dispatchStore{
		claimOK: true, fenceOK: true, renewOK: true,
		claim: messaging.DispatchClaim{Message: messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"}, FencingToken: 9},
	}
	sender := &onceSender{result: messaging.ProviderSendResult{Accepted: true}}
	dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{Store: store, Sender: sender, OwnerID: "owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	didWork, err := dispatcher.DispatchLane(context.Background(), messaging.LaneKey{TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"})
	if err != nil || !didWork {
		t.Fatalf("DispatchLane() = (%v, %v)", didWork, err)
	}
	want := []domain.MessageState{domain.MessageStateProviderAccepted, domain.MessageStateAwaitingPhone}
	if sender.calls != 1 || !store.begun || len(store.completed) != len(want) {
		t.Fatalf("calls=%d begun=%v states=%v", sender.calls, store.begun, store.completed)
	}
	for index := range want {
		if store.completed[index] != want[index] {
			t.Fatalf("states=%v, want %v", store.completed, want)
		}
	}
}

func TestDispatcherPersistsProviderCreatedConversationWithAcceptance(t *testing.T) {
	store := &dispatchStore{
		claimOK: true, fenceOK: true, renewOK: true,
		claim: messaging.DispatchClaim{Message: messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", Recipient: "+12025550123"}, FencingToken: 9},
	}
	sender := &onceSender{result: messaging.ProviderSendResult{Accepted: true, ConversationID: "provider-conversation"}}
	dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{Store: store, Sender: sender, OwnerID: "owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dispatcher.DispatchLane(context.Background(), messaging.LaneKey{TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "new:+12025550123"}); err != nil {
		t.Fatal(err)
	}
	if store.completedClaim.Message.ConversationID != "provider-conversation" {
		t.Fatalf("completed conversation = %q", store.completedClaim.Message.ConversationID)
	}
}

func TestDispatcherMarksEveryPostBeginAmbiguityUncertainAndNeverRetries(t *testing.T) {
	for _, sendErr := range []error{context.DeadlineExceeded, context.Canceled, errors.New("connection reset")} {
		t.Run(sendErr.Error(), func(t *testing.T) {
			store := &dispatchStore{
				claimOK: true, fenceOK: true, renewOK: true,
				claim: messaging.DispatchClaim{Message: messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"}, FencingToken: 9},
			}
			sender := &onceSender{err: sendErr}
			dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{Store: store, Sender: sender, OwnerID: "owner-a"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = dispatcher.DispatchLane(context.Background(), messaging.LaneKey{TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"}); err != nil {
				t.Fatalf("DispatchLane() error = %v", err)
			}
			if sender.calls != 1 || len(store.completed) != 1 || store.completed[0] != domain.MessageStateUncertain {
				t.Fatalf("calls=%d states=%v", sender.calls, store.completed)
			}
		})
	}
}

type cancellationSender struct{ calls int }

func (sender *cancellationSender) SendOnce(ctx context.Context, _ messaging.ProviderSendCommand) (messaging.ProviderSendResult, error) {
	sender.calls++
	<-ctx.Done()
	return messaging.ProviderSendResult{}, ctx.Err()
}

func TestDispatcherCancelsAmbiguousProviderIOWhenLaneRenewalIsLost(t *testing.T) {
	store := &dispatchStore{
		claimOK: true, fenceOK: true, renewOK: false,
		claim: messaging.DispatchClaim{
			Message:   messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"},
			AttemptID: "attempt-a", LaneToken: 4, FencingToken: 9,
		},
	}
	sender := &cancellationSender{}
	dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{
		Store: store, Sender: sender, OwnerID: "owner-a", RenewInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	worked, err := dispatcher.DispatchLane(context.Background(), messaging.LaneKey{
		TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
	})
	if err != nil || !worked {
		t.Fatalf("DispatchLane() = (%v, %v)", worked, err)
	}
	store.renewMu.Lock()
	renewCalls := store.renewCalls
	store.renewMu.Unlock()
	if sender.calls != 1 || renewCalls != 1 || len(store.completed) != 1 || store.completed[0] != domain.MessageStateUncertain {
		t.Fatalf("sender calls=%d renewals=%d completed=%v", sender.calls, renewCalls, store.completed)
	}
}

func TestDispatcherDoesNotCallProviderAfterFenceLoss(t *testing.T) {
	store := &dispatchStore{
		claimOK: true, fenceOK: false,
		claim: messaging.DispatchClaim{Message: messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"}, FencingToken: 9},
	}
	sender := &onceSender{}
	dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{Store: store, Sender: sender, OwnerID: "owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dispatcher.DispatchLane(context.Background(), messaging.LaneKey{TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"}); !errors.Is(err, messaging.ErrDispatchFenceLost) {
		t.Fatalf("DispatchLane() error = %v, want ErrDispatchFenceLost", err)
	}
	if sender.calls != 0 || !store.released || len(store.completed) != 0 {
		t.Fatalf("provider calls=%d released=%v states=%v", sender.calls, store.released, store.completed)
	}
}
