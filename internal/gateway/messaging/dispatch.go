package messaging

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var ErrDispatchFenceLost = errors.New("dispatch fence lost before provider I/O")

type LaneKey struct {
	TenantID       domain.TenantID
	ConnectionID   domain.ConnectionID
	ConversationID string
}

type DispatchClaim struct {
	Message      OutboundMessage
	AttemptID    string
	OrderingKey  string
	OwnerID      string
	LaneToken    uint64
	FencingToken uint64
}

type DispatchStore interface {
	// ClaimNext serializes one in-flight message in the ordering lane and
	// atomically appends dispatching to immutable status history.
	ClaimNext(context.Context, LaneKey, string) (DispatchClaim, bool, error)
	// BeginProviderIO rechecks both lane ownership and the current connection
	// lease/fencing token immediately before the one permitted send attempt.
	BeginProviderIO(context.Context, DispatchClaim, string) (bool, error)
	// RenewProviderIO keeps the ordering lane exclusive while bounded provider
	// I/O is in progress and rechecks the same attempt and connection fence.
	RenewProviderIO(context.Context, DispatchClaim, string) (bool, error)
	// CompleteDispatch atomically appends statuses, derives current state, closes
	// the attempt, and emits immutable outbox events.
	CompleteDispatch(context.Context, DispatchClaim, []domain.MessageState, string) error
	ReleaseBeforeDispatch(context.Context, DispatchClaim, string) error
}

type ProviderSendResult struct {
	Accepted       bool
	FailureReason  string
	ConversationID string
}

type ProviderSendCommand struct {
	Message      OutboundMessage
	FencingToken uint64
}

type ProviderSender interface {
	// SendOnce must not transparently retry a mutating provider request.
	SendOnce(context.Context, ProviderSendCommand) (ProviderSendResult, error)
}

type DispatchConfig struct {
	Store         DispatchStore
	Sender        ProviderSender
	OwnerID       string
	RenewInterval time.Duration
}

type Dispatcher struct {
	store   DispatchStore
	sender  ProviderSender
	ownerID string
	renew   time.Duration
}

func NewDispatcher(config DispatchConfig) (*Dispatcher, error) {
	if config.Store == nil || config.Sender == nil || config.OwnerID == "" || len(config.OwnerID) > 256 {
		return nil, ErrInvalidCommand
	}
	if config.RenewInterval == 0 {
		config.RenewInterval = 10 * time.Second
	}
	if config.RenewInterval < time.Millisecond || config.RenewInterval > 15*time.Second {
		return nil, ErrInvalidCommand
	}
	return &Dispatcher{store: config.Store, sender: config.Sender, ownerID: config.OwnerID, renew: config.RenewInterval}, nil
}

func (dispatcher *Dispatcher) DispatchLane(ctx context.Context, lane LaneKey) (bool, error) {
	if lane.TenantID == "" || lane.ConnectionID == "" || lane.ConversationID == "" {
		return false, ErrInvalidRoute
	}
	claim, ok, err := dispatcher.store.ClaimNext(ctx, lane, dispatcher.ownerID)
	if err != nil || !ok {
		return false, err
	}
	owned, err := dispatcher.store.BeginProviderIO(ctx, claim, dispatcher.ownerID)
	if err != nil || !owned {
		releaseErr := dispatcher.releaseDetached(claim, "fence_lost")
		if err != nil {
			return true, err
		}
		if releaseErr != nil {
			return true, releaseErr
		}
		return true, ErrDispatchFenceLost
	}

	sendCtx, cancelSend := context.WithCancel(ctx)
	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewResult := make(chan error, 1)
	go dispatcher.renewProviderIO(renewCtx, cancelSend, claim, renewResult)
	result, sendErr := dispatcher.sender.SendOnce(sendCtx, ProviderSendCommand{Message: claim.Message, FencingToken: claim.FencingToken})
	stopRenew()
	cancelSend()
	renewErr := <-renewResult
	if renewErr != nil {
		return true, dispatcher.completeDetached(claim, []domain.MessageState{domain.MessageStateUncertain}, "dispatch_fence_lost")
	}
	if sendErr != nil {
		return true, dispatcher.completeDetached(claim, []domain.MessageState{domain.MessageStateUncertain}, "provider_io_ambiguous")
	}
	if result.ConversationID != "" {
		claim.Message.ConversationID = result.ConversationID
	}
	if result.Accepted {
		return true, dispatcher.completeDetached(claim, []domain.MessageState{
			domain.MessageStateProviderAccepted,
			domain.MessageStateAwaitingPhone,
		}, "")
	}
	if result.FailureReason != "" {
		return true, dispatcher.completeDetached(claim, []domain.MessageState{domain.MessageStateFailed}, result.FailureReason)
	}
	return true, dispatcher.completeDetached(claim, []domain.MessageState{domain.MessageStateUncertain}, "provider_response_ambiguous")
}

func (dispatcher *Dispatcher) renewProviderIO(ctx context.Context, cancelSend context.CancelFunc, claim DispatchClaim, result chan<- error) {
	ticker := time.NewTicker(dispatcher.renew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			result <- nil
			return
		case <-ticker.C:
			owned, err := dispatcher.store.RenewProviderIO(ctx, claim, dispatcher.ownerID)
			if err != nil || !owned {
				cancelSend()
				if err == nil {
					err = ErrDispatchFenceLost
				}
				result <- err
				return
			}
		}
	}
}

func (dispatcher *Dispatcher) completeDetached(claim DispatchClaim, states []domain.MessageState, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return dispatcher.store.CompleteDispatch(ctx, claim, states, reason)
}

func (dispatcher *Dispatcher) releaseDetached(claim DispatchClaim, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return dispatcher.store.ReleaseBeforeDispatch(ctx, claim, reason)
}
