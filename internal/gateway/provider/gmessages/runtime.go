package gmessages

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
)

type runtimeClient interface {
	ConnectContext(context.Context) error
	DisconnectContext(context.Context) error
	WaitContext(context.Context) error
	SetLifecycleHooks(libgm.LifecycleHooks)
	SetEventHandler(libgm.EventHandler)
	SnapshotSession() (*libgm.AuthData, *libgm.PushKeys)
	ClearSessionSecrets()
}

type RuntimeFactory struct {
	logger      zerolog.Logger
	newClient   func(*libgm.AuthData, *libgm.PushKeys, zerolog.Logger) runtimeClient
	durable     DurableMessageSink
	mediaPolicy libgm.MediaRequestPolicy
}

func (factory *RuntimeFactory) WithMediaRequestPolicy(policy libgm.MediaRequestPolicy) *RuntimeFactory {
	factory.mediaPolicy = policy
	return factory
}

type DurableMessageSink interface {
	PersistEnvelopeOutcome(context.Context, connectionactor.ProviderOwnership, libgm.DurableEnvelope) (libgm.DurableOutcome, error)
	MarkACKed(context.Context, connectionactor.ProviderOwnership, []string) error
	PendingACKs(context.Context, connectionactor.ProviderOwnership, int) ([]string, error)
	CoordinateACKs(context.Context, connectionactor.ProviderOwnership, []string, libgm.ACKBatchSender) (libgm.ACKCoordinationResult, error)
	ACKTimeout() time.Duration
}

type durableCapableClient interface {
	SetDurableEnvelopeHandler(libgm.DurableEnvelopeHandler)
	SetDurableFailureObserver(libgm.DurableFailureObserver)
	SetACKCoordinator(libgm.ACKCoordinator)
	QueueDurableACKs([]string) error
}

func NewRuntimeFactory(logger zerolog.Logger) *RuntimeFactory {
	return &RuntimeFactory{
		logger: logger,
		newClient: func(auth *libgm.AuthData, push *libgm.PushKeys, logger zerolog.Logger) runtimeClient {
			return libgm.NewClient(auth, push, logger)
		},
	}
}

func (factory *RuntimeFactory) WithDurableMessaging(sink DurableMessageSink) *RuntimeFactory {
	factory.durable = sink
	return factory
}

func (factory *RuntimeFactory) Restore(ctx context.Context, plaintext []byte, hooks connectionactor.Hooks) (connectionactor.Provider, error) {
	auth, push, err := DecodeSession(plaintext)
	if err != nil {
		return nil, connectionactor.ErrProviderPermanentConfig
	}
	client := factory.newClient(auth, push, factory.logger)
	var ownership connectionactor.ProviderOwnership
	var durableClient durableCapableClient
	if factory.durable != nil {
		var ok bool
		ownership, ok = connectionactor.ProviderOwnershipFromContext(ctx)
		if !ok {
			client.ClearSessionSecrets()
			return nil, connectionactor.ErrProviderPermanentConfig
		}
	}
	if factory.mediaPolicy != nil {
		policyClient, ok := client.(interface {
			SetMediaRequestPolicy(libgm.MediaRequestPolicy)
		})
		if !ok {
			client.ClearSessionSecrets()
			return nil, connectionactor.ErrProviderPermanentConfig
		}
		policyClient.SetMediaRequestPolicy(factory.mediaPolicy)
	}
	if factory.durable != nil {
		var supportsDurable bool
		durableClient, supportsDurable = client.(durableCapableClient)
		if !supportsDurable {
			client.ClearSessionSecrets()
			return nil, connectionactor.ErrProviderPermanentConfig
		}
		if ownership.LeaseTTL <= postgres.ProviderACKCoordinationHardTimeout || factory.durable.ACKTimeout()*3 >= ownership.LeaseTTL {
			client.ClearSessionSecrets()
			return nil, connectionactor.ErrProviderPermanentConfig
		}
		durableClient.SetDurableEnvelopeHandler(func(handlerCtx context.Context, envelope libgm.DurableEnvelope) (libgm.DurableOutcome, error) {
			return factory.durable.PersistEnvelopeOutcome(handlerCtx, ownership, envelope)
		})
	}
	runtime := &runtimeProvider{
		client: client, hooks: hooks, failureNotify: make(chan struct{}, 1),
	}
	if factory.durable != nil {
		durableClient.SetDurableFailureObserver(runtime.signalFailure)
		durableClient.SetACKCoordinator(func(handlerCtx context.Context, ids []string, send libgm.ACKBatchSender) (libgm.ACKCoordinationResult, error) {
			result, err := factory.durable.CoordinateACKs(handlerCtx, ownership, ids, send)
			if err != nil || result.ProviderError != nil {
				return result, err
			}
			if refillErr := runtime.queuePendingACKs(handlerCtx, factory.durable, durableClient, ownership); refillErr != nil {
				if len(result.AdmittedIDs) > 0 {
					result.RetryIDs = append([]string(nil), result.AdmittedIDs...)
				}
				return result, refillErr
			}
			return result, nil
		})
	}
	client.SetLifecycleHooks(libgm.LifecycleHooks{
		OnReady: func() {
			if factory.durable != nil {
				recoveryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := runtime.queuePendingACKs(recoveryCtx, factory.durable, durableClient, ownership)
				cancel()
				if err != nil {
					runtime.signalFailure(err)
					return
				}
			}
			hooks.Ready()
		},
		OnFrame:         hooks.Frame,
		OnPhoneResponse: hooks.PhoneResponse,
		OnSessionChange: runtime.sessionChanged,
	})
	client.SetEventHandler(runtime.handleEvent)
	return runtime, nil
}

func (provider *runtimeProvider) queuePendingACKs(ctx context.Context, sink DurableMessageSink, client durableCapableClient, ownership connectionactor.ProviderOwnership) error {
	ids, err := sink.PendingACKs(ctx, ownership, 256)
	if err != nil {
		return err
	}
	return client.QueueDurableACKs(ids)
}

func (provider *runtimeProvider) signalFailure(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrACKAdmissionLimited) ||
		(errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrDurableInfrastructure)) {
		return
	}
	switch {
	case errors.Is(err, ErrDurableFenceLost):
		err = errors.Join(connectionactor.ErrStaleGeneration, err)
	case errors.Is(err, ErrDurableInfrastructure):
		err = errors.Join(connectionactor.ErrSharedInfrastructure, err)
	case errors.Is(err, libgm.ErrDurablePoisoned), errors.Is(err, ingress.ErrConflictingEnvelope),
		errors.Is(err, ingress.ErrInvalidProviderResponseID), errors.Is(err, ingress.ErrProviderResponseCapacity):
		err = errors.Join(connectionactor.ErrProviderPermanentProtocol, err)
	case errors.Is(err, libgm.ErrDurablePersistence):
		err = errors.Join(connectionactor.ErrProviderPermanentProtocol, err)
	}
	provider.recordFailure(err)
}

func (provider *runtimeProvider) recordFailure(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	provider.failureMu.Lock()
	if provider.failureNotify == nil {
		provider.failureNotify = make(chan struct{}, 1)
	}
	if provider.failure == nil || terminalFailurePriority(err) > terminalFailurePriority(provider.failure) {
		provider.failure = err
	}
	notify := provider.failureNotify
	provider.failureMu.Unlock()
	select {
	case notify <- struct{}{}:
	default:
	}
}

func terminalFailurePriority(err error) int {
	switch {
	case errors.Is(err, connectionactor.ErrSharedInfrastructure):
		return 500
	case errors.Is(err, libgm.ErrDurablePoisoned), errors.Is(err, ingress.ErrConflictingEnvelope),
		errors.Is(err, ingress.ErrInvalidProviderResponseID), errors.Is(err, ingress.ErrProviderResponseCapacity):
		return 450
	case errors.Is(err, connectionactor.ErrProviderPermanentProtocol):
		return 400
	case errors.Is(err, connectionactor.ErrStaleGeneration):
		return 350
	case errors.Is(err, connectionactor.ErrProviderAuthorization):
		return 300
	case errors.Is(err, connectionactor.ErrProviderPermanentConfig):
		return 250
	case errors.Is(err, connectionactor.ErrProviderTransient):
		return 100
	default:
		return 200
	}
}

func (provider *runtimeProvider) failureSignal() <-chan struct{} {
	provider.failureMu.Lock()
	defer provider.failureMu.Unlock()
	if provider.failureNotify == nil {
		provider.failureNotify = make(chan struct{}, 1)
	}
	return provider.failureNotify
}

func (provider *runtimeProvider) terminalFailure() error {
	provider.failureMu.Lock()
	defer provider.failureMu.Unlock()
	return provider.failure
}

func (provider *runtimeProvider) terminalFailureOr(fallback error) error {
	if failure := provider.terminalFailure(); failure != nil {
		return failure
	}
	return fallback
}

type runtimeProvider struct {
	client        runtimeClient
	hooks         connectionactor.Hooks
	failureMu     sync.Mutex
	failure       error
	failureNotify chan struct{}
	clear         sync.Once
}

func (provider *runtimeProvider) gatewayMessagingClient() gatewayMessagingClient {
	client, _ := provider.client.(gatewayMessagingClient)
	return client
}

func (provider *runtimeProvider) gatewayBackfillClient() gatewayBackfillClient {
	client, _ := provider.client.(gatewayBackfillClient)
	return client
}

func (provider *runtimeProvider) gatewayContactClient() ContactClient {
	client, _ := provider.client.(ContactClient)
	return client
}

func (provider *runtimeProvider) Connect(ctx context.Context) error {
	if err := provider.client.ConnectContext(ctx); err != nil {
		if failure := provider.terminalFailure(); failure != nil {
			return failure
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var httpError events.HTTPError
		if errors.As(err, &httpError) && (httpError.Classification == "authorization" || httpError.StatusCode == http.StatusUnauthorized || httpError.StatusCode == http.StatusForbidden) {
			return connectionactor.ErrProviderAuthorization
		}
		return connectionactor.ErrProviderTransient
	}
	wait := make(chan error, 1)
	go func() { wait <- provider.client.WaitContext(ctx) }()
	failureSignal := provider.failureSignal()
	select {
	case <-failureSignal:
		_ = provider.client.DisconnectContext(context.Background())
		<-wait
		if failure := provider.terminalFailure(); failure != nil {
			return failure
		}
		return connectionactor.ErrProviderPermanentProtocol
	case err := <-wait:
		if failure := provider.terminalFailure(); failure != nil {
			return failure
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return context.Canceled
		}
		return connectionactor.ErrProviderPermanentProtocol
	case <-ctx.Done():
		_ = provider.client.DisconnectContext(context.Background())
		<-wait
		return provider.terminalFailureOr(ctx.Err())
	}
}

func (provider *runtimeProvider) Disconnect(ctx context.Context) error {
	err := provider.client.DisconnectContext(ctx)
	provider.clear.Do(provider.client.ClearSessionSecrets)
	return err
}

func (provider *runtimeProvider) handleEvent(event any) {
	var failure error
	switch typed := event.(type) {
	case *events.GaiaLoggedOut:
		failure = connectionactor.ErrProviderAuthorization
	case *events.ListenFatalError:
		var httpError events.HTTPError
		if errors.As(typed.Error, &httpError) && httpError.Classification == "authorization" {
			failure = connectionactor.ErrProviderAuthorization
		} else {
			failure = connectionactor.ErrProviderPermanentProtocol
		}
	case *libgm.AuthenticatedSettings:
		// Authenticated Settings are projected by DurableSink and committed with
		// the raw provider envelope before ACK eligibility. The compatibility
		// event intentionally has no post-commit database side effect.
		return
	default:
		return
	}
	provider.recordFailure(failure)
}

func (provider *runtimeProvider) sessionChanged() {
	auth, push := provider.client.SnapshotSession()
	if auth == nil {
		return
	}
	defer auth.ClearSecrets()
	plaintext, err := EncodeSession(auth, push)
	if push != nil {
		clearPushKeys(push)
	}
	if err != nil {
		return
	}
	provider.hooks.SessionChanged(plaintext)
	zero(plaintext)
}

var _ connectionactor.ProviderFactory = (*RuntimeFactory)(nil)
var _ connectionactor.Provider = (*runtimeProvider)(nil)
var _ gatewayBackfillProvider = (*runtimeProvider)(nil)
