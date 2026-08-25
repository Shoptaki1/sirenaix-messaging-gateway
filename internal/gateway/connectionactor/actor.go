package connectionactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

type Lease = postgres.ConnectionLease
type Health = postgres.ConnectionActorHealth

const (
	ReasonNone                 = "none"
	ReasonTransientNetwork     = "transient-network"
	ReasonProviderAuth         = "provider-auth"
	ReasonProviderConfig       = "provider-config"
	ReasonProviderProtocol     = "provider-protocol"
	ReasonSharedInfrastructure = "shared-infrastructure"
	ReasonLeaseLost            = "lease-lost"
	ReasonSessionConflict      = "session-conflict"
	ReasonShutdown             = "shutdown"
	defaultLeaseTTL            = 30 * time.Second
	defaultSessionDebounce     = 500 * time.Millisecond
	defaultJoinTimeout         = 5 * time.Second
	providerEventBufferSize    = 64
	defaultOperationQueueSize  = 64
)

var (
	ErrLeaseUnavailable          = errors.New("connection lease is owned by another replica")
	ErrProviderAuthorization     = errors.New("provider authorization failed")
	ErrProviderTransient         = errors.New("transient provider failure")
	ErrProviderPermanentConfig   = errors.New("permanent provider configuration failure")
	ErrProviderPermanentProtocol = errors.New("permanent provider protocol failure")
	ErrProviderUnavailable       = errors.New("provider generation is not ready")
	ErrProviderJoinTimeout       = errors.New("provider generation did not stop before join deadline")
	ErrSharedInfrastructure      = errors.New("shared gateway infrastructure failure")
	ErrStaleGeneration           = errors.New("provider generation lost its durable connection fence")
)

type Store interface {
	GetConnection(context.Context, domain.TenantID, domain.ConnectionID) (domain.Connection, error)
	AcquireConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, time.Duration) (Lease, bool, error)
	RenewConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, time.Duration) (bool, error)
	ReleaseConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, uint64) (bool, error)
	WriteConnectionHealthFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, Health) (bool, error)
	MarkReauthorizationRequiredFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64) (bool, error)
}

type SessionSnapshot struct {
	Plaintext []byte
	Revision  uint64
}

type SessionStore interface {
	LoadVersioned(context.Context, Key) (SessionSnapshot, error)
	CompareAndSwapFenced(context.Context, Key, string, uint64, uint64, []byte) (bool, error)
}

type Provider interface {
	// Connect blocks for the lifetime of one provider connection generation.
	Connect(context.Context) error
	Disconnect(context.Context) error
}

type ProviderFactory interface {
	Restore(context.Context, []byte, Hooks) (Provider, error)
}

type ProviderOperation func(context.Context, Provider) error

type ProviderExecutor interface {
	// Execute applies bounded backpressure and runs operation only inside the
	// ready provider generation that owns the current database lease.
	Execute(context.Context, Key, ProviderOperation) error
}

type ProviderOwnership struct {
	Key          Key
	OwnerID      string
	FencingToken uint64
	LeaseTTL     time.Duration
}

type providerOwnershipContextKey struct{}

func ContextWithProviderOwnership(ctx context.Context, ownership ProviderOwnership) context.Context {
	return context.WithValue(ctx, providerOwnershipContextKey{}, ownership)
}

func ProviderOwnershipFromContext(ctx context.Context) (ProviderOwnership, bool) {
	ownership, ok := ctx.Value(providerOwnershipContextKey{}).(ProviderOwnership)
	if !ok || !ownership.Key.valid() || ownership.OwnerID == "" || ownership.FencingToken == 0 {
		return ProviderOwnership{}, false
	}
	return ownership, true
}

type MetricsSink interface {
	ActorState(string)
	LeaseAcquired()
	LeaseLost()
	Reconnect(string)
	Backoff(string, time.Duration)
}

type noopMetrics struct{}

func (noopMetrics) ActorState(string)             {}
func (noopMetrics) LeaseAcquired()                {}
func (noopMetrics) LeaseLost()                    {}
func (noopMetrics) Reconnect(string)              {}
func (noopMetrics) Backoff(string, time.Duration) {}

type ActorConfig struct {
	OwnerID   string
	Store     Store
	Sessions  SessionStore
	Providers ProviderFactory
	Metrics   MetricsSink

	LeaseTTL            time.Duration
	SessionDebounce     time.Duration
	Backoff             BackoffConfig
	Now                 func() time.Time
	NewRenewTimer       TimerFactory
	NewBackoffTimer     TimerFactory
	NewDebounceTimer    TimerFactory
	OperationQueueSize  int
	JoinTimeout         time.Duration
	beforeDebounceFlush func()
}

type Actor struct {
	ownerID             string
	store               Store
	sessions            SessionStore
	providers           ProviderFactory
	metrics             MetricsSink
	leaseTTL            time.Duration
	sessionDebounce     time.Duration
	backoffConfig       BackoffConfig
	now                 func() time.Time
	newRenewTimer       TimerFactory
	newBackoffTimer     TimerFactory
	newDebounceTimer    TimerFactory
	beforeDebounceFlush func()
	operations          chan providerOperationRequest
	operationMu         sync.RWMutex
	operationKey        Key
	operationRunID      uint64
	operationActive     bool
	operationReady      bool
	operationDone       chan struct{}
	joinTimeout         time.Duration
}

func NewActor(config ActorConfig) (*Actor, error) {
	if config.OwnerID == "" || len(config.OwnerID) > 256 || config.Store == nil || config.Sessions == nil || config.Providers == nil {
		return nil, ErrInvalidConfig
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.LeaseTTL < 3*time.Second || config.LeaseTTL > 10*time.Minute {
		return nil, ErrInvalidConfig
	}
	if config.SessionDebounce == 0 {
		config.SessionDebounce = defaultSessionDebounce
	}
	if config.SessionDebounce < 0 || config.SessionDebounce > time.Minute {
		return nil, ErrInvalidConfig
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewRenewTimer == nil {
		config.NewRenewTimer = NewTimer
	}
	if config.NewBackoffTimer == nil {
		config.NewBackoffTimer = NewTimer
	}
	if config.NewDebounceTimer == nil {
		config.NewDebounceTimer = NewTimer
	}
	if config.Metrics == nil {
		config.Metrics = noopMetrics{}
	}
	if config.OperationQueueSize == 0 {
		config.OperationQueueSize = defaultOperationQueueSize
	}
	if config.OperationQueueSize < 1 || config.OperationQueueSize > 1024 {
		return nil, ErrInvalidConfig
	}
	if config.JoinTimeout == 0 {
		config.JoinTimeout = defaultJoinTimeout
	}
	if config.JoinTimeout < 10*time.Millisecond || config.JoinTimeout > 30*time.Second {
		return nil, ErrInvalidConfig
	}
	if _, err := NewBackoff(config.Backoff); err != nil {
		return nil, err
	}
	return &Actor{
		ownerID: config.OwnerID, store: config.Store, sessions: config.Sessions, providers: config.Providers,
		metrics: config.Metrics, leaseTTL: config.LeaseTTL, sessionDebounce: config.SessionDebounce,
		backoffConfig: config.Backoff, now: config.Now, newRenewTimer: config.NewRenewTimer,
		newBackoffTimer: config.NewBackoffTimer, newDebounceTimer: config.NewDebounceTimer,
		beforeDebounceFlush: config.beforeDebounceFlush,
		operations:          make(chan providerOperationRequest, config.OperationQueueSize),
		joinTimeout:         config.JoinTimeout,
	}, nil
}

type providerOperationRequest struct {
	runID   uint64
	key     Key
	ctx     context.Context
	execute ProviderOperation
	result  chan error
}

type activeProviderOperation struct {
	request providerOperationRequest
	cancel  context.CancelFunc
	stop    func() bool
	done    <-chan error
}

func (actor *Actor) Execute(ctx context.Context, key Key, operation ProviderOperation) error {
	if ctx == nil || ctx.Err() != nil || !key.valid() || operation == nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrInvalidConfig
	}
	actor.operationMu.RLock()
	if !actor.operationActive || !actor.operationReady || actor.operationKey != key {
		actor.operationMu.RUnlock()
		return ErrProviderUnavailable
	}
	runID, stopped := actor.operationRunID, actor.operationDone
	actor.operationMu.RUnlock()
	request := providerOperationRequest{runID: runID, key: key, ctx: ctx, execute: operation, result: make(chan error, 1)}
	select {
	case actor.operations <- request:
	case <-stopped:
		return ErrProviderUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-stopped:
		return ErrProviderUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (actor *Actor) beginOperationRun(key Key) (uint64, error) {
	actor.operationMu.Lock()
	defer actor.operationMu.Unlock()
	if actor.operationActive {
		return 0, ErrInvalidConfig
	}
	for {
		select {
		case stale := <-actor.operations:
			stale.result <- ErrProviderUnavailable
		default:
			actor.operationRunID++
			actor.operationKey = key
			actor.operationActive = true
			actor.operationReady = false
			actor.operationDone = make(chan struct{})
			return actor.operationRunID, nil
		}
	}
}

func (actor *Actor) setOperationReady(runID uint64, ready bool) {
	actor.operationMu.Lock()
	if actor.operationActive && actor.operationRunID == runID {
		actor.operationReady = ready
	}
	actor.operationMu.Unlock()
}

func (actor *Actor) endOperationRun(runID uint64) {
	actor.operationMu.Lock()
	if actor.operationActive && actor.operationRunID == runID {
		actor.operationActive = false
		actor.operationReady = false
		close(actor.operationDone)
	}
	actor.operationMu.Unlock()
	for {
		select {
		case request := <-actor.operations:
			request.result <- ErrProviderUnavailable
		default:
			return
		}
	}
}

type providerEventKind uint8

const (
	eventReady providerEventKind = iota + 1
	eventFrame
	eventPhoneResponse
	eventSessionChanged
)

type providerEvent struct {
	kind    providerEventKind
	session []byte
}

type Hooks struct {
	send    func(providerEvent)
	session func([]byte)
}

type providerCallbacks struct {
	events    chan providerEvent
	sessions  chan []byte
	sessionMu sync.Mutex
	active    atomic.Bool
}

func (hooks Hooks) Ready()         { hooks.emit(providerEvent{kind: eventReady}) }
func (hooks Hooks) Frame()         { hooks.emit(providerEvent{kind: eventFrame}) }
func (hooks Hooks) PhoneResponse() { hooks.emit(providerEvent{kind: eventPhoneResponse}) }
func (hooks Hooks) SessionChanged(plaintext []byte) {
	if hooks.session != nil {
		hooks.session(plaintext)
		return
	}
	hooks.emit(providerEvent{kind: eventSessionChanged, session: append([]byte(nil), plaintext...)})
}

func (hooks Hooks) emit(event providerEvent) {
	if hooks.send != nil {
		hooks.send(event)
	}
}

func (actor *Actor) Run(ctx context.Context, key Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !key.valid() {
		return ErrInvalidConfig
	}
	lease, acquired, err := actor.store.AcquireConnectionLease(ctx, key.TenantID, key.ConnectionID, actor.ownerID, actor.leaseTTL)
	if err != nil {
		return err
	}
	if !acquired {
		return ErrLeaseUnavailable
	}
	actor.metrics.LeaseAcquired()
	defer actor.release(key, lease.FencingToken)
	operationRunID, err := actor.beginOperationRun(key)
	if err != nil {
		return err
	}
	defer actor.endOperationRun(operationRunID)

	connection, err := actor.store.GetConnection(ctx, key.TenantID, key.ConnectionID)
	if err != nil {
		return err
	}
	if connection.TenantID != key.TenantID || connection.ID != key.ConnectionID {
		return domain.ErrTenantBoundary
	}
	switch connection.State {
	case domain.ConnectionStateConnected, domain.ConnectionStateDegraded, domain.ConnectionStateDisconnected:
	case domain.ConnectionStateReauthorizationRequired, domain.ConnectionStateUnpaired, domain.ConnectionStatePairing, domain.ConnectionStateSuspended:
		return nil
	default:
		return domain.ErrInvalidConnectionState
	}

	snapshot, err := actor.sessions.LoadVersioned(ctx, key)
	if err != nil {
		return err
	}
	defer zero(snapshot.Plaintext)
	if len(snapshot.Plaintext) == 0 || snapshot.Revision == 0 {
		return ErrInvalidConfig
	}
	backoff, _ := NewBackoff(actor.backoffConfig)
	health := Health{ActorState: "connecting", ConnectionState: domain.ConnectionStateDisconnected, LastSafeReason: ReasonNone, FencingToken: lease.FencingToken}
	if owned, writeErr := actor.writeHealth(ctx, key, lease.FencingToken, &health); writeErr != nil || !owned {
		return writeErr
	}

	for {
		callbacks, hooks := newProviderCallbacks(ctx)
		restoredSession := append([]byte(nil), snapshot.Plaintext...)
		providerCtx := ContextWithProviderOwnership(ctx, ProviderOwnership{Key: key, OwnerID: actor.ownerID, FencingToken: lease.FencingToken, LeaseTTL: actor.leaseTTL})
		provider, restoreErr := actor.providers.Restore(providerCtx, restoredSession, hooks)
		zero(restoredSession)
		if restoreErr != nil {
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonProviderConfig
			_, _ = actor.writeHealth(ctx, key, lease.FencingToken, &health)
			return nil
		}
		outcome := actor.runGeneration(ctx, key, lease.FencingToken, operationRunID, provider, &snapshot, backoff, &health, callbacks)
		if outcome.stop {
			return outcome.err
		}
		actor.metrics.Reconnect(ReasonTransientNetwork)
		delay := backoff.Fail()
		health.ActorState = "backoff"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.ReconnectCount++
		health.CurrentBackoff = delay
		health.LastSafeReason = ReasonTransientNetwork
		if owned, writeErr := actor.writeHealth(ctx, key, lease.FencingToken, &health); writeErr != nil || !owned {
			return writeErr
		}
		actor.metrics.Backoff(ReasonTransientNetwork, delay)
		if waitErr := actor.waitBackoff(ctx, key, lease.FencingToken, delay, &health); waitErr != nil {
			if errors.Is(waitErr, context.Canceled) {
				return nil
			}
			return waitErr
		}
		health.ActorState = "connecting"
		health.CurrentBackoff = 0
		if owned, writeErr := actor.writeHealth(ctx, key, lease.FencingToken, &health); writeErr != nil || !owned {
			return writeErr
		}
	}
}

type generationOutcome struct {
	stop bool
	err  error
}

type generationExitKind uint8

const (
	generationExitShutdown generationExitKind = iota + 1
	generationExitLeaseLost
	generationExitSessionConflict
	generationExitProvider
)

type generationExit struct {
	kind           generationExitKind
	err            error
	providerErr    error
	providerJoined bool
}

func (actor *Actor) runGeneration(ctx context.Context, key Key, token, operationRunID uint64, provider Provider, snapshot *SessionSnapshot, backoff *Backoff, health *Health, callbacks *providerCallbacks) generationOutcome {
	generationCtx, cancel := context.WithCancel(ctx)
	actor.setOperationReady(operationRunID, false)
	connectResult := make(chan error, 1)
	go func() { connectResult <- provider.Connect(generationCtx) }()
	renewTimer := actor.newRenewTimer(actor.renewDelay())
	defer renewTimer.Stop()
	var pending []byte
	var debounce Timer
	var activeOperation *activeProviderOperation
	defer func() {
		actor.setOperationReady(operationRunID, false)
		if debounce != nil {
			debounce.Stop()
		}
		zero(pending)
	}()

	var exit generationExit
generationLoop:
	for {
		var debounceChannel <-chan time.Time
		var operationQueue <-chan providerOperationRequest
		var operationResult <-chan error
		if debounce != nil {
			debounceChannel = debounce.C()
		}
		if activeOperation == nil && health.ActorState == "ready" {
			operationQueue = actor.operations
		} else if activeOperation != nil {
			operationResult = activeOperation.done
		}
		select {
		case <-ctx.Done():
			exit = generationExit{kind: generationExitShutdown}
			break generationLoop
		case <-renewTimer.C():
			renewed, err := actor.store.RenewConnectionLease(ctx, key.TenantID, key.ConnectionID, actor.ownerID, token, actor.leaseTTL)
			if err != nil || !renewed {
				if ctx.Err() != nil {
					exit = generationExit{kind: generationExitShutdown}
				} else {
					exit = generationExit{kind: generationExitLeaseLost, err: err}
				}
				break generationLoop
			}
			renewTimer.Stop()
			renewTimer = actor.newRenewTimer(actor.renewDelay())
		case event := <-callbacks.events:
			switch event.kind {
			case eventReady:
				backoff.Ready()
				now := actor.now().UTC()
				health.ActorState = "ready"
				health.ConnectionState = domain.ConnectionStateConnected
				health.ConnectedAt = &now
				health.CurrentBackoff = 0
				health.LastSafeReason = ReasonNone
			case eventFrame:
				now := actor.now().UTC()
				health.LastFrameAt = &now
			case eventPhoneResponse:
				now := actor.now().UTC()
				health.LastPhoneResponseAt = &now
			case eventSessionChanged:
				zero(pending)
				pending = event.session
				if debounce == nil {
					debounce = actor.newDebounceTimer(actor.sessionDebounce)
				}
				continue
			}
			if owned, err := actor.writeHealth(ctx, key, token, health); err != nil || !owned {
				if ctx.Err() != nil {
					exit = generationExit{kind: generationExitShutdown}
				} else {
					exit = generationExit{kind: generationExitLeaseLost, err: err}
				}
				break generationLoop
			}
			if event.kind == eventReady {
				actor.setOperationReady(operationRunID, true)
			}
		case request := <-operationQueue:
			if request.runID != operationRunID || request.key != key || request.ctx.Err() != nil {
				request.result <- ErrProviderUnavailable
				continue
			}
			operationCtx, operationCancel := context.WithCancel(ContextWithProviderOwnership(generationCtx, ProviderOwnership{
				Key: key, OwnerID: actor.ownerID, FencingToken: token, LeaseTTL: actor.leaseTTL,
			}))
			stop := context.AfterFunc(request.ctx, operationCancel)
			result := make(chan error, 1)
			go func() { result <- request.execute(operationCtx, provider) }()
			activeOperation = &activeProviderOperation{request: request, cancel: operationCancel, stop: stop, done: result}
		case operationErr := <-operationResult:
			activeOperation.stop()
			activeOperation.cancel()
			activeOperation.request.result <- operationErr
			activeOperation = nil
		case session := <-callbacks.sessions:
			zero(pending)
			pending = session
			if debounce == nil {
				debounce = actor.newDebounceTimer(actor.sessionDebounce)
			}
		case <-debounceChannel:
			if actor.beforeDebounceFlush != nil {
				actor.beforeDebounceFlush()
			}
			debounce.Stop()
			debounce = nil
			if latest := callbacks.takeLatest(); len(latest) > 0 {
				zero(pending)
				pending = latest
			}
			if ctx.Err() != nil {
				exit = generationExit{kind: generationExitShutdown}
				break generationLoop
			}
			swapped, err := actor.flushPendingSessionDetached(key, token, snapshot, &pending)
			if err != nil || !swapped {
				exit = generationExit{kind: generationExitSessionConflict, err: err}
				break generationLoop
			}
		case connectErr := <-connectResult:
			if terminalProviderError(connectErr) {
				exit = generationExit{kind: generationExitProvider, providerErr: connectErr, providerJoined: true}
			} else if ctx.Err() != nil || errors.Is(connectErr, context.Canceled) {
				exit = generationExit{kind: generationExitShutdown, providerJoined: true}
			} else {
				exit = generationExit{kind: generationExitProvider, providerErr: connectErr, providerJoined: true}
			}
			break generationLoop
		}
	}
	actor.setOperationReady(operationRunID, false)
	if activeOperation != nil {
		activeOperation.cancel()
		activeOperation.stop()
		select {
		case operationErr := <-activeOperation.done:
			activeOperation.request.result <- operationErr
		case <-time.After(actor.joinTimeout):
			activeOperation.request.result <- ErrProviderUnavailable
			exit.err = errors.Join(exit.err, ErrProviderJoinTimeout)
		}
	}
	return actor.finalizeGeneration(ctx, cancel, key, token, provider, connectResult, exit, snapshot, &pending, health, callbacks)
}

func (actor *Actor) finalizeGeneration(ctx context.Context, cancel context.CancelFunc, key Key, token uint64, provider Provider, connectResult <-chan error, exit generationExit, snapshot *SessionSnapshot, pending *[]byte, health *Health, callbacks *providerCallbacks) generationOutcome {
	cancel()
	disconnectJoined := actor.disconnectProvider(provider)
	connectJoined := exit.providerJoined
	if !exit.providerJoined {
		select {
		case connectErr := <-connectResult:
			connectJoined = true
			if terminalProviderError(connectErr) {
				exit.kind = generationExitProvider
				exit.providerErr = errors.Join(exit.providerErr, connectErr)
			}
		case <-time.After(actor.joinTimeout):
		}
	}
	if latest := callbacks.stopAcceptingAndTakeLatest(); len(latest) > 0 {
		zero(*pending)
		*pending = latest
	}
	swapped, swapErr := actor.flushPendingSessionDetached(key, token, snapshot, pending)
	if swapErr != nil || !swapped {
		if actor.applyTerminalProviderHealth(health, exit.providerErr) {
			// A provider terminal cause observed during the join remains stronger
			// than a final session-CAS error or stale revision.
		} else if exit.kind == generationExitLeaseLost {
			health.ActorState = "lease-lost"
			health.ConnectionState = domain.ConnectionStateDisconnected
			health.LastSafeReason = ReasonLeaseLost
			actor.metrics.LeaseLost()
		} else {
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonSessionConflict
		}
		health.CurrentBackoff = 0
		actor.writeHealthDetached(key, token, health)
		if swapErr != nil {
			return generationOutcome{stop: true, err: errors.Join(swapErr, exit.err, exit.providerErr, joinError(disconnectJoined, connectJoined))}
		}
		return generationOutcome{stop: true, err: errors.Join(exit.err, exit.providerErr, joinError(disconnectJoined, connectJoined))}
	}
	if !disconnectJoined || !connectJoined || errors.Is(exit.err, ErrProviderJoinTimeout) {
		if !actor.applyTerminalProviderHealth(health, exit.providerErr) {
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonProviderProtocol
		}
		health.CurrentBackoff = 0
		actor.writeHealthDetached(key, token, health)
		return generationOutcome{stop: true, err: errors.Join(exit.err, exit.providerErr, joinError(disconnectJoined, connectJoined))}
	}
	// Teardown may begin for shutdown, lease loss, or session conflict before a
	// stronger terminal provider failure arrives from the joined Connect call.
	// Normalize here so no earlier exit kind can discard the joined cause.
	if terminalProviderError(exit.providerErr) {
		exit.kind = generationExitProvider
	}

	switch exit.kind {
	case generationExitShutdown:
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDisconnected
		health.CurrentBackoff = 0
		health.LastSafeReason = ReasonShutdown
		actor.writeHealthDetached(key, token, health)
		return generationOutcome{stop: true}
	case generationExitLeaseLost:
		health.ActorState = "lease-lost"
		health.ConnectionState = domain.ConnectionStateDisconnected
		health.CurrentBackoff = 0
		health.LastSafeReason = ReasonLeaseLost
		actor.metrics.LeaseLost()
		actor.writeHealthDetached(key, token, health)
		return generationOutcome{stop: true, err: exit.err}
	case generationExitSessionConflict:
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.CurrentBackoff = 0
		health.LastSafeReason = ReasonSessionConflict
		actor.writeHealthDetached(key, token, health)
		return generationOutcome{stop: true, err: exit.err}
	case generationExitProvider:
		switch {
		case errors.Is(exit.providerErr, ErrStaleGeneration):
			health.ActorState = "lease-lost"
			health.ConnectionState = domain.ConnectionStateDisconnected
			health.CurrentBackoff = 0
			health.LastSafeReason = ReasonLeaseLost
			actor.metrics.LeaseLost()
			actor.writeHealthDetached(key, token, health)
			return generationOutcome{stop: true, err: exit.providerErr}
		case errors.Is(exit.providerErr, ErrSharedInfrastructure):
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonSharedInfrastructure
			actor.writeHealthDetached(key, token, health)
			return generationOutcome{stop: true, err: exit.providerErr}
		case errors.Is(exit.providerErr, ErrProviderAuthorization):
			transitioned, err := actor.store.MarkReauthorizationRequiredFenced(ctx, key.TenantID, key.ConnectionID, actor.ownerID, token)
			if err != nil || !transitioned {
				return generationOutcome{stop: true, err: err}
			}
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateReauthorizationRequired
			health.RequiresReauthorization = true
			health.LastSafeReason = ReasonProviderAuth
			actor.writeHealthDetached(key, token, health)
			return generationOutcome{stop: true}
		case errors.Is(exit.providerErr, ErrProviderPermanentConfig):
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonProviderConfig
			actor.writeHealthDetached(key, token, health)
			return generationOutcome{stop: true}
		case errors.Is(exit.providerErr, ErrProviderPermanentProtocol):
			health.ActorState = "stopped"
			health.ConnectionState = domain.ConnectionStateDegraded
			health.LastSafeReason = ReasonProviderProtocol
			actor.writeHealthDetached(key, token, health)
			return generationOutcome{stop: true, err: exit.providerErr}
		default:
			return generationOutcome{}
		}
	default:
		return generationOutcome{stop: true, err: exit.err}
	}
}

func terminalProviderError(err error) bool {
	return errors.Is(err, ErrStaleGeneration) ||
		errors.Is(err, ErrSharedInfrastructure) ||
		errors.Is(err, ErrProviderAuthorization) ||
		errors.Is(err, ErrProviderPermanentConfig) ||
		errors.Is(err, ErrProviderPermanentProtocol)
}

func (actor *Actor) applyTerminalProviderHealth(health *Health, providerErr error) bool {
	switch {
	case errors.Is(providerErr, ErrStaleGeneration):
		health.ActorState = "lease-lost"
		health.ConnectionState = domain.ConnectionStateDisconnected
		health.LastSafeReason = ReasonLeaseLost
		actor.metrics.LeaseLost()
	case errors.Is(providerErr, ErrSharedInfrastructure):
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.LastSafeReason = ReasonSharedInfrastructure
	case errors.Is(providerErr, ErrProviderAuthorization):
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.LastSafeReason = ReasonProviderAuth
	case errors.Is(providerErr, ErrProviderPermanentConfig):
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.LastSafeReason = ReasonProviderConfig
	case errors.Is(providerErr, ErrProviderPermanentProtocol):
		health.ActorState = "stopped"
		health.ConnectionState = domain.ConnectionStateDegraded
		health.LastSafeReason = ReasonProviderProtocol
	default:
		return false
	}
	health.CurrentBackoff = 0
	return true
}

func (actor *Actor) flushPendingSession(ctx context.Context, key Key, token uint64, snapshot *SessionSnapshot, pending *[]byte) (bool, error) {
	if len(*pending) == 0 {
		return true, nil
	}
	swapped, err := actor.sessions.CompareAndSwapFenced(ctx, key, actor.ownerID, token, snapshot.Revision, *pending)
	if err != nil || !swapped {
		zero(*pending)
		*pending = nil
		return swapped, err
	}
	zero(snapshot.Plaintext)
	snapshot.Plaintext = append([]byte(nil), (*pending)...)
	snapshot.Revision++
	zero(*pending)
	*pending = nil
	return true, nil
}

func (actor *Actor) flushPendingSessionDetached(key Key, token uint64, snapshot *SessionSnapshot, pending *[]byte) (bool, error) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return actor.flushPendingSession(cleanupCtx, key, token, snapshot, pending)
}

func (actor *Actor) disconnectProvider(provider Provider) bool {
	ctx, cancel := context.WithTimeout(context.Background(), actor.joinTimeout)
	defer cancel()
	done := make(chan struct{}, 1)
	go func() {
		_ = provider.Disconnect(ctx)
		done <- struct{}{}
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func joinError(disconnectJoined, connectJoined bool) error {
	if disconnectJoined && connectJoined {
		return nil
	}
	return ErrProviderJoinTimeout
}

func newProviderCallbacks(ctx context.Context) (*providerCallbacks, Hooks) {
	callbacks := &providerCallbacks{
		events: make(chan providerEvent, providerEventBufferSize), sessions: make(chan []byte, 1),
	}
	callbacks.active.Store(true)
	hooks := Hooks{send: func(event providerEvent) {
		if ctx.Err() != nil || !callbacks.active.Load() {
			zero(event.session)
			return
		}
		select {
		case callbacks.events <- event:
		default:
			zero(event.session)
		}
	}, session: func(plaintext []byte) {
		callbacks.replaceSession(plaintext)
	}}
	return callbacks, hooks
}

func (callbacks *providerCallbacks) replaceSession(plaintext []byte) {
	latest := append([]byte(nil), plaintext...)
	callbacks.sessionMu.Lock()
	defer callbacks.sessionMu.Unlock()
	if !callbacks.active.Load() {
		zero(latest)
		return
	}
	select {
	case stale := <-callbacks.sessions:
		zero(stale)
	default:
	}
	callbacks.sessions <- latest
}

func (callbacks *providerCallbacks) stopAcceptingAndTakeLatest() []byte {
	callbacks.sessionMu.Lock()
	defer callbacks.sessionMu.Unlock()
	callbacks.active.Store(false)
	return callbacks.takeLatestLocked()
}

func (callbacks *providerCallbacks) takeLatest() []byte {
	callbacks.sessionMu.Lock()
	defer callbacks.sessionMu.Unlock()
	return callbacks.takeLatestLocked()
}

func (callbacks *providerCallbacks) takeLatestLocked() []byte {
	select {
	case latest := <-callbacks.sessions:
		return latest
	default:
		return nil
	}
}

func (actor *Actor) waitBackoff(ctx context.Context, key Key, token uint64, delay time.Duration, health *Health) error {
	backoffTimer := actor.newBackoffTimer(delay)
	defer backoffTimer.Stop()
	renewTimer := actor.newRenewTimer(actor.renewDelay())
	defer renewTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backoffTimer.C():
			return nil
		case <-renewTimer.C():
			renewed, err := actor.store.RenewConnectionLease(ctx, key.TenantID, key.ConnectionID, actor.ownerID, token, actor.leaseTTL)
			if err != nil || !renewed {
				health.ActorState = "lease-lost"
				health.LastSafeReason = ReasonLeaseLost
				actor.metrics.LeaseLost()
				_, _ = actor.writeHealth(ctx, key, token, health)
				if err != nil {
					return err
				}
				return ErrLeaseUnavailable
			}
			renewTimer.Stop()
			renewTimer = actor.newRenewTimer(actor.renewDelay())
		}
	}
}

func (actor *Actor) renewDelay() time.Duration {
	base := actor.leaseTTL / 3
	jitterRange := actor.leaseTTL / 6
	if jitterRange <= 0 {
		return base
	}
	return base + time.Duration(cryptoInt63n(int64(jitterRange)))
}

func (actor *Actor) writeHealth(ctx context.Context, key Key, token uint64, health *Health) (bool, error) {
	health.FencingToken = token
	health.UpdatedAt = actor.now().UTC()
	actor.metrics.ActorState(health.ActorState)
	return actor.store.WriteConnectionHealthFenced(ctx, key.TenantID, key.ConnectionID, actor.ownerID, token, *health)
}

func (actor *Actor) writeHealthDetached(key Key, token uint64, health *Health) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = actor.writeHealth(shutdownCtx, key, token, health)
}

func (actor *Actor) release(key Key, token uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = actor.store.ReleaseConnectionLease(ctx, key.TenantID, key.ConnectionID, actor.ownerID, token)
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
