package connectionactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestActorLeaseLossCancelsProviderAndRejectsCallbacksAfterExit(t *testing.T) {
	renewTimer := newManualTimer()
	store := newFakeActorStore()
	store.renew = false
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	sessions := &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("encrypted-vault-output"), Revision: 4}}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		LeaseTTL: 9 * time.Second, NewRenewTimer: func(time.Duration) Timer { return renewTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	renewTimer.Fire()
	receive(t, provider.cancelled, "provider cancellation")
	if err := receiveValue(t, done, "actor exit"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := store.lastHealth().LastSafeReason; got != ReasonLeaseLost {
		t.Fatalf("last safe reason = %q, want %q", got, ReasonLeaseLost)
	}
	writes := store.healthWrites.Load()
	factory.hooks.Frame()
	factory.hooks.PhoneResponse()
	if got := store.healthWrites.Load(); got != writes {
		t.Fatalf("callbacks after stop wrote health: before=%d after=%d", writes, got)
	}
}

func TestActorExecutesProviderIOOnlyInsideReadyFencedGeneration(t *testing.T) {
	renewTimer := newManualTimer()
	store := newFakeActorStore()
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}}, Providers: factory,
		LeaseTTL: 9 * time.Second, NewRenewTimer: func(time.Duration) Timer { return renewTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	if err := actor.Execute(context.Background(), testActorKey(), func(context.Context, Provider) error { return nil }); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("operation before ready error = %v, want ErrProviderUnavailable", err)
	}
	factory.hooks.Ready()
	readyDeadline := time.After(2 * time.Second)
	readyTicker := time.NewTicker(time.Millisecond)
	defer readyTicker.Stop()
	for store.lastHealth().ActorState != "ready" {
		select {
		case <-readyTicker.C:
		case <-readyDeadline:
			t.Fatal("actor did not enter ready state")
		}
	}
	executed := make(chan ProviderOwnership, 1)
	err := actor.Execute(context.Background(), testActorKey(), func(operationCtx context.Context, active Provider) error {
		ownership, ok := ProviderOwnershipFromContext(operationCtx)
		if !ok || active != provider {
			return errors.New("operation escaped active generation")
		}
		executed <- ownership
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ownership := receiveValue(t, executed, "fenced provider operation")
	if ownership.OwnerID != "owner-a" || ownership.FencingToken == 0 || ownership.Key != testActorKey() {
		t.Fatalf("operation ownership = %+v", ownership)
	}
	store.renew = false
	renewTimer.Fire()
	receive(t, provider.cancelled, "provider cancellation")
	if err = receiveValue(t, done, "actor exit"); err != nil {
		t.Fatal(err)
	}
	if err = actor.Execute(context.Background(), testActorKey(), func(context.Context, Provider) error { return nil }); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("operation after lease loss error = %v, want ErrProviderUnavailable", err)
	}
	cancel()
}

func TestActorLeaseLossBoundsStuckProviderOperationJoin(t *testing.T) {
	renewTimer := newManualTimer()
	store := newFakeActorStore()
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}}, Providers: factory,
		LeaseTTL: 9 * time.Second, JoinTimeout: 20 * time.Millisecond, NewRenewTimer: func(time.Duration) Timer { return renewTimer },
	})
	done := make(chan error, 1)
	go func() { done <- actor.Run(context.Background(), testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.Ready()
	for store.lastHealth().ActorState != "ready" {
		time.Sleep(time.Millisecond)
	}
	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- actor.Execute(context.Background(), testActorKey(), func(context.Context, Provider) error {
			close(operationStarted)
			<-releaseOperation
			return nil
		})
	}()
	receive(t, operationStarted, "stuck provider operation")
	store.renew = false
	renewTimer.Fire()
	if err := receiveValue(t, done, "bounded stuck-operation exit"); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Run() error = %v, want ErrProviderJoinTimeout", err)
	}
	close(releaseOperation)
	if err := receiveValue(t, operationDone, "rejected stale operation"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("Execute() error = %v, want ErrProviderUnavailable", err)
	}
}

func TestActorShutdownBoundsProviderConnectAndDisconnectJoins(t *testing.T) {
	provider := &stuckLifecycleProvider{started: make(chan struct{}), disconnectStarted: make(chan struct{}), release: make(chan struct{})}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: newFakeActorStore(), Sessions: &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}},
		Providers: &singleProviderFactory{provider: provider}, JoinTimeout: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "stuck provider connect")
	cancel()
	receive(t, provider.disconnectStarted, "stuck provider disconnect")
	if err := receiveValue(t, done, "bounded provider lifecycle exit"); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Run() error = %v, want ErrProviderJoinTimeout", err)
	}
	close(provider.release)
}

type singleProviderFactory struct{ provider Provider }

func (factory *singleProviderFactory) Restore(context.Context, []byte, Hooks) (Provider, error) {
	return factory.provider, nil
}

type stuckLifecycleProvider struct {
	started           chan struct{}
	disconnectStarted chan struct{}
	release           chan struct{}
	startOnce         sync.Once
	disconnectOnce    sync.Once
}

func (provider *stuckLifecycleProvider) Connect(context.Context) error {
	provider.startOnce.Do(func() { close(provider.started) })
	<-provider.release
	return context.Canceled
}

func (provider *stuckLifecycleProvider) Disconnect(context.Context) error {
	provider.disconnectOnce.Do(func() { close(provider.disconnectStarted) })
	<-provider.release
	return context.Canceled
}

func TestActorAuthorizationFailureTransitionsOnceAndDoesNotReconnect(t *testing.T) {
	store := newFakeActorStore()
	provider := &fakeRuntimeProvider{connectErr: ErrProviderAuthorization, started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions:  &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}},
		Providers: factory,
	})

	if err := actor.Run(context.Background(), testActorKey()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := store.reauthorizationCalls.Load(); got != 1 {
		t.Fatalf("reauthorization transitions = %d, want 1", got)
	}
	if got := factory.restores.Load(); got != 1 {
		t.Fatalf("provider restorations = %d, want 1", got)
	}
	health := store.lastHealth()
	if !health.RequiresReauthorization || health.ConnectionState != domain.ConnectionStateReauthorizationRequired || health.LastSafeReason != ReasonProviderAuth {
		t.Fatalf("authorization health = %#v", health)
	}
}

func TestActorSharedInfrastructureFailureStopsWithDistinctHealthClassification(t *testing.T) {
	store := newFakeActorStore()
	sharedFailure := errors.Join(ErrSharedInfrastructure, errors.New("durable inbox unavailable"))
	provider := &fakeRuntimeProvider{connectErr: sharedFailure, started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions: &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}}, Providers: factory,
	})
	err := actor.Run(context.Background(), testActorKey())
	if !errors.Is(err, ErrSharedInfrastructure) || factory.restores.Load() != 1 {
		t.Fatalf("shared infrastructure Run() = %v restorations=%d", err, factory.restores.Load())
	}
	if health := store.lastHealth(); health.LastSafeReason != ReasonSharedInfrastructure || health.ConnectionState != domain.ConnectionStateDegraded {
		t.Fatalf("shared infrastructure health = %#v", health)
	}
}

func TestActorStaleDurableFenceStopsWithoutProviderReconnect(t *testing.T) {
	store := newFakeActorStore()
	stale := errors.Join(ErrStaleGeneration, errors.New("connection fence lost"))
	provider := &fakeRuntimeProvider{connectErr: stale, started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions: &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}}, Providers: factory,
	})
	err := actor.Run(context.Background(), testActorKey())
	if !errors.Is(err, ErrStaleGeneration) || factory.restores.Load() != 1 {
		t.Fatalf("stale fence Run() = %v restorations=%d", err, factory.restores.Load())
	}
	if health := store.lastHealth(); health.LastSafeReason != ReasonLeaseLost || health.ConnectionState != domain.ConnectionStateDisconnected {
		t.Fatalf("stale fence health = %#v", health)
	}
}

func TestActorTransientFailureReconnectsAfterCancellableBackoff(t *testing.T) {
	store := newFakeActorStore()
	backoffTimer := newManualTimer()
	first := &fakeRuntimeProvider{connectErr: ErrProviderTransient, started: make(chan struct{}), cancelled: make(chan struct{})}
	second := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{providers: []*fakeRuntimeProvider{first, second}}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions:        &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}},
		Providers:       factory,
		Backoff:         BackoffConfig{Base: time.Second, Cap: time.Minute, Int63n: func(int64) int64 { return int64(time.Second) }},
		NewBackoffTimer: func(time.Duration) Timer { return backoffTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, first.started, "first provider start")
	receive(t, backoffTimer.Created(), "backoff creation")
	backoffTimer.Fire()
	receive(t, second.started, "second provider start")
	if got := factory.restores.Load(); got != 2 {
		t.Fatalf("provider restorations = %d, want 2", got)
	}
	cancel()
	if err := receiveValue(t, done, "actor shutdown"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestActorCancellationWhileBackoffActiveStopsTimerAndDoesNotRestart(t *testing.T) {
	store := newFakeActorStore()
	backoffTimer := newManualTimer()
	provider := &fakeRuntimeProvider{connectErr: ErrProviderTransient, started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions:        &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 2}},
		Providers:       factory,
		Backoff:         BackoffConfig{Base: time.Hour, Cap: time.Hour, Int63n: func(int64) int64 { return int64(time.Hour) - 1 }},
		NewBackoffTimer: func(time.Duration) Timer { return backoffTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	receive(t, backoffTimer.Created(), "active backoff wait")
	cancel()
	if err := receiveValue(t, done, "prompt actor backoff cancellation"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !backoffTimer.stopped.Load() {
		t.Fatal("backoff timer was not stopped on cancellation")
	}
	if got := factory.restores.Load(); got != 1 {
		t.Fatalf("provider restorations after cancellation = %d, want 1", got)
	}
}

func TestActorCoalescesSessionChangesAndFencedCASConflictStopsActor(t *testing.T) {
	store := newFakeActorStore()
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot:   SessionSnapshot{Plaintext: []byte("restored"), Revision: 7},
		swapResult: false,
		swapped:    make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Second, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	done := make(chan error, 1)
	go func() { done <- actor.Run(context.Background(), testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-one"))
	receive(t, debounceTimer.Created(), "debounce creation")
	factory.hooks.SessionChanged([]byte("rotation-two"))
	factory.hooks.SessionChanged([]byte("rotation-latest"))
	factory.hooks.Frame()
	receive(t, store.frameWritten, "frame write after session events")
	debounceTimer.Fire()
	if got := string(receiveValue(t, sessions.swapped, "session swap")); got != "rotation-latest" {
		t.Fatalf("swapped session = %q", got)
	}
	if err := receiveValue(t, done, "actor exit after CAS rejection"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := sessions.swapCalls.Load(); got != 1 {
		t.Fatalf("session CAS calls = %d, want 1", got)
	}
	if got := store.lastHealth().LastSafeReason; got != ReasonSessionConflict {
		t.Fatalf("last reason = %q, want %q", got, ReasonSessionConflict)
	}
}

func TestActorPersistsLatestSessionWhenLivenessMailboxIsSaturated(t *testing.T) {
	store := &blockingHealthActorStore{
		fakeActorStore: newFakeActorStore(),
		frameEntered:   make(chan struct{}),
		releaseFrame:   make(chan struct{}),
	}
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot:   SessionSnapshot{Plaintext: []byte("restored"), Revision: 7},
		swapResult: false,
		swapped:    make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Second, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	done := make(chan error, 1)
	go func() { done <- actor.Run(context.Background(), testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.Frame()
	receive(t, store.frameEntered, "blocked frame health write")
	for range providerEventBufferSize {
		factory.hooks.Frame()
	}
	for index := range 100 {
		factory.hooks.SessionChanged([]byte(fmt.Sprintf("rotation-%03d", index)))
	}
	close(store.releaseFrame)
	receive(t, debounceTimer.Created(), "debounce creation after liveness saturation")
	debounceTimer.Fire()
	if got := string(receiveValue(t, sessions.swapped, "latest saturated session swap")); got != "rotation-099" {
		t.Fatalf("swapped session = %q, want newest rotation", got)
	}
	if err := receiveValue(t, done, "actor exit after saturated session CAS rejection"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestActorCancellationFlushesLatestPendingSessionWithBoundedFence(t *testing.T) {
	store := newFakeActorStore()
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot:   SessionSnapshot{Plaintext: []byte("restored"), Revision: 7},
		swapResult: true,
		swapped:    make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{
		started:            make(chan struct{}),
		cancelled:          make(chan struct{}),
		releaseAfterCancel: make(chan struct{}),
		disconnectCalled:   make(chan struct{}),
	}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-pending"))
	receive(t, debounceTimer.Created(), "pending session debounce")
	cancel()
	receive(t, provider.disconnectCalled, "provider teardown")
	// A provider-owned final callback may race shutdown, but Connect has not
	// returned yet, so it still belongs to this generation.
	factory.hooks.SessionChanged([]byte("rotation-teardown-latest"))
	close(provider.releaseAfterCancel)
	if err := receiveValue(t, done, "actor cancellation flush"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := string(receiveValue(t, sessions.swapped, "final session swap")); got != "rotation-teardown-latest" {
		t.Fatalf("swapped session = %q, want teardown latest", got)
	}
	if got := string(sessions.persistedSession()); got != "rotation-teardown-latest" {
		t.Fatalf("persisted session = %q, want teardown latest", got)
	}
	if got := sessions.swapCalls.Load(); got != 1 {
		t.Fatalf("session CAS calls = %d, want exactly 1", got)
	}
	if !sessions.swapHadDeadline() || sessions.swapContextError() != nil {
		t.Fatalf("cleanup CAS context: deadline=%v err=%v", sessions.swapHadDeadline(), sessions.swapContextError())
	}
	if !allZero(sessions.passedSession()) {
		t.Fatal("plaintext passed to final CAS was not zeroed after shutdown")
	}
}

func TestActorCancellationRejectsPendingSessionWhenFenceIsStale(t *testing.T) {
	store := newFakeActorStore()
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot:   SessionSnapshot{Plaintext: []byte("restored"), Revision: 7},
		swapResult: false,
		swapped:    make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{
		started:            make(chan struct{}),
		cancelled:          make(chan struct{}),
		releaseAfterCancel: make(chan struct{}),
		disconnectCalled:   make(chan struct{}),
	}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-stale"))
	receive(t, debounceTimer.Created(), "pending stale session debounce")
	cancel()
	receive(t, provider.disconnectCalled, "provider teardown")
	close(provider.releaseAfterCancel)
	if err := receiveValue(t, done, "actor stale-fence shutdown"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := string(receiveValue(t, sessions.swapped, "rejected final session swap")); got != "rotation-stale" {
		t.Fatalf("attempted session = %q, want rotation-stale", got)
	}
	if persisted := sessions.persistedSession(); len(persisted) != 0 {
		t.Fatalf("stale fence persisted session %q", persisted)
	}
	if got := sessions.swapCalls.Load(); got != 1 {
		t.Fatalf("session CAS calls = %d, want exactly 1", got)
	}
	if !sessions.swapHadDeadline() || sessions.swapContextError() != nil {
		t.Fatalf("rejected CAS context: deadline=%v err=%v", sessions.swapHadDeadline(), sessions.swapContextError())
	}
	if got := store.lastHealth().LastSafeReason; got != ReasonSessionConflict {
		t.Fatalf("last reason = %q, want %q", got, ReasonSessionConflict)
	}
	if !allZero(sessions.passedSession()) {
		t.Fatal("rejected plaintext was not zeroed after shutdown")
	}
}

func TestActorCancellationDuringRenewArmFinalizesLatestSessionOnce(t *testing.T) {
	store := &blockingRenewActorStore{
		fakeActorStore: newFakeActorStore(), renewEntered: make(chan struct{}), releaseRenew: make(chan struct{}),
	}
	renewTimer := newManualTimer()
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 7}, swapResult: true, swapped: make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		NewRenewTimer:   func(time.Duration) Timer { return renewTimer },
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-before-renew"))
	receive(t, debounceTimer.Created(), "pending renew session debounce")
	renewTimer.Fire()
	receive(t, store.renewEntered, "renew arm")
	cancel()
	factory.hooks.SessionChanged([]byte("rotation-renew-latest"))
	close(store.releaseRenew)
	err := receiveValue(t, done, "renew-arm cancellation finalization")
	assertFinalSessionSwap(t, sessions, "rotation-renew-latest")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestActorCancellationDuringLivenessArmFinalizesLatestSessionOnce(t *testing.T) {
	store := &blockingLivenessActorStore{
		fakeActorStore: newFakeActorStore(), livenessEntered: make(chan struct{}), releaseLiveness: make(chan struct{}),
	}
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 7}, swapResult: true, swapped: make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-before-liveness"))
	receive(t, debounceTimer.Created(), "pending liveness session debounce")
	factory.hooks.Frame()
	receive(t, store.livenessEntered, "liveness health arm")
	cancel()
	factory.hooks.SessionChanged([]byte("rotation-liveness-latest"))
	close(store.releaseLiveness)
	err := receiveValue(t, done, "liveness-arm cancellation finalization")
	assertFinalSessionSwap(t, sessions, "rotation-liveness-latest")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestActorCancellationDuringDebounceArmFinalizesLatestSessionOnce(t *testing.T) {
	store := newFakeActorStore()
	debounceTimer := newManualTimer()
	debounceEntered := make(chan struct{})
	releaseDebounce := make(chan struct{})
	sessions := &fakeActorSessions{
		snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 7}, swapResult: true, respectContext: true,
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{})}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
		beforeDebounceFlush: func() {
			close(debounceEntered)
			<-releaseDebounce
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- actor.Run(ctx, testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-before-debounce"))
	receive(t, debounceTimer.Created(), "pending debounce session")
	debounceTimer.Fire()
	receive(t, debounceEntered, "ready debounce arm")
	cancel()
	factory.hooks.SessionChanged([]byte("rotation-debounce-latest"))
	close(releaseDebounce)
	err := receiveValue(t, done, "debounce-arm cancellation finalization")
	if got := sessions.swapCalls.Load(); got != 1 {
		t.Fatalf("session CAS calls = %d, want exactly 1", got)
	}
	if got := string(sessions.persistedSession()); got != "rotation-debounce-latest" {
		t.Fatalf("persisted session = %q, want debounce latest", got)
	}
	if !sessions.swapHadDeadline() || sessions.swapContextError() != nil {
		t.Fatalf("final CAS context: deadline=%v err=%v", sessions.swapHadDeadline(), sessions.swapContextError())
	}
	if !allZero(sessions.passedSession()) {
		t.Fatal("plaintext passed to final CAS was not zeroed")
	}
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestActorConnectResultArmFinalizesPendingSessionOnce(t *testing.T) {
	store := newFakeActorStore()
	debounceTimer := newManualTimer()
	sessions := &fakeActorSessions{
		snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 7}, swapResult: true, swapped: make(chan []byte, 1),
	}
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{}), result: make(chan error, 1)}
	factory := &fakeProviderFactory{provider: provider}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store, Sessions: sessions, Providers: factory,
		SessionDebounce: time.Minute, NewDebounceTimer: func(time.Duration) Timer { return debounceTimer },
	})
	done := make(chan error, 1)
	go func() { done <- actor.Run(context.Background(), testActorKey()) }()
	receive(t, provider.started, "provider start")
	factory.hooks.SessionChanged([]byte("rotation-provider-result"))
	receive(t, debounceTimer.Created(), "pending provider-result session")
	provider.result <- ErrProviderPermanentConfig
	if err := receiveValue(t, done, "provider-result finalization"); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertFinalSessionSwap(t, sessions, "rotation-provider-result")
	if got := store.lastHealth().LastSafeReason; got != ReasonProviderConfig {
		t.Fatalf("last reason = %q, want %q", got, ReasonProviderConfig)
	}
}

func TestActorPermanentProviderProtocolReturnsTombstoneCause(t *testing.T) {
	store := newFakeActorStore()
	provider := &fakeRuntimeProvider{started: make(chan struct{}), cancelled: make(chan struct{}), result: make(chan error, 1)}
	actor := newTestActor(t, ActorConfig{
		OwnerID: "owner-a", Store: store,
		Sessions:  &fakeActorSessions{snapshot: SessionSnapshot{Plaintext: []byte("restored"), Revision: 7}},
		Providers: &fakeProviderFactory{provider: provider},
	})
	done := make(chan error, 1)
	go func() { done <- actor.Run(context.Background(), testActorKey()) }()
	receive(t, provider.started, "provider start")
	provider.result <- ErrProviderPermanentProtocol
	if err := receiveValue(t, done, "permanent protocol stop"); !errors.Is(err, ErrProviderPermanentProtocol) {
		t.Fatalf("Run() error = %v, want permanent protocol tombstone cause", err)
	}
	if got := store.lastHealth().LastSafeReason; got != ReasonProviderProtocol {
		t.Fatalf("last safe reason = %q, want %q", got, ReasonProviderProtocol)
	}
}

func assertFinalSessionSwap(t *testing.T, sessions *fakeActorSessions, want string) {
	t.Helper()
	if got := string(receiveValue(t, sessions.swapped, "final session swap")); got != want {
		t.Fatalf("swapped session = %q, want %q", got, want)
	}
	if got := sessions.swapCalls.Load(); got != 1 {
		t.Fatalf("session CAS calls = %d, want exactly 1", got)
	}
	if got := string(sessions.persistedSession()); got != want {
		t.Fatalf("persisted session = %q, want %q", got, want)
	}
	if !sessions.swapHadDeadline() || sessions.swapContextError() != nil {
		t.Fatalf("final CAS context: deadline=%v err=%v", sessions.swapHadDeadline(), sessions.swapContextError())
	}
	if !allZero(sessions.passedSession()) {
		t.Fatal("plaintext passed to final CAS was not zeroed")
	}
}

func TestProviderCallbacksFlushLatestSessionWhenGenerationStopsAccepting(t *testing.T) {
	callbacks, hooks := newProviderCallbacks(context.Background())
	hooks.SessionChanged([]byte("rotation-old"))
	hooks.SessionChanged([]byte("rotation-latest"))
	latest := callbacks.stopAcceptingAndTakeLatest()
	defer zero(latest)
	if got := string(latest); got != "rotation-latest" {
		t.Fatalf("generation-exit session = %q, want latest rotation", got)
	}
	hooks.SessionChanged([]byte("rotation-after-stop"))
	select {
	case stale := <-callbacks.sessions:
		zero(stale)
		t.Fatal("callback accepted a session after generation stop")
	default:
	}
}

func newTestActor(t *testing.T, config ActorConfig) *Actor {
	t.Helper()
	if config.LeaseTTL == 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.NewRenewTimer == nil {
		config.NewRenewTimer = func(time.Duration) Timer { return newManualTimer() }
	}
	actor, err := NewActor(config)
	if err != nil {
		t.Fatalf("NewActor() error = %v", err)
	}
	return actor
}

func testActorKey() Key { return Key{TenantID: "tenant-a", ConnectionID: "connection-1"} }

type fakeActorStore struct {
	mu sync.Mutex

	lease                Lease
	renew                bool
	health               []Health
	healthWrites         atomic.Int32
	reauthorizationCalls atomic.Int32
	frameWritten         chan struct{}
}

type blockingHealthActorStore struct {
	*fakeActorStore
	frameOnce    sync.Once
	frameEntered chan struct{}
	releaseFrame chan struct{}
}

type blockingRenewActorStore struct {
	*fakeActorStore
	renewEntered chan struct{}
	releaseRenew chan struct{}
}

func (store *blockingRenewActorStore) RenewConnectionLease(ctx context.Context, _ domain.TenantID, _ domain.ConnectionID, _ string, _ uint64, _ time.Duration) (bool, error) {
	close(store.renewEntered)
	<-store.releaseRenew
	return false, ctx.Err()
}

type blockingLivenessActorStore struct {
	*fakeActorStore
	livenessEntered chan struct{}
	releaseLiveness chan struct{}
	once            sync.Once
}

func (store *blockingLivenessActorStore) WriteConnectionHealthFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, token uint64, health Health) (bool, error) {
	if health.LastFrameAt != nil {
		store.once.Do(func() {
			close(store.livenessEntered)
			<-store.releaseLiveness
		})
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	return store.fakeActorStore.WriteConnectionHealthFenced(ctx, tenantID, connectionID, ownerID, token, health)
}

func (store *blockingHealthActorStore) WriteConnectionHealthFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, token uint64, health Health) (bool, error) {
	if health.LastFrameAt != nil {
		store.frameOnce.Do(func() {
			close(store.frameEntered)
			<-store.releaseFrame
		})
	}
	return store.fakeActorStore.WriteConnectionHealthFenced(ctx, tenantID, connectionID, ownerID, token, health)
}

func newFakeActorStore() *fakeActorStore {
	return &fakeActorStore{
		lease:        Lease{OwnerID: "owner-a", FencingToken: 11, ExpiresAt: time.Now().Add(time.Minute)},
		renew:        true,
		frameWritten: make(chan struct{}, 1),
	}
}

func (store *fakeActorStore) GetConnection(context.Context, domain.TenantID, domain.ConnectionID) (domain.Connection, error) {
	return domain.Connection{ID: "connection-1", TenantID: "tenant-a", Name: "Phone", State: domain.ConnectionStateConnected}, nil
}

func (store *fakeActorStore) AcquireConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, time.Duration) (Lease, bool, error) {
	return store.lease, true, nil
}

func (store *fakeActorStore) RenewConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, time.Duration) (bool, error) {
	return store.renew, nil
}

func (store *fakeActorStore) ReleaseConnectionLease(context.Context, domain.TenantID, domain.ConnectionID, string, uint64) (bool, error) {
	return true, nil
}

func (store *fakeActorStore) WriteConnectionHealthFenced(_ context.Context, _ domain.TenantID, _ domain.ConnectionID, _ string, _ uint64, health Health) (bool, error) {
	store.mu.Lock()
	store.health = append(store.health, health)
	store.mu.Unlock()
	store.healthWrites.Add(1)
	if health.LastFrameAt != nil {
		select {
		case store.frameWritten <- struct{}{}:
		default:
		}
	}
	return true, nil
}

func (store *fakeActorStore) MarkReauthorizationRequiredFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64) (bool, error) {
	store.reauthorizationCalls.Add(1)
	return true, nil
}

func (store *fakeActorStore) lastHealth() Health {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.health) == 0 {
		return Health{}
	}
	return store.health[len(store.health)-1]
}

type fakeActorSessions struct {
	mu             sync.Mutex
	snapshot       SessionSnapshot
	swapResult     bool
	swapCalls      atomic.Int32
	swapped        chan []byte
	persisted      []byte
	passed         []byte
	hadDeadline    bool
	contextErr     error
	respectContext bool
}

func (sessions *fakeActorSessions) LoadVersioned(context.Context, Key) (SessionSnapshot, error) {
	return SessionSnapshot{Plaintext: append([]byte(nil), sessions.snapshot.Plaintext...), Revision: sessions.snapshot.Revision}, nil
}

func (sessions *fakeActorSessions) CompareAndSwapFenced(ctx context.Context, _ Key, _ string, _, _ uint64, plaintext []byte) (bool, error) {
	sessions.swapCalls.Add(1)
	copyOfSession := append([]byte(nil), plaintext...)
	_, hadDeadline := ctx.Deadline()
	sessions.mu.Lock()
	sessions.passed = plaintext
	sessions.hadDeadline = hadDeadline
	sessions.contextErr = ctx.Err()
	contextErr := sessions.contextErr
	if sessions.swapResult && (!sessions.respectContext || contextErr == nil) {
		sessions.persisted = append([]byte(nil), plaintext...)
	}
	sessions.mu.Unlock()
	if sessions.swapped != nil {
		sessions.swapped <- copyOfSession
	}
	if sessions.respectContext && contextErr != nil {
		return false, contextErr
	}
	return sessions.swapResult, nil
}

func (sessions *fakeActorSessions) persistedSession() []byte {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return append([]byte(nil), sessions.persisted...)
}

func (sessions *fakeActorSessions) passedSession() []byte {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return append([]byte(nil), sessions.passed...)
}

func (sessions *fakeActorSessions) swapHadDeadline() bool {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return sessions.hadDeadline
}

func (sessions *fakeActorSessions) swapContextError() error {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	return sessions.contextErr
}

type fakeProviderFactory struct {
	provider  *fakeRuntimeProvider
	providers []*fakeRuntimeProvider
	restores  atomic.Int32
	hooks     Hooks
}

func (factory *fakeProviderFactory) Restore(_ context.Context, plaintext []byte, hooks Hooks) (Provider, error) {
	if len(plaintext) == 0 {
		return nil, errors.New("empty restored session")
	}
	index := int(factory.restores.Add(1)) - 1
	factory.hooks = hooks
	if len(factory.providers) > 0 {
		return factory.providers[index], nil
	}
	return factory.provider, nil
}

type fakeRuntimeProvider struct {
	connectErr         error
	started            chan struct{}
	cancelled          chan struct{}
	releaseAfterCancel chan struct{}
	disconnectCalled   chan struct{}
	result             chan error
	once               sync.Once
	disconnectOnce     sync.Once
}

func (provider *fakeRuntimeProvider) Connect(ctx context.Context) error {
	provider.once.Do(func() { close(provider.started) })
	if provider.result != nil {
		return <-provider.result
	}
	if provider.connectErr != nil {
		return provider.connectErr
	}
	<-ctx.Done()
	if provider.releaseAfterCancel != nil {
		<-provider.releaseAfterCancel
	}
	close(provider.cancelled)
	return ctx.Err()
}

func (provider *fakeRuntimeProvider) Disconnect(context.Context) error {
	if provider.disconnectCalled != nil {
		provider.disconnectOnce.Do(func() { close(provider.disconnectCalled) })
	}
	return nil
}

type manualTimer struct {
	ch      chan time.Time
	created chan struct{}
	once    sync.Once
	stopped atomic.Bool
}

func newManualTimer() *manualTimer {
	return &manualTimer{ch: make(chan time.Time, 1), created: make(chan struct{}, 1)}
}
func (timer *manualTimer) C() <-chan time.Time {
	timer.once.Do(func() { timer.created <- struct{}{} })
	return timer.ch
}
func (timer *manualTimer) Stop() bool               { return timer.stopped.CompareAndSwap(false, true) }
func (timer *manualTimer) Fire()                    { timer.ch <- time.Now() }
func (timer *manualTimer) Created() <-chan struct{} { return timer.created }

func receive(t *testing.T, channel <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func receiveValue[T any](t *testing.T, channel <-chan T, name string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		var zero T
		return zero
	}
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
