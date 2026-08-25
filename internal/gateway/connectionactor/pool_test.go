package connectionactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type timedOutPoolActor struct{ started chan struct{} }

func (actor *timedOutPoolActor) Run(context.Context, Key) error {
	close(actor.started)
	return ErrProviderJoinTimeout
}
func (*timedOutPoolActor) Execute(context.Context, Key, ProviderOperation) error {
	return ErrProviderUnavailable
}

func TestPoolTombstonesConnectionAfterProviderJoinTimeout(t *testing.T) {
	var creations atomic.Int32
	firstStarted := make(chan struct{})
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		if creations.Add(1) == 1 {
			return &timedOutPoolActor{started: firstStarted}, nil
		}
		return &timedOutPoolActor{started: make(chan struct{})}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	deadline := time.Now().Add(time.Second)
	for {
		err = pool.Start(context.Background(), key)
		if errors.Is(err, ErrProviderJoinTimeout) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(err, ErrProviderJoinTimeout) || creations.Load() != 1 {
		t.Fatalf("replacement Start() = %v, creations=%d; timed-out provider must remain quarantined", err, creations.Load())
	}
	if err = pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil }); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("tombstoned Execute() = %v", err)
	}
}

type poolActor struct {
	started chan struct{}
	done    chan struct{}
	calls   atomic.Int32
}

func (actor *poolActor) Run(ctx context.Context, _ Key) error {
	close(actor.started)
	<-ctx.Done()
	close(actor.done)
	return ctx.Err()
}

func (actor *poolActor) Execute(ctx context.Context, _ Key, operation ProviderOperation) error {
	actor.calls.Add(1)
	return operation(ctx, nil)
}

func TestPoolStartsOneActorPerKeyRoutesExecutionAndJoinsShutdown(t *testing.T) {
	var mu sync.Mutex
	created := make(map[Key][]*poolActor)
	pool, err := NewPool(PoolConfig{NewActor: func(key Key) (RunnerExecutor, error) {
		actor := &poolActor{started: make(chan struct{}), done: make(chan struct{})}
		mu.Lock()
		created[key] = append(created[key], actor)
		mu.Unlock()
		return actor, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err = pool.Start(ctx, key); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	actors := append([]*poolActor(nil), created[key]...)
	mu.Unlock()
	if len(actors) != 1 {
		t.Fatalf("actors created = %d, want 1", len(actors))
	}
	select {
	case <-actors[0].started:
	case <-time.After(time.Second):
		t.Fatal("actor did not start")
	}
	called := false
	if err = pool.Execute(ctx, key, func(context.Context, Provider) error { called = true; return nil }); err != nil || !called {
		t.Fatalf("Execute() = %v, called=%v", err, called)
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err = pool.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-actors[0].done:
	default:
		t.Fatal("Shutdown returned before actor joined")
	}
	if err = pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil }); err != ErrProviderUnavailable {
		t.Fatalf("Execute after shutdown = %v, want unavailable", err)
	}
}

func TestPoolDoesNotLetOldGenerationDeleteReplacement(t *testing.T) {
	first := &poolActor{started: make(chan struct{}), done: make(chan struct{})}
	second := &poolActor{started: make(chan struct{}), done: make(chan struct{})}
	var created atomic.Int32
	pool, _ := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		if created.Add(1) == 1 {
			return first, nil
		}
		return second, nil
	}})
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	firstCtx, stopFirst := context.WithCancel(context.Background())
	if err := pool.Start(firstCtx, key); err != nil {
		t.Fatal(err)
	}
	<-first.started
	stopFirst()
	<-first.done
	deadline := time.Now().Add(time.Second)
	for pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil }) == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	if err := pool.Start(secondCtx, key); err != nil {
		t.Fatal(err)
	}
	<-second.started
	if err := pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil }); err != nil {
		t.Fatalf("replacement Execute() = %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

type quarantinePoolActor struct {
	started chan struct{}
	stopped chan struct{}
	entered chan struct{}
	blocked chan struct{}
}

func (actor *quarantinePoolActor) Run(ctx context.Context, _ Key) error {
	close(actor.started)
	<-ctx.Done()
	close(actor.stopped)
	return ctx.Err()
}

func (actor *quarantinePoolActor) Execute(_ context.Context, _ Key, operation ProviderOperation) error {
	close(actor.entered)
	select {
	case <-actor.stopped:
		return ErrProviderUnavailable
	case <-actor.blocked:
		return operation(context.Background(), nil)
	}
}

func TestPoolQuarantineSynchronouslyStopsAndTombstonesOnlyTargetConnection(t *testing.T) {
	actors := make(map[Key]*quarantinePoolActor)
	var mu sync.Mutex
	pool, err := NewPool(PoolConfig{NewActor: func(key Key) (RunnerExecutor, error) {
		actor := &quarantinePoolActor{started: make(chan struct{}), stopped: make(chan struct{}), entered: make(chan struct{}), blocked: make(chan struct{})}
		mu.Lock()
		actors[key] = actor
		mu.Unlock()
		return actor, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bad := Key{TenantID: "tenant-a", ConnectionID: "connection-bad"}
	good := Key{TenantID: "tenant-a", ConnectionID: "connection-good"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, key := range []Key{bad, good} {
		if err = pool.Start(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	badActor, goodActor := actors[bad], actors[good]
	mu.Unlock()
	<-badActor.started
	<-goodActor.started
	operationDone := make(chan error, 1)
	go func() {
		operationDone <- pool.Execute(context.Background(), bad, func(context.Context, Provider) error { return nil })
	}()
	<-badActor.entered
	quarantineCtx, stopQuarantine := context.WithTimeout(context.Background(), time.Second)
	defer stopQuarantine()
	if err = pool.Quarantine(quarantineCtx, bad); err != nil {
		t.Fatal(err)
	}
	if executeErr := <-operationDone; !errors.Is(executeErr, ErrProviderUnavailable) {
		t.Fatalf("in-flight Execute() after quarantine = %v", executeErr)
	}
	if err = pool.Execute(context.Background(), bad, func(context.Context, Provider) error { return nil }); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("new Execute() after quarantine = %v", err)
	}
	if err = pool.Start(context.Background(), bad); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("replacement Start() after quarantine = %v", err)
	}
	close(goodActor.blocked)
	if err = pool.Execute(context.Background(), good, func(context.Context, Provider) error { return nil }); err != nil {
		t.Fatalf("healthy actor Execute() = %v", err)
	}
}

type sharedFailurePoolActor struct{ release chan struct{} }

func (actor *sharedFailurePoolActor) Run(context.Context, Key) error {
	<-actor.release
	return errors.Join(ErrSharedInfrastructure, errors.New("durable database unavailable"))
}
func (*sharedFailurePoolActor) Execute(context.Context, Key, ProviderOperation) error {
	return ErrProviderUnavailable
}

func TestPoolTombstonesSharedInfrastructureFailureForSupervisorEscalation(t *testing.T) {
	release := make(chan struct{})
	var creations atomic.Int32
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		creations.Add(1)
		return &sharedFailurePoolActor{release: release}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		err = pool.Start(context.Background(), key)
		if errors.Is(err, ErrSharedInfrastructure) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(err, ErrSharedInfrastructure) || creations.Load() != 1 {
		t.Fatalf("shared failure Start() = %v, creations=%d", err, creations.Load())
	}
}

func TestPoolShutdownAfterQuarantiningAbsentActorDoesNotPanic(t *testing.T) {
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		t.Fatal("absent quarantine must not create an actor")
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-absent"}
	if err = pool.Quarantine(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err = pool.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type quarantineJoinTimeoutActor struct{ started chan struct{} }

func (actor *quarantineJoinTimeoutActor) Run(ctx context.Context, _ Key) error {
	close(actor.started)
	<-ctx.Done()
	return ErrProviderJoinTimeout
}

func (*quarantineJoinTimeoutActor) Execute(context.Context, Key, ProviderOperation) error {
	return ErrProviderUnavailable
}

func TestPoolQuarantineCannotResumeGenerationWithUnknownProviderJoin(t *testing.T) {
	var creations atomic.Int32
	actor := &quarantineJoinTimeoutActor{started: make(chan struct{})}
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		creations.Add(1)
		return actor, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	<-actor.started
	if err = pool.Quarantine(context.Background(), key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Quarantine() = %v, want unresolved provider join timeout", err)
	}
	if err = pool.Resume(key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Resume() = %v, want process-lifetime join-timeout tombstone", err)
	}
	if err = pool.Start(context.Background(), key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("replacement Start() = %v", err)
	}
	if creations.Load() != 1 {
		t.Fatalf("actor creations = %d, want one", creations.Load())
	}
}

func TestPoolQuarantinePreservesJoinTimeoutFromAlreadyCompletedGeneration(t *testing.T) {
	var creations atomic.Int32
	actor := &timedOutPoolActor{started: make(chan struct{})}
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		creations.Add(1)
		return actor, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	<-actor.started
	deadline := time.Now().Add(time.Second)
	for {
		err = pool.Start(context.Background(), key)
		if errors.Is(err, ErrProviderJoinTimeout) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("completed generation Start() = %v, want join-timeout tombstone", err)
	}
	if err = pool.Quarantine(context.Background(), key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Quarantine() = %v, must preserve existing join-timeout tombstone", err)
	}
	if err = pool.Resume(key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("Resume() = %v, want process-lifetime join-timeout tombstone", err)
	}
	if err = pool.Start(context.Background(), key); !errors.Is(err, ErrProviderJoinTimeout) {
		t.Fatalf("replacement Start() = %v", err)
	}
	if creations.Load() != 1 {
		t.Fatalf("actor creations = %d, want one", creations.Load())
	}
}

type permanentProtocolActor struct{ stopped chan struct{} }

func (actor *permanentProtocolActor) Run(context.Context, Key) error {
	close(actor.stopped)
	return ErrProviderPermanentProtocol
}
func (*permanentProtocolActor) Execute(context.Context, Key, ProviderOperation) error {
	return ErrProviderUnavailable
}

func TestPoolPermanentProtocolGenerationIsNotReadmittedUntilExplicitResume(t *testing.T) {
	var creations atomic.Int32
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) {
		creations.Add(1)
		return &permanentProtocolActor{stopped: make(chan struct{})}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if err = pool.Start(context.Background(), key); errors.Is(err, ErrProviderPermanentProtocol) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("permanent provider result was not retained: last Start() = %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	for range 3 {
		if err = pool.Start(context.Background(), key); !errors.Is(err, ErrProviderPermanentProtocol) {
			t.Fatalf("rediscovery Start() = %v, want permanent tombstone", err)
		}
	}
	if creations.Load() != 1 {
		t.Fatalf("provider generations = %d, want one before operator resume", creations.Load())
	}
	if err = pool.Resume(key); err != nil {
		t.Fatalf("explicit Resume() = %v", err)
	}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatalf("Start() after explicit resume = %v", err)
	}
	if creations.Load() != 2 {
		t.Fatalf("provider generations after resume = %d, want two", creations.Load())
	}
}

type racingQuarantineActor struct {
	started chan struct{}
	stopped chan struct{}
	calls   atomic.Int64
}

func (actor *racingQuarantineActor) Run(ctx context.Context, _ Key) error {
	close(actor.started)
	<-ctx.Done()
	close(actor.stopped)
	return ctx.Err()
}

func (actor *racingQuarantineActor) Execute(context.Context, Key, ProviderOperation) error {
	select {
	case <-actor.stopped:
		return ErrProviderUnavailable
	default:
		actor.calls.Add(1)
		return nil
	}
}

func TestPoolConcurrentExecuteQuarantineHasNoPostQuarantineAdmission(t *testing.T) {
	actor := &racingQuarantineActor{started: make(chan struct{}), stopped: make(chan struct{})}
	pool, err := NewPool(PoolConfig{NewActor: func(Key) (RunnerExecutor, error) { return actor, nil }})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{TenantID: "tenant-a", ConnectionID: "connection-a"}
	if err = pool.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	<-actor.started
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				_ = pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil })
			}
		}()
	}
	close(start)
	if err = pool.Quarantine(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	afterQuarantine := actor.calls.Load()
	for range 100 {
		if err = pool.Execute(context.Background(), key, func(context.Context, Provider) error { return nil }); !errors.Is(err, ErrProviderUnavailable) {
			t.Fatalf("Execute() after quarantine = %v", err)
		}
	}
	if calls := actor.calls.Load(); calls != afterQuarantine {
		t.Fatalf("provider calls advanced after quarantine: %d -> %d", afterQuarantine, calls)
	}
}
