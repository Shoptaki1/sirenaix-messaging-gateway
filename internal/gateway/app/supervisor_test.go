package app_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/app"
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
)

type starvationLanes struct {
	lanes []messaging.LaneKey
}

type rotatingStarvationLanes struct {
	lanes []messaging.LaneKey
}

func (store rotatingStarvationLanes) ListQueuedDispatchLanes(context.Context, domain.TenantID, int) ([]messaging.LaneKey, error) {
	return nil, nil
}

func (store rotatingStarvationLanes) ListQueuedDispatchLanesAfter(_ context.Context, _ domain.TenantID, after messaging.LaneKey, limit int) ([]messaging.LaneKey, error) {
	start := 0
	if after.ConnectionID != "" {
		for index, lane := range store.lanes {
			if lane.ConnectionID == after.ConnectionID && lane.ConversationID == after.ConversationID {
				start = index + 1
				break
			}
		}
	}
	end := min(len(store.lanes), start+limit)
	return append([]messaging.LaneKey(nil), store.lanes[start:end]...), nil
}

type selectivelyReadyActors struct {
	supervisorActors
	healthy domain.ConnectionID
}

func (actors *selectivelyReadyActors) Ready(_ context.Context, key connectionactor.Key) bool {
	return key.ConnectionID == actors.healthy
}

func TestSupervisorRotatingCursorPassesMoreThanSixtyFourUnavailableLanes(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	lanes := make([]messaging.LaneKey, 0, 65)
	for index := 0; index < 64; index++ {
		lanes = append(lanes, messaging.LaneKey{TenantID: tenant, ConnectionID: domain.ConnectionID(fmt.Sprintf("connection-%03d", index)), ConversationID: "conversation-a"})
	}
	healthy := messaging.LaneKey{TenantID: tenant, ConnectionID: "connection-healthy", ConversationID: "conversation-z"}
	lanes = append(lanes, healthy)
	dispatcher := &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)}
	actors := &selectivelyReadyActors{healthy: healthy.ConnectionID}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, Connections: supervisorConnections{}, Actors: actors,
		Lanes: rotatingStarvationLanes{lanes: lanes}, Dispatcher: dispatcher,
		LaneLimit: 64, PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	select {
	case got := <-dispatcher.called:
		if got != healthy {
			t.Fatalf("dispatched lane = %+v, want healthy lane", got)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy lane behind 64 unavailable lanes starved")
	}
	cancel()
	<-done
}

func (store starvationLanes) ListQueuedDispatchLanes(context.Context, domain.TenantID, int) ([]messaging.LaneKey, error) {
	return append([]messaging.LaneKey(nil), store.lanes...), nil
}

type starvationDispatcher struct {
	slowStarted chan struct{}
	healthyDone chan struct{}
	onceSlow    sync.Once
	onceHealthy sync.Once
}

func (dispatcher *starvationDispatcher) DispatchLane(ctx context.Context, lane messaging.LaneKey) (bool, error) {
	if lane.ConnectionID == "connection-slow" {
		dispatcher.onceSlow.Do(func() { close(dispatcher.slowStarted) })
		<-ctx.Done()
		return true, ctx.Err()
	}
	dispatcher.onceHealthy.Do(func() { close(dispatcher.healthyDone) })
	return true, nil
}

func TestSupervisorSlowOrFailingLaneCannotStarveHealthyConnection(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	dispatcher := &starvationDispatcher{slowStarted: make(chan struct{}), healthyDone: make(chan struct{})}
	reported := make(chan error, 8)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond, DispatchConcurrency: 2,
		Connections: supervisorConnections{}, Actors: &supervisorActors{},
		Lanes: starvationLanes{lanes: []messaging.LaneKey{
			{TenantID: tenant, ConnectionID: "connection-slow", ConversationID: "conversation-a"},
			{TenantID: tenant, ConnectionID: "connection-healthy", ConversationID: "conversation-b"},
		}},
		Dispatcher: dispatcher, OnError: func(err error) {
			if !errors.Is(err, context.Canceled) {
				select {
				case reported <- err:
				default:
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	select {
	case <-dispatcher.slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow lane did not start")
	}
	select {
	case <-dispatcher.healthyDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("healthy connection was blocked behind slow lane")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not join slow dispatch on cancellation")
	}
}

type supervisorConnections struct{ records []postgres.ConnectionRecord }

func (store supervisorConnections) ListConnectionsPage(_ context.Context, _ domain.TenantID, after domain.ConnectionID, limit int) (postgres.ConnectionPage, error) {
	start := 0
	if after != "" {
		for index, record := range store.records {
			if record.Connection.ID == after {
				start = index + 1
				break
			}
		}
	}
	end := min(len(store.records), start+limit+1)
	records := append([]postgres.ConnectionRecord(nil), store.records[start:end]...)
	page := postgres.ConnectionPage{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		page.NextCursor = page.Records[len(page.Records)-1].Connection.ID
	}
	return page, nil
}

type failingConnections struct{ err error }

func (store failingConnections) ListConnectionsPage(context.Context, domain.TenantID, domain.ConnectionID, int) (postgres.ConnectionPage, error) {
	return postgres.ConnectionPage{}, store.err
}

type recordingPagedConnections struct {
	mu       sync.Mutex
	records  map[domain.TenantID][]postgres.ConnectionRecord
	maxLimit int
	calls    int
}

func (store *recordingPagedConnections) ListConnectionsPage(_ context.Context, tenantID domain.TenantID, after domain.ConnectionID, limit int) (postgres.ConnectionPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	store.maxLimit = max(store.maxLimit, limit)
	records := store.records[tenantID]
	start := 0
	if after != "" {
		for index, record := range records {
			if record.Connection.ID == after {
				start = index + 1
				break
			}
		}
	}
	end := min(len(records), start+limit+1)
	page := postgres.ConnectionPage{Records: append([]postgres.ConnectionRecord(nil), records[start:end]...)}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
		page.NextCursor = page.Records[len(page.Records)-1].Connection.ID
	}
	return page, nil
}

type multiTenantPagedLanes struct{}

func (multiTenantPagedLanes) ListQueuedDispatchLanes(context.Context, domain.TenantID, int) ([]messaging.LaneKey, error) {
	return nil, nil
}

func (multiTenantPagedLanes) ListQueuedDispatchLanesAfter(_ context.Context, tenantID domain.TenantID, _ messaging.LaneKey, _ int) ([]messaging.LaneKey, error) {
	return []messaging.LaneKey{{TenantID: tenantID, ConnectionID: domain.ConnectionID("connection-" + tenantID), ConversationID: "conversation-a"}}, nil
}

func TestSupervisorUsesBoundedConnectionPagesAndSynchronizesMultiTenantCursors(t *testing.T) {
	tenants := make([]domain.TenantID, 12)
	records := make(map[domain.TenantID][]postgres.ConnectionRecord, len(tenants))
	backfill := make(map[domain.TenantID]app.ConnectionWorker, len(tenants))
	for tenantIndex := range tenants {
		tenantID := domain.TenantID(fmt.Sprintf("tenant-%02d", tenantIndex))
		tenants[tenantIndex] = tenantID
		for connectionIndex := 0; connectionIndex < 80; connectionIndex++ {
			records[tenantID] = append(records[tenantID], postgres.ConnectionRecord{Connection: domain.Connection{
				ID: domain.ConnectionID(fmt.Sprintf("connection-%03d", connectionIndex)), TenantID: tenantID, State: domain.ConnectionStateConnected,
			}})
		}
		backfill[tenantID] = supervisorConnectionWorker{called: make(chan connectionactor.Key, 256)}
	}
	connections := &recordingPagedConnections{records: records}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: tenants, Connections: connections, ConnectionLimit: 17, BackfillLimit: 7,
		Actors: &supervisorActors{}, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 256)},
		Backfill: backfill, BackfillQuarantine: &recordingBackfillQuarantine{}, PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err = supervisor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	connections.mu.Lock()
	defer connections.mu.Unlock()
	if connections.maxLimit > 17 || connections.calls <= len(tenants) {
		t.Fatalf("bounded page calls=%d max_limit=%d", connections.calls, connections.maxLimit)
	}
}

func TestSupervisorEscalatesPersistentInfrastructureFailure(t *testing.T) {
	databaseErr := errors.New("database unavailable")
	actors := &supervisorActors{}
	reported := make(chan error, 4)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{"tenant-a"}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: failingConnections{err: databaseErr}, Actors: actors,
		Lanes: &supervisorLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		OnError: func(err error) { reported <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if runErr := supervisor.Run(ctx); !errors.Is(runErr, databaseErr) {
		t.Fatalf("Run() = %v, want persistent database failure", runErr)
	}
	if len(reported) < 2 {
		t.Fatalf("reported failures = %d, want threshold evidence", len(reported))
	}
	actors.mu.Lock()
	defer actors.mu.Unlock()
	if !actors.shutdown {
		t.Fatal("fatal worker escalation did not join actors")
	}
}

type supervisorActors struct {
	mu          sync.Mutex
	started     []connectionactor.Key
	quarantined []connectionactor.Key
	shutdown    bool
}

func (actors *supervisorActors) Start(_ context.Context, key connectionactor.Key) error {
	actors.mu.Lock()
	defer actors.mu.Unlock()
	actors.started = append(actors.started, key)
	return nil
}
func (*supervisorActors) Ready(context.Context, connectionactor.Key) bool { return true }
func (actors *supervisorActors) Quarantine(_ context.Context, key connectionactor.Key) error {
	actors.mu.Lock()
	defer actors.mu.Unlock()
	actors.quarantined = append(actors.quarantined, key)
	return nil
}
func (actors *supervisorActors) Shutdown(context.Context) error {
	actors.mu.Lock()
	defer actors.mu.Unlock()
	actors.shutdown = true
	return nil
}

type supervisorLanes struct {
	mu    sync.Mutex
	lane  messaging.LaneKey
	calls int
}

func (store *supervisorLanes) ListQueuedDispatchLanes(context.Context, domain.TenantID, int) ([]messaging.LaneKey, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	if store.calls == 1 {
		return []messaging.LaneKey{store.lane}, nil
	}
	return nil, nil
}

type supervisorDispatcher struct {
	called chan messaging.LaneKey
}

func (dispatcher *supervisorDispatcher) DispatchLane(_ context.Context, lane messaging.LaneKey) (bool, error) {
	select {
	case dispatcher.called <- lane:
	default:
	}
	return true, nil
}

type supervisorOneWorker struct {
	mu    sync.Mutex
	calls int
}

func (worker *supervisorOneWorker) RunOne(context.Context) (bool, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	worker.calls++
	return worker.calls == 1, nil
}

type supervisorBatchWorker struct{ called chan struct{} }

func (worker supervisorBatchWorker) RunBatch(context.Context) error {
	select {
	case worker.called <- struct{}{}:
	default:
	}
	return nil
}

type supervisorConnectionWorker struct{ called chan connectionactor.Key }

func (worker supervisorConnectionWorker) RunConnection(_ context.Context, key connectionactor.Key) (bool, error) {
	select {
	case worker.called <- key:
	default:
	}
	return true, nil
}

type isolatingBackfillWorker struct {
	failure error
	healthy chan struct{}
}

type recordingBackfillQuarantine struct {
	mu   sync.Mutex
	keys []connectionactor.Key
}

func (store *recordingBackfillQuarantine) QuarantineBackfillConnection(_ context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, safeReason string) error {
	if safeReason != "provider-protocol" {
		return fmt.Errorf("unsafe quarantine reason %q", safeReason)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.keys = append(store.keys, connectionactor.Key{TenantID: tenantID, ConnectionID: connectionID})
	return nil
}

type tenantBackfillWorker struct {
	failing connectionactor.Key
	err     error
	healthy chan connectionactor.Key
}

func (worker tenantBackfillWorker) RunConnection(_ context.Context, key connectionactor.Key) (bool, error) {
	if key == worker.failing {
		return false, worker.err
	}
	select {
	case worker.healthy <- key:
	default:
	}
	return true, nil
}

func TestSupervisorQuarantinesPersistentConnectionFailureWithoutStoppingOtherTenant(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-bad"), domain.TenantID("tenant-good")
	badKey := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-bad"}
	goodKey := connectionactor.Key{TenantID: goodTenant, ConnectionID: "connection-good"}
	records := map[domain.TenantID][]postgres.ConnectionRecord{
		badTenant:  {{Connection: domain.Connection{ID: badKey.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
		goodTenant: {{Connection: domain.Connection{ID: goodKey.ConnectionID, TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
	}
	healthy := make(chan connectionactor.Key, 8)
	quarantine := &recordingBackfillQuarantine{}
	poison := fmt.Errorf("%w: attachment identity conflict", messaging.ErrBackfillPoisoned)
	worker := tenantBackfillWorker{failing: badKey, err: poison, healthy: healthy}
	actors := &supervisorActors{}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: records}, Actors: actors,
		Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 8)},
		Backfill:           map[domain.TenantID]app.ConnectionWorker{badTenant: worker, goodTenant: worker},
		BackfillQuarantine: quarantine, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	deadline := time.After(time.Second)
	for {
		quarantine.mu.Lock()
		quarantined := len(quarantine.keys) == 1
		quarantine.mu.Unlock()
		if quarantined {
			break
		}
		select {
		case <-deadline:
			t.Fatal("persistent provider poison was not quarantined")
		case <-time.After(10 * time.Millisecond):
		}
	}
	select {
	case got := <-healthy:
		if got != goodKey {
			t.Fatalf("healthy worker key = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy tenant stopped after another connection was quarantined")
	}
	select {
	case runErr := <-done:
		t.Fatalf("connection-local poison stopped gateway: %v", runErr)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("graceful stop after quarantine = %v", runErr)
	}
	actors.mu.Lock()
	defer actors.mu.Unlock()
	if len(actors.quarantined) != 1 || actors.quarantined[0] != badKey {
		t.Fatalf("actor quarantines = %+v", actors.quarantined)
	}
}

func TestSupervisorQuarantinesPersistentProviderFailureWithoutStoppingGateway(t *testing.T) {
	tenant := domain.TenantID("tenant-provider")
	key := connectionactor.Key{TenantID: tenant, ConnectionID: "connection-offline"}
	quarantine := &recordingBackfillQuarantine{}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: supervisorConnections{records: []postgres.ConnectionRecord{{Connection: domain.Connection{
			ID: key.ConnectionID, TenantID: tenant, State: domain.ConnectionStateConnected,
		}}}},
		Actors: &supervisorActors{}, Lanes: &supervisorLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill:           map[domain.TenantID]app.ConnectionWorker{tenant: tenantBackfillWorker{failing: key, err: messaging.ErrBackfillProviderUnavailable, healthy: make(chan connectionactor.Key, 1)}},
		BackfillQuarantine: quarantine, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	deadline := time.After(time.Second)
	for {
		quarantine.mu.Lock()
		quarantined := len(quarantine.keys) == 1
		quarantine.mu.Unlock()
		if quarantined {
			break
		}
		select {
		case <-deadline:
			t.Fatal("persistent provider failure was not quarantined")
		case <-time.After(10 * time.Millisecond):
		}
	}
	select {
	case runErr := <-done:
		t.Fatalf("provider-local failure stopped gateway: %v", runErr)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("graceful stop after provider quarantine = %v", runErr)
	}
}

func TestSupervisorEscalatesDurablePersistenceFailureWithoutQuarantiningTenant(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-db-bad"), domain.TenantID("tenant-db-good")
	badKey := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-db-bad"}
	goodKey := connectionactor.Key{TenantID: goodTenant, ConnectionID: "connection-db-good"}
	quarantine := &recordingBackfillQuarantine{}
	healthy := make(chan connectionactor.Key, 8)
	durableFailure := errors.Join(connectionactor.ErrSharedInfrastructure, libgm.ErrDurablePersistence, errors.New("tenant transaction unavailable"))
	worker := tenantBackfillWorker{failing: badKey, err: durableFailure, healthy: healthy}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
			badTenant:  {{Connection: domain.Connection{ID: badKey.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
			goodTenant: {{Connection: domain.Connection{ID: goodKey.ConnectionID, TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
		}},
		Actors: &supervisorActors{}, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 8)},
		Backfill: map[domain.TenantID]app.ConnectionWorker{badTenant: worker, goodTenant: worker}, BackfillQuarantine: quarantine, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background()) }()
	select {
	case got := <-healthy:
		if got != goodKey {
			t.Fatalf("healthy tenant key = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy tenant did not progress before infrastructure escalation")
	}
	select {
	case runErr := <-done:
		if !errors.Is(runErr, libgm.ErrDurablePersistence) || !errors.Is(runErr, connectionactor.ErrSharedInfrastructure) {
			t.Fatalf("durable infrastructure escalation = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("durable persistence failure did not reach process escalation")
	}
	quarantine.mu.Lock()
	defer quarantine.mu.Unlock()
	if len(quarantine.keys) != 0 {
		t.Fatalf("shared durable failure quarantined connections = %+v", quarantine.keys)
	}
}

func TestSupervisorBackfillOnlyTypedSharedFailureCanStopOtherTenants(t *testing.T) {
	for name, fixture := range map[string]struct {
		failure        error
		wantFatal      error
		wantQuarantine bool
	}{
		"provider conflict": {
			failure: errors.Join(messaging.ErrBackfillPoisoned, ingress.ErrConflictingEnvelope), wantQuarantine: true,
		},
		"durable fence": {
			failure: errors.Join(connectionactor.ErrStaleGeneration, postgres.ErrConnectionLeaseLost),
		},
		"typed shared infrastructure": {
			failure:   errors.Join(connectionactor.ErrSharedInfrastructure, libgm.ErrDurablePersistence),
			wantFatal: connectionactor.ErrSharedInfrastructure,
		},
	} {
		t.Run(name, func(t *testing.T) {
			badTenant, goodTenant := domain.TenantID("tenant-bad"), domain.TenantID("tenant-good")
			badKey := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-bad"}
			goodKey := connectionactor.Key{TenantID: goodTenant, ConnectionID: "connection-good"}
			healthy := make(chan connectionactor.Key, 16)
			quarantine := &recordingBackfillQuarantine{}
			worker := tenantBackfillWorker{failing: badKey, err: fixture.failure, healthy: healthy}
			supervisor, err := app.NewSupervisor(app.SupervisorConfig{
				Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
				Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
					badTenant:  {{Connection: domain.Connection{ID: badKey.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
					goodTenant: {{Connection: domain.Connection{ID: goodKey.ConnectionID, TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
				}},
				Actors: &supervisorActors{}, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
				Backfill: map[domain.TenantID]app.ConnectionWorker{badTenant: worker, goodTenant: worker}, BackfillQuarantine: quarantine, OnError: func(error) {},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- supervisor.Run(ctx) }()
			select {
			case got := <-healthy:
				if got != goodKey {
					t.Fatalf("healthy key = %+v", got)
				}
			case <-time.After(time.Second):
				cancel()
				t.Fatal("healthy tenant did not progress")
			}
			if fixture.wantFatal != nil {
				select {
				case runErr := <-done:
					if !errors.Is(runErr, fixture.wantFatal) {
						t.Fatalf("fatal error = %v", runErr)
					}
				case <-time.After(time.Second):
					cancel()
					t.Fatal("typed shared failure did not escalate")
				}
			} else {
				deadline := time.Now().Add(150 * time.Millisecond)
				for fixture.wantQuarantine {
					quarantine.mu.Lock()
					count := len(quarantine.keys)
					quarantine.mu.Unlock()
					if count == 1 {
						break
					}
					if time.Now().After(deadline) {
						cancel()
						t.Fatal("provider-local conflict was not quarantined")
					}
					time.Sleep(time.Millisecond)
				}
				select {
				case runErr := <-done:
					cancel()
					t.Fatalf("connection-local backfill error stopped gateway: %v", runErr)
				case <-time.After(50 * time.Millisecond):
				}
				cancel()
				if runErr := <-done; runErr != nil {
					t.Fatalf("graceful shutdown = %v", runErr)
				}
			}
			quarantine.mu.Lock()
			gotQuarantine := len(quarantine.keys) > 0
			quarantine.mu.Unlock()
			if gotQuarantine != fixture.wantQuarantine {
				t.Fatalf("quarantined = %v, want %v", gotQuarantine, fixture.wantQuarantine)
			}
		})
	}
}

func TestSupervisorBackfillStartStaleGenerationNeverCountsTowardFatalThreshold(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-stale-backfill"), domain.TenantID("tenant-healthy-backfill")
	stale := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-stale"}
	actors := &staleGenerationActors{stale: stale, healthy: make(chan struct{}, 8)}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
			badTenant:  {{Connection: domain.Connection{ID: stale.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
			goodTenant: {{Connection: domain.Connection{ID: "connection-healthy", TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
		}},
		Actors: actors, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill: map[domain.TenantID]app.ConnectionWorker{
			badTenant: supervisorConnectionWorker{called: make(chan connectionactor.Key, 1)}, goodTenant: supervisorConnectionWorker{called: make(chan connectionactor.Key, 8)},
		}, BackfillQuarantine: &recordingBackfillQuarantine{}, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	deadline := time.After(time.Second)
	for actors.staleCalls.Load() < 3 || actors.healthyCalls.Load() == 0 {
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("backfill Start stale generation became fatal: %v", runErr)
		case <-deadline:
			cancel()
			t.Fatal("backfill stale/healthy paths did not run")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("graceful shutdown = %v", runErr)
	}
}

type sharedFailureActors struct {
	supervisorActors
	bad     connectionactor.Key
	healthy atomic.Int32
}

type permanentProtocolActors struct {
	supervisorActors
	bad           connectionactor.Key
	badStarts     atomic.Int32
	healthyStarts atomic.Int32
}

func (actors *permanentProtocolActors) Start(_ context.Context, key connectionactor.Key) error {
	if key == actors.bad {
		actors.badStarts.Add(1)
		return connectionactor.ErrProviderPermanentProtocol
	}
	actors.healthyStarts.Add(1)
	return nil
}

func TestSupervisorPermanentActorTombstoneIsConnectionLocalAcrossDiscoveryPolls(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-protocol-bad"), domain.TenantID("tenant-protocol-good")
	bad := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-protocol-bad"}
	actors := &permanentProtocolActors{bad: bad}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
			badTenant:  {{Connection: domain.Connection{ID: bad.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateDegraded}}},
			goodTenant: {{Connection: domain.Connection{ID: "connection-protocol-good", TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
		}},
		Actors: actors, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for actors.badStarts.Load() < 3 || actors.healthyStarts.Load() == 0 {
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("permanent provider protocol stopped gateway: %v", runErr)
		case <-time.After(time.Millisecond):
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("discovery did not keep healthy tenant progressing")
			}
		}
	}
	cancel()
	if runErr := <-done; runErr != nil {
		t.Fatalf("graceful stop after connection-local tombstone = %v", runErr)
	}
}

func (actors *sharedFailureActors) Start(_ context.Context, key connectionactor.Key) error {
	if key == actors.bad {
		return errors.Join(connectionactor.ErrSharedInfrastructure, libgm.ErrDurablePersistence)
	}
	actors.healthy.Add(1)
	return nil
}

func TestSupervisorEscalatesUnsolicitedActorDurableFailureWithoutStoppingHealthyDiscoveryEarly(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-bad"), domain.TenantID("tenant-good")
	bad := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-z-bad"}
	actors := &sharedFailureActors{bad: bad}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
			badTenant:  {{Connection: domain.Connection{ID: bad.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
			goodTenant: {{Connection: domain.Connection{ID: "connection-a-healthy", TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
		}},
		Actors: actors, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)}, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background()) }()
	select {
	case runErr := <-done:
		if !errors.Is(runErr, connectionactor.ErrSharedInfrastructure) || !errors.Is(runErr, libgm.ErrDurablePersistence) {
			t.Fatalf("unsolicited durable escalation = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("shared actor failure did not reach process escalation")
	}
	if actors.healthy.Load() == 0 {
		t.Fatal("healthy connection was not discovered before shared escalation")
	}
}

type staleGenerationActors struct {
	supervisorActors
	stale        connectionactor.Key
	staleCalls   atomic.Int32
	healthyCalls atomic.Int32
	healthy      chan struct{}
}

func (actors *staleGenerationActors) Start(_ context.Context, key connectionactor.Key) error {
	if key == actors.stale {
		actors.staleCalls.Add(1)
		return connectionactor.ErrStaleGeneration
	}
	actors.healthyCalls.Add(1)
	select {
	case actors.healthy <- struct{}{}:
	default:
	}
	return nil
}

func TestSupervisorStaleGenerationDoesNotBlockHealthyTenantOrBecomeProcessFatal(t *testing.T) {
	badTenant, goodTenant := domain.TenantID("tenant-stale"), domain.TenantID("tenant-good")
	stale := connectionactor.Key{TenantID: badTenant, ConnectionID: "connection-stale"}
	healthy := make(chan struct{}, 1)
	actors := &staleGenerationActors{stale: stale, healthy: healthy}
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{badTenant, goodTenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: &recordingPagedConnections{records: map[domain.TenantID][]postgres.ConnectionRecord{
			badTenant:  {{Connection: domain.Connection{ID: stale.ConnectionID, TenantID: badTenant, State: domain.ConnectionStateConnected}}},
			goodTenant: {{Connection: domain.Connection{ID: "connection-healthy", TenantID: goodTenant, State: domain.ConnectionStateConnected}}},
		}},
		Actors: actors, Lanes: multiTenantPagedLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)}, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	deadline := time.After(time.Second)
	for actors.staleCalls.Load() < 3 || actors.healthyCalls.Load() == 0 {
		select {
		case runErr := <-done:
			cancel()
			t.Fatalf("stale generation became process fatal: %v", runErr)
		case <-healthy:
		case <-time.After(time.Millisecond):
		case <-deadline:
			cancel()
			t.Fatalf("stale/healthy discovery calls = %d/%d", actors.staleCalls.Load(), actors.healthyCalls.Load())
		}
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("stale generation stopped gateway: %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("supervisor did not join after cancellation")
	}
}

func (worker isolatingBackfillWorker) RunConnection(_ context.Context, key connectionactor.Key) (bool, error) {
	if key.ConnectionID == "connection-failing" {
		return false, worker.failure
	}
	select {
	case worker.healthy <- struct{}{}:
	default:
	}
	return true, nil
}

func TestSupervisorBackfillIsolatesItemsAndEscalatesPersistentFailure(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	databaseErr := errors.New("cursor database unavailable")
	sharedFailure := errors.Join(connectionactor.ErrSharedInfrastructure, databaseErr)
	healthy := make(chan struct{}, 1)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2,
		Connections: supervisorConnections{records: []postgres.ConnectionRecord{
			{Connection: domain.Connection{ID: "connection-failing", TenantID: tenant, State: domain.ConnectionStateConnected}},
			{Connection: domain.Connection{ID: "connection-healthy", TenantID: tenant, State: domain.ConnectionStateConnected}},
		}},
		Actors: &supervisorActors{}, Lanes: &supervisorLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill:           map[domain.TenantID]app.ConnectionWorker{tenant: isolatingBackfillWorker{failure: sharedFailure, healthy: healthy}},
		BackfillQuarantine: &recordingBackfillQuarantine{}, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- supervisor.Run(context.Background()) }()
	select {
	case <-healthy:
	case <-time.After(time.Second):
		t.Fatal("failing backfill item blocked a healthy connection")
	}
	select {
	case runErr := <-runDone:
		if !errors.Is(runErr, databaseErr) {
			t.Fatalf("Run() = %v, want persistent backfill failure", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("persistent backfill failure was logged but never escalated")
	}
}

func TestSupervisorBackfillFailurePersistsAcrossRotatingHealthyBatches(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	databaseErr := errors.New("persistent checkpoint failure")
	sharedFailure := errors.Join(connectionactor.ErrSharedInfrastructure, databaseErr)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond, FatalErrorThreshold: 2, BackfillLimit: 1,
		Connections: supervisorConnections{records: []postgres.ConnectionRecord{
			{Connection: domain.Connection{ID: "connection-failing", TenantID: tenant, State: domain.ConnectionStateConnected}},
			{Connection: domain.Connection{ID: "connection-healthy-a", TenantID: tenant, State: domain.ConnectionStateConnected}},
			{Connection: domain.Connection{ID: "connection-healthy-b", TenantID: tenant, State: domain.ConnectionStateConnected}},
		}},
		Actors: &supervisorActors{}, Lanes: &supervisorLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill:           map[domain.TenantID]app.ConnectionWorker{tenant: isolatingBackfillWorker{failure: sharedFailure, healthy: make(chan struct{}, 1)}},
		BackfillQuarantine: &recordingBackfillQuarantine{}, OnError: func(error) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background()) }()
	select {
	case runErr := <-done:
		if !errors.Is(runErr, databaseErr) {
			t.Fatalf("Run() = %v, want persistent per-connection failure", runErr)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("rotating healthy batches reset the broken connection failure forever")
	}
}

func TestSupervisorRunsBackfillThroughReadyConnectionActor(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	key := connectionactor.Key{TenantID: tenant, ConnectionID: "connection-a"}
	backfilled := make(chan connectionactor.Key, 1)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond,
		Connections: supervisorConnections{records: []postgres.ConnectionRecord{{Connection: domain.Connection{
			ID: key.ConnectionID, TenantID: tenant, State: domain.ConnectionStateConnected,
		}}}},
		Actors: &supervisorActors{}, Lanes: &supervisorLanes{}, Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill:           map[domain.TenantID]app.ConnectionWorker{tenant: supervisorConnectionWorker{called: backfilled}},
		BackfillQuarantine: &recordingBackfillQuarantine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	select {
	case got := <-backfilled:
		if got != key {
			t.Fatalf("backfilled key = %+v, want %+v", got, key)
		}
	case <-time.After(time.Second):
		t.Fatal("actor-owned backfill worker did not run")
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorRunsRecoveryPipelinesAndJoinsActors(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	key := connectionactor.Key{TenantID: tenant, ConnectionID: "connection-a"}
	actors := &supervisorActors{}
	dispatcher := &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)}
	mediaWorker := &supervisorOneWorker{}
	webhooksCalled, kafkaCalled := make(chan struct{}, 1), make(chan struct{}, 1)
	supervisor, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: 10 * time.Millisecond,
		Connections: supervisorConnections{records: []postgres.ConnectionRecord{{Connection: domain.Connection{
			ID: key.ConnectionID, TenantID: tenant, State: domain.ConnectionStateConnected,
		}}}},
		Actors:     actors,
		Lanes:      &supervisorLanes{lane: messaging.LaneKey{TenantID: tenant, ConnectionID: key.ConnectionID, ConversationID: "conversation-a"}},
		Dispatcher: dispatcher,
		Media:      map[domain.TenantID]app.OneWorker{tenant: mediaWorker},
		Webhooks:   map[domain.TenantID]app.BatchWorker{tenant: supervisorBatchWorker{called: webhooksCalled}},
		Kafka:      map[domain.TenantID]app.BatchWorker{tenant: supervisorBatchWorker{called: kafkaCalled}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	select {
	case lane := <-dispatcher.called:
		if lane.TenantID != tenant || lane.ConnectionID != key.ConnectionID {
			t.Fatalf("dispatched lane = %+v", lane)
		}
	case <-time.After(time.Second):
		t.Fatal("queued lane was not dispatched")
	}
	for name, called := range map[string]<-chan struct{}{"webhook": webhooksCalled, "kafka": kafkaCalled} {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatalf("%s worker did not run", name)
		}
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("supervisor did not join")
	}
	actors.mu.Lock()
	defer actors.mu.Unlock()
	if len(actors.started) == 0 || actors.started[0] != key || !actors.shutdown {
		t.Fatalf("actor lifecycle = started %+v shutdown=%v", actors.started, actors.shutdown)
	}
	mediaWorker.mu.Lock()
	defer mediaWorker.mu.Unlock()
	if mediaWorker.calls < 2 {
		t.Fatalf("media RunOne calls = %d, want drain until empty", mediaWorker.calls)
	}
}

func TestSupervisorRejectsMissingTenantTrustRootsAndUnboundedConfiguration(t *testing.T) {
	for _, config := range []app.SupervisorConfig{
		{},
		{Tenants: []domain.TenantID{"tenant-a"}, PollInterval: time.Millisecond},
		{Tenants: []domain.TenantID{"tenant-a", "tenant-a"}, PollInterval: time.Second},
	} {
		if _, err := app.NewSupervisor(config); err == nil {
			t.Fatalf("NewSupervisor(%+v) unexpectedly succeeded", config)
		}
	}
}

func TestSupervisorRejectsBackfillWithoutDurableConnectionQuarantine(t *testing.T) {
	tenant := domain.TenantID("tenant-a")
	_, err := app.NewSupervisor(app.SupervisorConfig{
		Tenants: []domain.TenantID{tenant}, PollInterval: time.Second,
		Connections: supervisorConnections{}, Actors: &supervisorActors{}, Lanes: &supervisorLanes{},
		Dispatcher: &supervisorDispatcher{called: make(chan messaging.LaneKey, 1)},
		Backfill:   map[domain.TenantID]app.ConnectionWorker{tenant: supervisorConnectionWorker{called: make(chan connectionactor.Key, 1)}},
	})
	if !errors.Is(err, app.ErrInvalidSupervisor) {
		t.Fatalf("backfill without durable quarantine error = %v", err)
	}
}
