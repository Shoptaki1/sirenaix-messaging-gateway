//go:build loadtest

package connectionactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const (
	loadActorCount           = 1000
	loadReconnectCount       = 100
	loadReconnectWaveSize    = 10
	loadLeaseTTL             = 3 * time.Second
	loadBackoffLimit         = 10 * time.Second
	loadOperationDeadline    = 60 * time.Second
	loadShutdownDeadline     = 60 * time.Second
	loadSimulationDeadline   = 5 * time.Minute
	loadMaxRetainedHeap      = 192 << 20
	loadMaxRetainedWorkers   = 64
	loadBackoffBucket        = time.Second
	loadMaxBackoffsPerBucket = 15
)

// TestSimulated1000Actors exercises the real Pool and Actor lifecycle. Only
// the external provider, persistent store, sessions, and wall-clock timers are
// deterministic in-memory boundaries; no Google or carrier network is used.
func TestSimulated1000Actors(t *testing.T) {
	startedAt := time.Now()
	beforeGoroutines, beforeHeap := loadRuntimeSample()

	runCtx, cancelRun := context.WithTimeout(context.Background(), loadSimulationDeadline)
	defer cancelRun()

	keys := loadKeys()
	store := newLoadStore()
	sessions := loadSessionStore{}
	timers := newLoadTimerHub()
	providers := newLoadProviderFactory()
	fixedNow := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	var actorCreations atomic.Int32

	pool, err := NewPool(PoolConfig{NewActor: func(key Key) (RunnerExecutor, error) {
		index, ok := loadKeyIndex(key)
		if !ok {
			return nil, fmt.Errorf("unexpected load-test key")
		}
		actorCreations.Add(1)
		return NewActor(ActorConfig{
			OwnerID:   "load-replica-1",
			Store:     store,
			Sessions:  sessions,
			Providers: providers,
			LeaseTTL:  loadLeaseTTL,
			Now:       func() time.Time { return fixedNow },
			NewRenewTimer: func(delay time.Duration) Timer {
				return timers.newTimer(loadTimerRenew, key, delay)
			},
			NewBackoffTimer: func(delay time.Duration) Timer {
				return timers.newTimer(loadTimerBackoff, key, delay)
			},
			NewDebounceTimer: func(delay time.Duration) Timer {
				return timers.newTimer(loadTimerDebounce, key, delay)
			},
			Backoff: BackoffConfig{
				Base: loadBackoffLimit,
				Cap:  loadBackoffLimit,
				Int63n: func(limit int64) int64 {
					if limit <= 1 {
						return 0
					}
					// A deterministic prime stride spreads actors through the
					// full-jitter interval without sleeping in the test.
					return (int64(index+1) * int64(104729*time.Millisecond)) % limit
				},
			},
			OperationQueueSize: 4,
			JoinTimeout:        2 * time.Second,
		})
	}})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), loadShutdownDeadline)
			defer cancel()
			_ = pool.Shutdown(ctx)
		})
	}
	t.Cleanup(shutdown)

	if err := loadParallel(keys, func(key Key) error { return pool.Start(runCtx, key) }); err != nil {
		t.Fatalf("start 1000 actors: %v", err)
	}
	if err := store.waitEveryReady(runCtx, keys, 1); err != nil {
		t.Fatalf("wait for 1000 ready actors: %v", err)
	}
	if got := actorCreations.Load(); got != loadActorCount {
		t.Fatalf("actor creations = %d, want %d", got, loadActorCount)
	}

	// Concurrent duplicate Start calls must reuse the current Pool generation.
	if err := loadParallel(keys, func(key Key) error { return pool.Start(runCtx, key) }); err != nil {
		t.Fatalf("duplicate starts: %v", err)
	}
	if got := actorCreations.Load(); got != loadActorCount {
		t.Fatalf("actor creations after duplicate Start = %d, want %d", got, loadActorCount)
	}
	if got := store.leaseContentions(); got != 0 {
		t.Fatalf("lease contentions = %d, want 0 (Pool admitted a duplicate generation)", got)
	}
	if got := store.maxLeases(); got != loadActorCount {
		t.Fatalf("maximum concurrent leases = %d, want %d", got, loadActorCount)
	}

	// Fire the first manual renewal timer for every actor and wait for both the
	// fenced renewal and replacement timer. This is a causal barrier, not a
	// wall-clock sleep.
	if err := timers.waitTotal(runCtx, loadTimerRenew, loadActorCount); err != nil {
		t.Fatalf("wait for initial renewal timers: %v", err)
	}
	for _, key := range keys {
		if err := timers.fire(loadTimerRenew, key, 0); err != nil {
			t.Fatalf("fire renewal timer: %v", err)
		}
	}
	if err := store.waitRenewals(runCtx, loadActorCount); err != nil {
		t.Fatalf("wait for lease renewals: %v", err)
	}
	if err := timers.waitTotal(runCtx, loadTimerRenew, 2*loadActorCount); err != nil {
		t.Fatalf("wait for replacement renewal timers: %v", err)
	}

	// Fail a deterministic subset, inspect the real Actor backoff delays, and
	// release reconnects ten at a time in delay order. Non-fired actors cannot
	// reconnect because their manual timer is the causal gate.
	reconnects := append([]Key(nil), keys[:loadReconnectCount]...)
	for _, key := range reconnects {
		if err := providers.failTransient(key); err != nil {
			t.Fatalf("inject transient provider failure: %v", err)
		}
	}
	if err := timers.waitTotal(runCtx, loadTimerBackoff, loadReconnectCount); err != nil {
		t.Fatalf("wait for reconnect backoffs: %v", err)
	}

	type scheduledReconnect struct {
		key   Key
		delay time.Duration
	}
	schedule := make([]scheduledReconnect, 0, len(reconnects))
	distinctDelays := make(map[time.Duration]struct{}, len(reconnects))
	buckets := make(map[int64]int)
	for _, key := range reconnects {
		delay, err := timers.delay(loadTimerBackoff, key, 0)
		if err != nil {
			t.Fatalf("read reconnect delay: %v", err)
		}
		if delay < 0 || delay >= loadBackoffLimit {
			t.Fatalf("backoff delay = %s, want [0,%s)", delay, loadBackoffLimit)
		}
		schedule = append(schedule, scheduledReconnect{key: key, delay: delay})
		distinctDelays[delay] = struct{}{}
		buckets[int64(delay/loadBackoffBucket)]++
	}
	if len(distinctDelays) < loadReconnectCount/2 {
		t.Fatalf("distinct reconnect delays = %d, want at least %d", len(distinctDelays), loadReconnectCount/2)
	}
	for bucket, count := range buckets {
		if count > loadMaxBackoffsPerBucket {
			t.Fatalf("backoff bucket %d contains %d actors, limit %d", bucket, count, loadMaxBackoffsPerBucket)
		}
	}
	sort.Slice(schedule, func(left, right int) bool { return schedule[left].delay < schedule[right].delay })
	for waveStart := 0; waveStart < len(schedule); waveStart += loadReconnectWaveSize {
		waveEnd := waveStart + loadReconnectWaveSize
		waveKeys := make([]Key, 0, loadReconnectWaveSize)
		for _, reconnect := range schedule[waveStart:waveEnd] {
			if err := timers.fire(loadTimerBackoff, reconnect.key, 0); err != nil {
				t.Fatalf("fire reconnect timer: %v", err)
			}
			waveKeys = append(waveKeys, reconnect.key)
		}
		if err := store.waitEveryReady(runCtx, waveKeys, 2); err != nil {
			t.Fatalf("wait for reconnect wave ending at %d: %v", waveEnd, err)
		}
		if got := providers.totalGenerations(); got != loadActorCount+waveEnd {
			t.Fatalf("provider generations after reconnect wave = %d, want %d", got, loadActorCount+waveEnd)
		}
	}

	operationStarted := time.Now()
	operationCtx, cancelOperations := context.WithTimeout(runCtx, loadOperationDeadline)
	defer cancelOperations()
	var completedOperations atomic.Int32
	if err := loadParallel(keys, func(key Key) error {
		for {
			err := pool.Execute(operationCtx, key, func(ctx context.Context, provider Provider) error {
				ownership, ok := ProviderOwnershipFromContext(ctx)
				if !ok || ownership.Key != key || ownership.OwnerID != "load-replica-1" || ownership.FencingToken == 0 || ownership.LeaseTTL != loadLeaseTTL {
					return errors.New("operation did not receive exact fenced ownership")
				}
				loadProvider, ok := provider.(*loadProvider)
				if !ok || loadProvider.key != key {
					return errors.New("operation was routed to the wrong provider")
				}
				completedOperations.Add(1)
				return nil
			})
			if !errors.Is(err, ErrProviderUnavailable) {
				return err
			}
			if operationCtx.Err() != nil {
				return operationCtx.Err()
			}
			runtime.Gosched()
		}
	}); err != nil {
		t.Fatalf("execute responsive operation on every actor: %v", err)
	}
	operationDuration := time.Since(operationStarted)
	if got := completedOperations.Load(); got != loadActorCount {
		t.Fatalf("completed operations = %d, want %d", got, loadActorCount)
	}
	if operationDuration >= loadOperationDeadline {
		t.Fatalf("1000 operations took %s, deadline %s", operationDuration, loadOperationDeadline)
	}

	shutdownStarted := time.Now()
	var shutdownErr error
	shutdownOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), loadShutdownDeadline)
		defer cancel()
		shutdownErr = pool.Shutdown(ctx)
	})
	if shutdownErr != nil {
		t.Fatalf("Pool.Shutdown() error = %v", shutdownErr)
	}
	shutdownDuration := time.Since(shutdownStarted)
	if got := store.activeLeases(); got != 0 {
		t.Fatalf("active leases after shutdown = %d, want 0", got)
	}
	if got := store.releases(); got != loadActorCount {
		t.Fatalf("released leases = %d, want %d", got, loadActorCount)
	}
	if got := providers.activeConnections(); got != 0 {
		t.Fatalf("active provider workers after shutdown = %d, want 0", got)
	}
	if got := providers.maxActiveGeneration(); got != 1 {
		t.Fatalf("maximum active provider generations per connection = %d, want 1", got)
	}
	if got := providers.overlaps(); got != 0 {
		t.Fatalf("overlapping provider generations = %d, want 0", got)
	}
	if got := store.renewals(); got != loadActorCount {
		t.Fatalf("lease renewals = %d, want %d", got, loadActorCount)
	}

	afterGoroutines, afterHeap := loadRuntimeSample()
	if afterGoroutines > beforeGoroutines+loadMaxRetainedWorkers {
		t.Fatalf("retained goroutines = %d (before %d), allowed delta %d", afterGoroutines, beforeGoroutines, loadMaxRetainedWorkers)
	}
	if afterHeap > beforeHeap+loadMaxRetainedHeap {
		t.Fatalf("retained heap = %d bytes (before %d), allowed growth %d", afterHeap, beforeHeap, loadMaxRetainedHeap)
	}

	duration := time.Since(startedAt)
	summary := loadTestSummary{
		SchemaVersion:                    1,
		Status:                           "passed",
		SourceRevision:                   loadSourceRevision(),
		GOOS:                             runtime.GOOS,
		GOARCH:                           runtime.GOARCH,
		RaceDetector:                     loadRaceEnabled,
		ActorCount:                       loadActorCount,
		ReconnectActorCount:              loadReconnectCount,
		ReconnectWaveSize:                loadReconnectWaveSize,
		LeaseRenewals:                    store.renewals(),
		Operations:                       int(completedOperations.Load()),
		DurationMilliseconds:             duration.Milliseconds(),
		OperationDurationMilliseconds:    operationDuration.Milliseconds(),
		ShutdownDurationMilliseconds:     shutdownDuration.Milliseconds(),
		GoroutinesBefore:                 beforeGoroutines,
		GoroutinesAfter:                  afterGoroutines,
		HeapAllocBeforeBytes:             beforeHeap,
		HeapAllocAfterBytes:              afterHeap,
		HeapGrowthBytes:                  int64(afterHeap) - int64(beforeHeap),
		MaxActiveGenerationPerConnection: providers.maxActiveGeneration(),
		ActiveLeasesAfterShutdown:        store.activeLeases(),
	}
	if err := writeLoadSummary(t, summary); err != nil {
		t.Fatalf("write load-test summary: %v", err)
	}
}

type loadTestSummary struct {
	SchemaVersion                    int    `json:"schema_version"`
	Status                           string `json:"status"`
	SourceRevision                   string `json:"source_revision"`
	GOOS                             string `json:"goos"`
	GOARCH                           string `json:"goarch"`
	RaceDetector                     bool   `json:"race_detector"`
	ActorCount                       int    `json:"actor_count"`
	ReconnectActorCount              int    `json:"reconnect_actor_count"`
	ReconnectWaveSize                int    `json:"reconnect_wave_size"`
	LeaseRenewals                    int    `json:"lease_renewals"`
	Operations                       int    `json:"operations"`
	DurationMilliseconds             int64  `json:"duration_ms"`
	OperationDurationMilliseconds    int64  `json:"operation_duration_ms"`
	ShutdownDurationMilliseconds     int64  `json:"shutdown_duration_ms"`
	GoroutinesBefore                 int    `json:"goroutines_before"`
	GoroutinesAfter                  int    `json:"goroutines_after"`
	HeapAllocBeforeBytes             uint64 `json:"heap_alloc_before_bytes"`
	HeapAllocAfterBytes              uint64 `json:"heap_alloc_after_bytes"`
	HeapGrowthBytes                  int64  `json:"heap_growth_bytes"`
	MaxActiveGenerationPerConnection int    `json:"max_active_generation_per_connection"`
	ActiveLeasesAfterShutdown        int    `json:"active_leases_after_shutdown"`
}

func writeLoadSummary(t *testing.T, summary loadTestSummary) error {
	t.Helper()
	payload, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	t.Logf("SIRENAIX_LOADTEST_SUMMARY=%s", payload)
	path := os.Getenv("SIRENAIX_LOADTEST_SUMMARY_PATH")
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o600)
}

func loadSourceRevision() string {
	revision := os.Getenv("SIRENAIX_LOADTEST_SOURCE_REVISION")
	if (len(revision) != 40 && len(revision) != 64) || !loadHex(revision) {
		return "local"
	}
	return revision
}

func loadHex(value string) bool {
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') && (character < 'A' || character > 'F') {
			return false
		}
	}
	return true
}

func loadRuntimeSample() (int, uint64) {
	runtime.GC()
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return runtime.NumGoroutine(), memory.HeapAlloc
}

func loadKeys() []Key {
	keys := make([]Key, 0, loadActorCount)
	for index := 0; index < loadActorCount; index++ {
		keys = append(keys, Key{
			TenantID:     domain.TenantID(fmt.Sprintf("load-tenant-%02d", index/50)),
			ConnectionID: domain.ConnectionID(fmt.Sprintf("load-connection-%04d", index)),
		})
	}
	return keys
}

func loadKeyIndex(key Key) (int, bool) {
	var index int
	if _, err := fmt.Sscanf(string(key.ConnectionID), "load-connection-%04d", &index); err != nil || index < 0 || index >= loadActorCount {
		return 0, false
	}
	if key.TenantID != domain.TenantID(fmt.Sprintf("load-tenant-%02d", index/50)) {
		return 0, false
	}
	return index, true
}

func loadParallel(keys []Key, operation func(Key) error) error {
	start := make(chan struct{})
	errorsFound := make(chan error, len(keys))
	var workers sync.WaitGroup
	workers.Add(len(keys))
	for _, key := range keys {
		key := key
		go func() {
			defer workers.Done()
			<-start
			if err := operation(key); err != nil {
				errorsFound <- err
			}
		}()
	}
	close(start)
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		return err
	}
	return nil
}

func loadNotify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func loadWait(ctx context.Context, signal <-chan struct{}, ready func() bool) error {
	for {
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signal:
		}
	}
}
