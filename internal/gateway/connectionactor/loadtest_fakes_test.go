//go:build loadtest

package connectionactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type loadLease struct {
	owner string
	token uint64
}

type loadStore struct {
	mu sync.Mutex

	leases          map[Key]loadLease
	nextToken       map[Key]uint64
	readyWrites     map[Key]int
	totalReady      int
	totalRenewals   int
	totalReleases   int
	totalContention int
	maxLiveLeases   int
	changed         chan struct{}
}

func newLoadStore() *loadStore {
	return &loadStore{
		leases:      make(map[Key]loadLease, loadActorCount),
		nextToken:   make(map[Key]uint64, loadActorCount),
		readyWrites: make(map[Key]int, loadActorCount),
		changed:     make(chan struct{}, 1),
	}
}

func (store *loadStore) GetConnection(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (domain.Connection, error) {
	if err := ctx.Err(); err != nil {
		return domain.Connection{}, err
	}
	return domain.Connection{ID: connectionID, TenantID: tenantID, Name: "Load phone", State: domain.ConnectionStateConnected}, nil
}

func (store *loadStore) AcquireConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, owner string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl != loadLeaseTTL {
		return Lease{}, false, errors.New("unexpected lease TTL")
	}
	key := Key{TenantID: tenantID, ConnectionID: connectionID}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.leases[key]; exists {
		store.totalContention++
		loadNotify(store.changed)
		return Lease{}, false, nil
	}
	store.nextToken[key]++
	lease := loadLease{owner: owner, token: store.nextToken[key]}
	store.leases[key] = lease
	if len(store.leases) > store.maxLiveLeases {
		store.maxLiveLeases = len(store.leases)
	}
	loadNotify(store.changed)
	return Lease{OwnerID: owner, FencingToken: lease.token, ExpiresAt: time.Now().Add(ttl)}, true, nil
}

func (store *loadStore) RenewConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, owner string, token uint64, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := Key{TenantID: tenantID, ConnectionID: connectionID}
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, exists := store.leases[key]
	if !exists || lease.owner != owner || lease.token != token || ttl != loadLeaseTTL {
		return false, nil
	}
	store.totalRenewals++
	loadNotify(store.changed)
	return true, nil
}

func (store *loadStore) ReleaseConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, owner string, token uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := Key{TenantID: tenantID, ConnectionID: connectionID}
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, exists := store.leases[key]
	if !exists || lease.owner != owner || lease.token != token {
		return false, nil
	}
	delete(store.leases, key)
	store.totalReleases++
	loadNotify(store.changed)
	return true, nil
}

func (store *loadStore) WriteConnectionHealthFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, owner string, token uint64, health Health) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := Key{TenantID: tenantID, ConnectionID: connectionID}
	store.mu.Lock()
	defer store.mu.Unlock()
	lease, exists := store.leases[key]
	if !exists || lease.owner != owner || lease.token != token || health.FencingToken != token {
		return false, nil
	}
	if health.ActorState == "ready" {
		store.readyWrites[key]++
		store.totalReady++
	}
	loadNotify(store.changed)
	return true, nil
}

func (store *loadStore) MarkReauthorizationRequiredFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64) (bool, error) {
	return false, errors.New("unexpected reauthorization in load simulation")
}

func (store *loadStore) waitEveryReady(ctx context.Context, keys []Key, count int) error {
	return loadWait(ctx, store.changed, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		for _, key := range keys {
			if store.readyWrites[key] < count {
				return false
			}
		}
		return true
	})
}

func (store *loadStore) waitRenewals(ctx context.Context, count int) error {
	return loadWait(ctx, store.changed, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.totalRenewals >= count
	})
}

func (store *loadStore) activeLeases() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.leases)
}

func (store *loadStore) renewals() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.totalRenewals
}

func (store *loadStore) leaseContentions() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.totalContention
}

func (store *loadStore) maxLeases() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.maxLiveLeases
}

func (store *loadStore) releases() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.totalReleases
}

type loadSessionStore struct{}

func (loadSessionStore) LoadVersioned(ctx context.Context, _ Key) (SessionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return SessionSnapshot{}, err
	}
	return SessionSnapshot{Plaintext: []byte("load-session"), Revision: 1}, nil
}

func (loadSessionStore) CompareAndSwapFenced(context.Context, Key, string, uint64, uint64, []byte) (bool, error) {
	return true, nil
}

type loadProviderFactory struct {
	mu sync.Mutex

	current         map[Key]*loadProvider
	generations     map[Key]int
	active          map[Key]int
	maxActive       map[Key]int
	totalGeneration int
	totalActive     int
	overlapCount    int
}

func newLoadProviderFactory() *loadProviderFactory {
	return &loadProviderFactory{
		current:     make(map[Key]*loadProvider, loadActorCount),
		generations: make(map[Key]int, loadActorCount),
		active:      make(map[Key]int, loadActorCount),
		maxActive:   make(map[Key]int, loadActorCount),
	}
}

func (factory *loadProviderFactory) Restore(ctx context.Context, plaintext []byte, hooks Hooks) (Provider, error) {
	ownership, ok := ProviderOwnershipFromContext(ctx)
	if !ok || string(plaintext) != "load-session" {
		return nil, errors.New("invalid load provider restore")
	}
	factory.mu.Lock()
	defer factory.mu.Unlock()
	factory.generations[ownership.Key]++
	factory.totalGeneration++
	provider := &loadProvider{
		factory:    factory,
		key:        ownership.Key,
		generation: factory.generations[ownership.Key],
		hooks:      hooks,
		fail:       make(chan struct{}, 1),
	}
	factory.current[ownership.Key] = provider
	return provider, nil
}

func (factory *loadProviderFactory) connect(key Key) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	factory.active[key]++
	factory.totalActive++
	if factory.active[key] > factory.maxActive[key] {
		factory.maxActive[key] = factory.active[key]
	}
	if factory.active[key] > 1 {
		factory.overlapCount++
	}
}

func (factory *loadProviderFactory) disconnect(key Key) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	factory.active[key]--
	factory.totalActive--
}

func (factory *loadProviderFactory) failTransient(key Key) error {
	factory.mu.Lock()
	provider := factory.current[key]
	factory.mu.Unlock()
	if provider == nil || provider.generation != 1 {
		return errors.New("no first provider generation to fail")
	}
	select {
	case provider.fail <- struct{}{}:
		return nil
	default:
		return errors.New("provider failure already requested")
	}
}

func (factory *loadProviderFactory) totalGenerations() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.totalGeneration
}

func (factory *loadProviderFactory) activeConnections() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.totalActive
}

func (factory *loadProviderFactory) maxActiveGeneration() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	maximum := 0
	for _, count := range factory.maxActive {
		if count > maximum {
			maximum = count
		}
	}
	return maximum
}

func (factory *loadProviderFactory) overlaps() int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.overlapCount
}

type loadProvider struct {
	factory    *loadProviderFactory
	key        Key
	generation int
	hooks      Hooks
	fail       chan struct{}
}

func (provider *loadProvider) Connect(ctx context.Context) error {
	provider.factory.connect(provider.key)
	defer provider.factory.disconnect(provider.key)
	provider.hooks.Ready()
	select {
	case <-provider.fail:
		return ErrProviderTransient
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*loadProvider) Disconnect(context.Context) error { return nil }

type loadTimerKind uint8

const (
	loadTimerRenew loadTimerKind = iota + 1
	loadTimerBackoff
	loadTimerDebounce
)

type loadTimerRecord struct {
	delay time.Duration
	timer *loadManualTimer
}

type loadTimerHub struct {
	mu sync.Mutex

	records map[loadTimerKind]map[Key][]loadTimerRecord
	totals  map[loadTimerKind]int
	changed chan struct{}
}

func newLoadTimerHub() *loadTimerHub {
	return &loadTimerHub{
		records: make(map[loadTimerKind]map[Key][]loadTimerRecord),
		totals:  make(map[loadTimerKind]int),
		changed: make(chan struct{}, 1),
	}
}

func (hub *loadTimerHub) newTimer(kind loadTimerKind, key Key, delay time.Duration) Timer {
	timer := &loadManualTimer{channel: make(chan time.Time, 1)}
	hub.mu.Lock()
	byKey := hub.records[kind]
	if byKey == nil {
		byKey = make(map[Key][]loadTimerRecord)
		hub.records[kind] = byKey
	}
	byKey[key] = append(byKey[key], loadTimerRecord{delay: delay, timer: timer})
	hub.totals[kind]++
	hub.mu.Unlock()
	loadNotify(hub.changed)
	return timer
}

func (hub *loadTimerHub) waitTotal(ctx context.Context, kind loadTimerKind, count int) error {
	return loadWait(ctx, hub.changed, func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.totals[kind] >= count
	})
}

func (hub *loadTimerHub) record(kind loadTimerKind, key Key, index int) (loadTimerRecord, error) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	records := hub.records[kind][key]
	if index < 0 || index >= len(records) {
		return loadTimerRecord{}, errors.New("timer record not found")
	}
	return records[index], nil
}

func (hub *loadTimerHub) fire(kind loadTimerKind, key Key, index int) error {
	record, err := hub.record(kind, key, index)
	if err != nil {
		return err
	}
	return record.timer.fire()
}

func (hub *loadTimerHub) delay(kind loadTimerKind, key Key, index int) (time.Duration, error) {
	record, err := hub.record(kind, key, index)
	return record.delay, err
}

type loadManualTimer struct {
	channel chan time.Time
	fired   atomic.Bool
	stopped atomic.Bool
}

func (timer *loadManualTimer) C() <-chan time.Time { return timer.channel }

func (timer *loadManualTimer) Stop() bool {
	return timer.stopped.CompareAndSwap(false, true)
}

func (timer *loadManualTimer) fire() error {
	if timer.stopped.Load() {
		return errors.New("timer already stopped")
	}
	if !timer.fired.CompareAndSwap(false, true) {
		return errors.New("timer already fired")
	}
	timer.channel <- time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	return nil
}

var _ Store = (*loadStore)(nil)
var _ SessionStore = loadSessionStore{}
var _ ProviderFactory = (*loadProviderFactory)(nil)
var _ Provider = (*loadProvider)(nil)
