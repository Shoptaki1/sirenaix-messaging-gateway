package connectionactor

import (
	"context"
	"errors"
	"sync"
)

// RunnerExecutor is one connection-scoped actor generation. Pool keeps actor
// lookup and lifecycle bounded while Actor continues to own lease fencing and
// all provider I/O.
type RunnerExecutor interface {
	Run(context.Context, Key) error
	ProviderExecutor
}

type PoolConfig struct {
	NewActor func(Key) (RunnerExecutor, error)
}

type poolEntry struct {
	actor       RunnerExecutor
	cancel      context.CancelFunc
	done        chan struct{}
	terminalErr error
	quarantined bool
}

// Pool routes provider operations only to the currently running actor for a
// tenant connection. It never provides a direct provider fallback.
type Pool struct {
	newActor func(Key) (RunnerExecutor, error)

	mu     sync.RWMutex
	actors map[Key]*poolEntry
	closed bool
}

func NewPool(config PoolConfig) (*Pool, error) {
	if config.NewActor == nil {
		return nil, ErrInvalidConfig
	}
	return &Pool{newActor: config.NewActor, actors: make(map[Key]*poolEntry)}, nil
}

func (pool *Pool) Start(ctx context.Context, key Key) error {
	if ctx == nil || ctx.Err() != nil || !key.valid() {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrInvalidConfig
	}
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return ErrProviderUnavailable
	}
	if existing := pool.actors[key]; existing != nil {
		terminalErr := existing.terminalErr
		pool.mu.Unlock()
		if terminalErr != nil {
			return terminalErr
		}
		return nil
	}
	actor, err := pool.newActor(key)
	if err != nil || actor == nil {
		pool.mu.Unlock()
		if err == nil {
			err = ErrInvalidConfig
		}
		return err
	}
	actorCtx, cancel := context.WithCancel(ctx)
	entry := &poolEntry{actor: actor, cancel: cancel, done: make(chan struct{})}
	pool.actors[key] = entry
	pool.mu.Unlock()

	go func() {
		runErr := actor.Run(actorCtx, key)
		pool.mu.Lock()
		if pool.actors[key] == entry {
			if errors.Is(runErr, ErrProviderJoinTimeout) {
				// Actor.Run has bounded its own teardown, but a provider callback
				// may still be executing. Keep a process-lifetime tombstone so a
				// replacement generation can never overlap that unknown operation.
				entry.terminalErr = ErrProviderJoinTimeout
			} else if errors.Is(runErr, ErrSharedInfrastructure) || errors.Is(runErr, ErrStaleGeneration) || errors.Is(runErr, ErrProviderPermanentProtocol) {
				// Shared persistence/KMS corruption and an explicitly stale fence
				// must remain observable to the supervisor; neither generation may
				// silently restart and resume provider I/O.
				entry.terminalErr = runErr
				if errors.Is(runErr, ErrProviderPermanentProtocol) {
					entry.quarantined = true
				}
			} else if !entry.quarantined {
				delete(pool.actors, key)
			}
		}
		close(entry.done)
		pool.mu.Unlock()
	}()
	return nil
}

func (pool *Pool) Execute(ctx context.Context, key Key, operation ProviderOperation) error {
	if ctx == nil || operation == nil || !key.valid() {
		return ErrInvalidConfig
	}
	pool.mu.RLock()
	entry := pool.actors[key]
	if entry == nil || entry.quarantined || entry.terminalErr != nil || entry.actor == nil {
		pool.mu.RUnlock()
		return ErrProviderUnavailable
	}
	actor, done := entry.actor, entry.done
	pool.mu.RUnlock()
	select {
	case <-done:
		return ErrProviderUnavailable
	default:
		return actor.Execute(ctx, key, operation)
	}
}

// Quarantine synchronously removes a connection generation from admission,
// cancels it, and waits for its bounded actor teardown. The tombstone is kept
// even if teardown times out, so a replacement can never overlap the old
// generation. Resumption is an explicit operator action.
func (pool *Pool) Quarantine(ctx context.Context, key Key) error {
	if ctx == nil || !key.valid() {
		return ErrInvalidConfig
	}
	pool.mu.Lock()
	entry := pool.actors[key]
	if entry == nil {
		done := make(chan struct{})
		close(done)
		pool.actors[key] = &poolEntry{done: done, terminalErr: ErrProviderUnavailable, quarantined: true}
		pool.mu.Unlock()
		return nil
	}
	entry.quarantined = true
	// Quarantine is weaker than a terminal actor result. In particular, a
	// provider join timeout is a process-lifetime tombstone: replacing it with
	// a resumable unavailable marker could overlap an operation whose goroutine
	// is still outside our control.
	if entry.terminalErr == nil {
		entry.terminalErr = ErrProviderUnavailable
	}
	if entry.cancel != nil {
		entry.cancel()
	}
	done := entry.done
	pool.mu.Unlock()
	select {
	case <-done:
		pool.mu.RLock()
		terminalErr := entry.terminalErr
		pool.mu.RUnlock()
		if errors.Is(terminalErr, ErrProviderJoinTimeout) {
			return ErrProviderJoinTimeout
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resume clears a joined quarantine tombstone. The caller must separately
// clear durable connection health before starting a new generation.
func (pool *Pool) Resume(key Key) error {
	if !key.valid() {
		return ErrInvalidConfig
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	entry := pool.actors[key]
	if entry == nil || !entry.quarantined {
		return ErrProviderUnavailable
	}
	if errors.Is(entry.terminalErr, ErrProviderJoinTimeout) {
		return ErrProviderJoinTimeout
	}
	select {
	case <-entry.done:
		delete(pool.actors, key)
		return nil
	default:
		return ErrProviderJoinTimeout
	}
}

func (pool *Pool) Ready(ctx context.Context, key Key) bool {
	return pool.Execute(ctx, key, func(context.Context, Provider) error { return nil }) == nil
}

func (pool *Pool) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	pool.mu.Lock()
	pool.closed = true
	entries := make([]*poolEntry, 0, len(pool.actors))
	for _, entry := range pool.actors {
		if entry.cancel != nil {
			entry.cancel()
		}
		entries = append(entries, entry)
	}
	pool.mu.Unlock()

	var result error
	for _, entry := range entries {
		select {
		case <-entry.done:
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		}
	}
	return result
}

var _ ProviderExecutor = (*Pool)(nil)
