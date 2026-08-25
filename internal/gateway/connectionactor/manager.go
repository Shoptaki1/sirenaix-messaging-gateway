package connectionactor

import (
	"context"
	"sync"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type Key struct {
	TenantID     domain.TenantID
	ConnectionID domain.ConnectionID
}

func (key Key) valid() bool { return key.TenantID != "" && key.ConnectionID != "" }

type ManagerConfig struct {
	Run                 func(context.Context, Key)
	beforeCleanup       func(Key)
	beforeStartDecision func(Key)
}

type localActor struct {
	cancel   context.CancelFunc
	exited   chan struct{}
	done     chan struct{}
	stopping bool
}

type Manager struct {
	run                 func(context.Context, Key)
	beforeCleanup       func(Key)
	beforeStartDecision func(Key)

	mu     sync.Mutex
	actors map[Key]*localActor
}

func NewManager(config ManagerConfig) (*Manager, error) {
	if config.Run == nil {
		return nil, ErrInvalidConfig
	}
	return &Manager{
		run: config.Run, beforeCleanup: config.beforeCleanup, beforeStartDecision: config.beforeStartDecision,
		actors: make(map[Key]*localActor),
	}, nil
}

func (manager *Manager) Start(ctx context.Context, key Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !key.valid() {
		return ErrInvalidConfig
	}
	for {
		manager.mu.Lock()
		current := manager.actors[key]
		if current == nil {
			break
		}
		if manager.beforeStartDecision != nil {
			manager.beforeStartDecision(key)
		}
		if !current.stopping {
			select {
			case <-current.exited:
			default:
				manager.mu.Unlock()
				return nil
			}
		}
		done := current.done
		manager.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		manager.mu.Lock()
		if manager.actors[key] == current {
			delete(manager.actors, key)
		}
		manager.mu.Unlock()
	}
	actorCtx, cancel := context.WithCancel(context.Background())
	actor := &localActor{cancel: cancel, exited: make(chan struct{}), done: make(chan struct{})}
	manager.actors[key] = actor
	manager.mu.Unlock()
	go func() {
		manager.run(actorCtx, key)
		close(actor.exited)
		if manager.beforeCleanup != nil {
			manager.beforeCleanup(key)
		}
		manager.mu.Lock()
		actor.stopping = true
		if manager.actors[key] == actor {
			delete(manager.actors, key)
		}
		close(actor.done)
		manager.mu.Unlock()
	}()
	return nil
}

func (manager *Manager) Stop(ctx context.Context, key Key) error {
	if !key.valid() {
		return ErrInvalidConfig
	}
	manager.mu.Lock()
	actor := manager.actors[key]
	if actor != nil {
		actor.stopping = true
		actor.cancel()
	}
	manager.mu.Unlock()
	if actor == nil {
		return nil
	}
	select {
	case <-actor.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
