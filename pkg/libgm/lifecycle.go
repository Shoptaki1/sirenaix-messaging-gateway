package libgm

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
)

const (
	defaultLongPollRetryBase = 2 * time.Second
	defaultLongPollRetryCap  = 2 * time.Minute
	lifecycleActivityBuffer  = 64
)

var ErrInvalidLifecyclePolicy = errors.New("invalid libgm lifecycle policy")

type lifecycleTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realLifecycleTimer struct{ timer *time.Timer }

func (timer realLifecycleTimer) C() <-chan time.Time { return timer.timer.C }
func (timer realLifecycleTimer) Stop() bool          { return timer.timer.Stop() }

type lifecycleTicker interface {
	C() <-chan time.Time
	Stop()
}

type realLifecycleTicker struct{ ticker *time.Ticker }

func (ticker realLifecycleTicker) C() <-chan time.Time { return ticker.ticker.C }
func (ticker realLifecycleTicker) Stop()               { ticker.ticker.Stop() }

type LongPollRetryPolicy struct {
	Base     time.Duration
	Cap      time.Duration
	Int63n   func(int64) int64
	NewTimer func(time.Duration) lifecycleTimer
}

func defaultLongPollRetryPolicy() LongPollRetryPolicy {
	return LongPollRetryPolicy{
		Base:   defaultLongPollRetryBase,
		Cap:    defaultLongPollRetryCap,
		Int63n: secureInt63n,
		NewTimer: func(after time.Duration) lifecycleTimer {
			return realLifecycleTimer{timer: time.NewTimer(after)}
		},
	}
}

func normalizeLongPollRetryPolicy(policy LongPollRetryPolicy) (LongPollRetryPolicy, error) {
	defaults := defaultLongPollRetryPolicy()
	if policy.Base == 0 {
		policy.Base = defaults.Base
	}
	if policy.Cap == 0 {
		policy.Cap = defaults.Cap
	}
	if policy.Int63n == nil {
		policy.Int63n = defaults.Int63n
	}
	if policy.NewTimer == nil {
		policy.NewTimer = defaults.NewTimer
	}
	if policy.Base <= 0 || policy.Cap < policy.Base {
		return LongPollRetryPolicy{}, ErrInvalidLifecyclePolicy
	}
	return policy, nil
}

func (c *Client) SetLongPollRetryPolicy(policy LongPollRetryPolicy) error {
	normalized, err := normalizeLongPollRetryPolicy(policy)
	if err != nil {
		return err
	}
	c.retryPolicyMu.Lock()
	c.longPollRetry = normalized
	c.retryPolicyMu.Unlock()
	return nil
}

func (c *Client) waitLongPollRetry(ctx context.Context, failures uint) bool {
	c.retryPolicyMu.RLock()
	policy := c.longPollRetry
	c.retryPolicyMu.RUnlock()
	limit := policy.Base
	for exponent := uint(1); exponent < failures && limit < policy.Cap; exponent++ {
		if limit > policy.Cap/2 {
			limit = policy.Cap
			break
		}
		limit *= 2
	}
	if limit > policy.Cap {
		limit = policy.Cap
	}
	delay := time.Duration(policy.Int63n(int64(limit)))
	timer := policy.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

func secureInt63n(limit int64) int64 {
	if limit <= 1 {
		return 0
	}
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return time.Now().UnixNano() % limit
	}
	return int64(binary.LittleEndian.Uint64(value[:])&(^uint64(0)>>1)) % limit
}

type LifecycleHooks struct {
	OnReady         func()
	OnFrame         func()
	OnPhoneResponse func()
	OnSessionChange func()
}

type lifecycleActivity uint8

const (
	lifecycleActivityReady lifecycleActivity = iota + 1
	lifecycleActivityFrame
	lifecycleActivityPhoneResponse
	lifecycleActivitySessionChange
)

type lifecycleDependencies struct {
	refresh            func(context.Context) error
	longPoll           func(context.Context, func()) bool
	auxLongPoll        func(context.Context, func()) bool
	postConnect        func(context.Context)
	browserPresence    func(context.Context)
	newTimer           func(time.Duration) lifecycleTimer
	newTicker          func(time.Duration) lifecycleTicker
	afterRefresh       func()
	beforeActivityLoop func()
}

type lifecycleState uint8

const (
	lifecycleStarting lifecycleState = iota + 1
	lifecycleRunning
	lifecycleStopping
)

type clientLifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc

	started        chan struct{}
	done           chan struct{}
	startErr       error
	activity       chan lifecycleActivity
	sessionChanged chan struct{}
	sessionMu      sync.Mutex
	acceptSessions bool
	active         atomic.Bool
	wg             sync.WaitGroup
	state          lifecycleState
}

type auxiliaryLifecycle struct {
	ctx            context.Context
	cancel         context.CancelFunc
	ready          chan struct{}
	done           chan struct{}
	reconnect      chan time.Duration
	allowReconnect atomic.Bool
}

func (c *Client) initLifecycle() {
	policy, _ := normalizeLongPollRetryPolicy(LongPollRetryPolicy{})
	c.longPollRetry = policy
	c.lifecycleDeps = lifecycleDependencies{
		refresh:         func(ctx context.Context) error { return c.refreshAuthTokenContext(ctx, nil) },
		longPoll:        func(ctx context.Context, ready func()) bool { return c.doLongPollContext(ctx, true, false, ready) },
		auxLongPoll:     func(ctx context.Context, ready func()) bool { return c.doLongPollContext(ctx, false, false, ready) },
		postConnect:     c.postConnectContext,
		browserPresence: c.ackBrowserPresence,
		newTimer: func(after time.Duration) lifecycleTimer {
			return realLifecycleTimer{timer: time.NewTimer(after)}
		},
		newTicker: func(interval time.Duration) lifecycleTicker {
			return realLifecycleTicker{ticker: time.NewTicker(interval)}
		},
	}
}

func (c *Client) SetLifecycleHooks(hooks LifecycleHooks) {
	c.lifecycleHooksMu.Lock()
	c.lifecycleHooks = hooks
	c.lifecycleHooksMu.Unlock()
}

func (c *Client) ConnectContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.stopAuxiliary(ctx); err != nil {
		return err
	}
	for {
		c.lifecycleMu.Lock()
		if resetDone := c.credentialReset; resetDone != nil {
			c.lifecycleMu.Unlock()
			select {
			case <-resetDone:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		current := c.lifecycle
		if current == nil {
			break
		}
		started, done, state := current.started, current.done, current.state
		c.lifecycleMu.Unlock()
		if state != lifecycleStopping {
			select {
			case <-started:
			case <-ctx.Done():
				return ctx.Err()
			}
			c.lifecycleMu.Lock()
			state = current.state
			startErr := current.startErr
			c.lifecycleMu.Unlock()
			if startErr != nil {
				return startErr
			}
			if state != lifecycleStopping {
				return nil
			}
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.lifecycleMu.Lock()
		if c.lifecycle == current {
			c.lifecycle = nil
		}
		c.lifecycleMu.Unlock()
	}
	generationCtx, cancel := context.WithCancel(ctx)
	generation := &clientLifecycle{
		ctx: generationCtx, cancel: cancel, started: make(chan struct{}), done: make(chan struct{}),
		activity: make(chan lifecycleActivity, lifecycleActivityBuffer), sessionChanged: make(chan struct{}, 1),
		acceptSessions: true, state: lifecycleStarting,
	}
	generation.active.Store(true)
	c.lifecycle = generation
	deps := c.lifecycleDeps
	c.lifecycleMu.Unlock()
	auth := c.AuthData.Snapshot()
	if auth == nil {
		return c.failLifecycleStart(generation, errors.New("no auth token"))
	}
	valid := auth.TachyonAuthToken != nil && auth.Browser != nil
	auth.ClearSecrets()
	if !valid {
		return c.failLifecycleStart(generation, errors.New("not logged in"))
	}

	if err := deps.refresh(generationCtx); err != nil {
		return c.failLifecycleStart(generation, fmt.Errorf("failed to refresh auth token: %w", err))
	}
	if deps.afterRefresh != nil {
		deps.afterRefresh()
	}
	c.lifecycleMu.Lock()
	if c.lifecycle != generation || generation.state == lifecycleStopping || generation.ctx.Err() != nil {
		c.lifecycleMu.Unlock()
		return c.failLifecycleStart(generation, context.Canceled)
	}
	c.lifecycleMu.Unlock()
	c.bumpNextDataReceiveCheck(10 * time.Minute)
	generation.wg.Add(3)
	ready := make(chan struct{})
	var readyOnce sync.Once
	go func() {
		if deps.beforeActivityLoop != nil {
			deps.beforeActivityLoop()
		}
		c.lifecycleActivityLoop(generation)
	}()
	go func() {
		defer generation.wg.Done()
		c.sessionHandler.ackLoop(generation.ctx, deps.newTicker)
	}()
	go func() {
		defer generation.wg.Done()
		deps.longPoll(generation.ctx, func() {
			readyOnce.Do(func() { close(ready) })
			c.emitLifecycleActivity(lifecycleActivityReady)
		})
		c.lifecycleMu.Lock()
		if c.lifecycle == generation {
			generation.state = lifecycleStopping
			generation.active.Store(false)
		}
		c.lifecycleMu.Unlock()
		generation.cancel()
	}()
	if deps.postConnect != nil {
		generation.wg.Add(1)
		go func() {
			defer generation.wg.Done()
			select {
			case <-ready:
				deps.postConnect(generation.ctx)
			case <-generation.ctx.Done():
			}
		}()
	}
	c.lifecycleMu.Lock()
	if c.lifecycle == generation && generation.state == lifecycleStarting {
		generation.state = lifecycleRunning
	}
	c.lifecycleMu.Unlock()
	close(generation.started)
	go func() {
		generation.wg.Wait()
		generation.active.Store(false)
		c.lifecycleMu.Lock()
		generation.state = lifecycleStopping
		if c.lifecycle == generation {
			c.lifecycle = nil
		}
		c.lifecycleMu.Unlock()
		close(generation.done)
	}()
	return nil
}

func (c *Client) failLifecycleStart(generation *clientLifecycle, err error) error {
	generation.startErr = err
	generation.active.Store(false)
	generation.cancel()
	c.finishLifecycleSessions(generation)
	close(generation.started)
	c.lifecycleMu.Lock()
	generation.state = lifecycleStopping
	if c.lifecycle == generation {
		c.lifecycle = nil
	}
	c.lifecycleMu.Unlock()
	close(generation.done)
	return err
}

func (c *Client) beginCredentialReset() chan struct{} {
	for {
		c.lifecycleMu.Lock()
		if current := c.credentialReset; current != nil {
			c.lifecycleMu.Unlock()
			<-current
			continue
		}
		done := make(chan struct{})
		c.credentialReset = done
		c.lifecycleMu.Unlock()
		return done
	}
}

func (c *Client) finishCredentialReset(done chan struct{}) {
	c.lifecycleMu.Lock()
	if c.credentialReset == done {
		c.credentialReset = nil
		close(done)
	}
	c.lifecycleMu.Unlock()
}

func (c *Client) lifecycleActivityLoop(generation *clientLifecycle) {
	defer generation.wg.Done()
	for {
		select {
		case <-generation.ctx.Done():
			c.finishLifecycleSessions(generation)
			return
		case activity := <-generation.activity:
			if generation.ctx.Err() != nil || !generation.active.Load() {
				continue
			}
			c.lifecycleHooksMu.RLock()
			hooks := c.lifecycleHooks
			c.lifecycleHooksMu.RUnlock()
			switch activity {
			case lifecycleActivityReady:
				if hooks.OnReady != nil {
					hooks.OnReady()
				}
			case lifecycleActivityFrame:
				if hooks.OnFrame != nil {
					hooks.OnFrame()
				}
			case lifecycleActivityPhoneResponse:
				if hooks.OnPhoneResponse != nil {
					hooks.OnPhoneResponse()
				}
			}
		case <-generation.sessionChanged:
			c.invokeSessionChangeHook()
		}
	}
}

func (c *Client) finishLifecycleSessions(generation *clientLifecycle) {
	generation.sessionMu.Lock()
	generation.acceptSessions = false
	pending := false
	select {
	case <-generation.sessionChanged:
		pending = true
	default:
	}
	generation.sessionMu.Unlock()
	if pending {
		c.invokeSessionChangeHook()
	}
}

func (c *Client) invokeSessionChangeHook() {
	c.lifecycleHooksMu.RLock()
	hook := c.lifecycleHooks.OnSessionChange
	c.lifecycleHooksMu.RUnlock()
	if hook != nil {
		hook()
	}
}

func (c *Client) emitLifecycleActivity(activity lifecycleActivity) {
	c.lifecycleMu.Lock()
	generation := c.lifecycle
	c.lifecycleMu.Unlock()
	if generation == nil {
		return
	}
	if activity == lifecycleActivitySessionChange {
		generation.sessionMu.Lock()
		defer generation.sessionMu.Unlock()
		if !generation.acceptSessions {
			return
		}
		select {
		case generation.sessionChanged <- struct{}{}:
		default:
		}
		return
	}
	if !generation.active.Load() || generation.ctx.Err() != nil {
		return
	}
	select {
	case generation.activity <- activity:
	default:
	}
}

func (c *Client) startBrowserPresenceACK() bool {
	c.lifecycleMu.Lock()
	generation := c.lifecycle
	if generation == nil || generation.state != lifecycleRunning || !generation.active.Load() || generation.ctx.Err() != nil {
		c.lifecycleMu.Unlock()
		return false
	}
	run := c.lifecycleDeps.browserPresence
	if run == nil {
		run = c.ackBrowserPresence
	}
	generation.wg.Add(1)
	ctx := generation.ctx
	c.lifecycleMu.Unlock()
	go func() {
		defer generation.wg.Done()
		run(ctx)
	}()
	return true
}

func (c *Client) DisconnectContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.lifecycleMu.Lock()
	generation := c.lifecycle
	if generation != nil {
		generation.state = lifecycleStopping
		generation.active.Store(false)
		generation.cancel()
	}
	c.lifecycleMu.Unlock()
	c.auxiliaryMu.Lock()
	auxiliary := c.auxiliary
	if auxiliary != nil {
		auxiliary.allowReconnect.Store(false)
		auxiliary.cancel()
	}
	c.auxiliaryMu.Unlock()
	c.closeLongPolling()
	if generation != nil {
		select {
		case <-generation.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if auxiliary != nil {
		select {
		case <-auxiliary.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.sessionHandler.cancelAllResponseWaiters()
	c.http.CloseIdleConnections()
	return nil
}

func (c *Client) startAuxiliaryLongPoll(parent context.Context) (<-chan struct{}, <-chan struct{}) {
	if parent == nil {
		parent = context.Background()
	}
	c.auxiliaryMu.Lock()
	if current := c.auxiliary; current != nil {
		c.auxiliaryMu.Unlock()
		return current.ready, current.done
	}
	ctx, cancel := context.WithCancel(parent)
	generation := &auxiliaryLifecycle{
		ctx: ctx, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}),
		reconnect: make(chan time.Duration, 1),
	}
	generation.allowReconnect.Store(true)
	c.auxiliary = generation
	deps := c.lifecycleDeps
	c.auxiliaryMu.Unlock()
	go c.runAuxiliaryLongPoll(generation, deps)
	return generation.ready, generation.done
}

func (c *Client) runAuxiliaryLongPoll(generation *auxiliaryLifecycle, deps lifecycleDependencies) {
	auxLongPoll := deps.auxLongPoll
	if auxLongPoll == nil {
		auxLongPoll = func(ctx context.Context, ready func()) bool {
			return c.doLongPollContext(ctx, false, false, ready)
		}
	}
	newTimer := deps.newTimer
	if newTimer == nil {
		newTimer = func(after time.Duration) lifecycleTimer {
			return realLifecycleTimer{timer: time.NewTimer(after)}
		}
	}
	var readyOnce sync.Once
	pollDone := make(chan struct{})
	go func() {
		auxLongPoll(generation.ctx, func() { readyOnce.Do(func() { close(generation.ready) }) })
		close(pollDone)
	}()

	reconnect := false
	select {
	case <-pollDone:
	case delay := <-generation.reconnect:
		pollStopped := false
		timer := newTimer(delay)
		select {
		case <-timer.C():
			reconnect = generation.allowReconnect.Load()
		case <-generation.ctx.Done():
		case <-pollDone:
			pollStopped = true
		}
		timer.Stop()
		if !pollStopped {
			generation.cancel()
			c.closeLongPolling()
			<-pollDone
		}
	case <-generation.ctx.Done():
		c.closeLongPolling()
		<-pollDone
	}

	c.auxiliaryMu.Lock()
	if c.auxiliary == generation {
		c.auxiliary = nil
	}
	c.auxiliaryMu.Unlock()
	close(generation.done)
	if reconnect && generation.allowReconnect.Load() {
		if err := c.Connect(); err != nil {
			c.triggerEvent(&events.ListenFatalError{Error: fmt.Errorf("failed to reconnect after pair success: %w", err)})
		}
	}
}

func (c *Client) requestAuxiliaryReconnect(delay time.Duration) bool {
	c.auxiliaryMu.Lock()
	defer c.auxiliaryMu.Unlock()
	generation := c.auxiliary
	if generation == nil || !generation.allowReconnect.Load() {
		return false
	}
	select {
	case generation.reconnect <- delay:
		return true
	default:
		return false
	}
}

func (c *Client) stopAuxiliary(ctx context.Context) error {
	c.auxiliaryMu.Lock()
	generation := c.auxiliary
	if generation != nil {
		generation.allowReconnect.Store(false)
		generation.cancel()
	}
	c.auxiliaryMu.Unlock()
	if generation == nil {
		return nil
	}
	c.closeLongPolling()
	select {
	case <-generation.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitContext waits for the current lifecycle generation to stop. It does not
// initiate shutdown; callers that own the client should call DisconnectContext.
func (c *Client) WaitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.lifecycleMu.Lock()
	generation := c.lifecycle
	c.lifecycleMu.Unlock()
	if generation == nil {
		return nil
	}
	select {
	case <-generation.done:
		return generation.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) lifecycleRunning() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.lifecycle != nil && (c.lifecycle.state == lifecycleStarting || c.lifecycle.state == lifecycleRunning)
}
