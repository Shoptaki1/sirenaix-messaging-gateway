package libgm

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestConcurrentConnectOwnsOnePollAndACKWorkerAndRestartCreatesOneGeneration(t *testing.T) {
	client := lifecycleTestClient()
	var polls atomic.Int32
	pollStarted := make(chan struct{}, 2)
	tickers := make(chan *manualLifecycleTicker, 2)
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, ready func()) bool {
			polls.Add(1)
			pollStarted <- struct{}{}
			ready()
			<-ctx.Done()
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker {
			ticker := newManualLifecycleTicker()
			tickers <- ticker
			return ticker
		},
	}
	ready := make(chan struct{}, 2)
	client.SetLifecycleHooks(LifecycleHooks{OnReady: func() { ready <- struct{}{} }})

	connectMany(t, client, 32)
	receiveLifecycle(t, pollStarted, "first poll")
	receiveLifecycle(t, tickers, "first ACK ticker")
	receiveLifecycle(t, ready, "first ready hook")
	if got := polls.Load(); got != 1 {
		t.Fatalf("poll generations = %d, want 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("DisconnectContext() error = %v", err)
	}
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("repeated DisconnectContext() error = %v", err)
	}

	connectMany(t, client, 16)
	receiveLifecycle(t, pollStarted, "second poll")
	receiveLifecycle(t, tickers, "second ACK ticker")
	if got := polls.Load(); got != 2 {
		t.Fatalf("poll generations after restart = %d, want 2", got)
	}
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("second generation disconnect = %v", err)
	}
}

func TestDisconnectCancelsLongPollRetryTimerAndWaitsWithoutActiveHTTPConnection(t *testing.T) {
	client := lifecycleTestClient()
	retryTimer := newManualLifecycleTimer()
	retryEntered := make(chan struct{})
	client.SetLongPollRetryPolicy(LongPollRetryPolicy{
		Base: time.Hour, Cap: time.Hour,
		Int63n:   func(int64) int64 { return int64(time.Hour) - 1 },
		NewTimer: func(time.Duration) lifecycleTimer { return retryTimer },
	})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, _ func()) bool {
			close(retryEntered)
			return client.waitLongPollRetry(ctx, 1)
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, retryEntered, "retry wait")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("DisconnectContext() during retry = %v", err)
	}
	if !retryTimer.stopped.Load() {
		t.Fatal("retry timer was not stopped")
	}
}

func TestDisconnectStopsACKTickerAndDropsActivityCallbacks(t *testing.T) {
	client := lifecycleTestClient()
	ticker := newManualLifecycleTicker()
	pollStarted := make(chan struct{})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, _ func()) bool {
			close(pollStarted)
			<-ctx.Done()
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return ticker },
	}
	frames := make(chan struct{}, 1)
	client.SetLifecycleHooks(LifecycleHooks{OnFrame: func() { frames <- struct{}{} }})
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, pollStarted, "poll start")
	client.emitLifecycleActivity(lifecycleActivityFrame)
	receiveLifecycle(t, frames, "frame callback")
	client.Disconnect()
	if !ticker.stopped.Load() {
		t.Fatal("ACK ticker was not stopped")
	}
	if client.sessionHandler.hasACKTicker() {
		t.Fatal("ACK ticker remains attached after disconnect")
	}
	client.emitLifecycleActivity(lifecycleActivityFrame)
	select {
	case <-frames:
		t.Fatal("activity callback ran after disconnect")
	default:
	}
}

func TestClearSessionSecretsDisconnectsWorkersBeforeClearingCredentials(t *testing.T) {
	client := lifecycleTestClient()
	pollExited := make(chan struct{})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, _ func()) bool {
			<-ctx.Done()
			close(pollExited)
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	client.ClearSessionSecrets()
	receiveLifecycle(t, pollExited, "poll cleanup")
	if client.lifecycleRunning() {
		t.Fatal("lifecycle remains active after credential reset")
	}
	auth, push := client.SnapshotSession()
	if auth == nil || auth.Browser != nil || len(auth.TachyonAuthToken) != 0 || push != nil {
		t.Fatalf("credentials remain after reset: auth=%#v push=%#v", auth, push)
	}
}

func TestClearSessionSecretsSerializesCredentialResetAgainstConnect(t *testing.T) {
	client := lifecycleTestClient()
	var refreshes atomic.Int32
	client.lifecycleDeps.refresh = func(context.Context) error {
		refreshes.Add(1)
		return nil
	}
	client.AuthData.sessionLock.Lock()
	clearDone := make(chan struct{})
	go func() {
		client.ClearSessionSecrets()
		close(clearDone)
	}()
	resetClaimed := make(chan struct{})
	go func() {
		for {
			client.lifecycleMu.Lock()
			claimed := client.credentialReset != nil
			client.lifecycleMu.Unlock()
			if claimed {
				close(resetClaimed)
				return
			}
			runtime.Gosched()
		}
	}()
	receiveLifecycle(t, resetClaimed, "credential reset ownership")
	connectDone := make(chan error, 1)
	go func() { connectDone <- client.ConnectContext(context.Background()) }()
	client.AuthData.sessionLock.Unlock()
	receiveLifecycle(t, clearDone, "credential reset completion")
	if err := receiveLifecycle(t, connectDone, "connect after credential reset"); err == nil {
		t.Fatal("Connect succeeded with credentials that were concurrently cleared")
	}
	if got := refreshes.Load(); got != 0 {
		t.Fatalf("refreshes during credential reset = %d, want 0", got)
	}
	if client.lifecycleRunning() {
		t.Fatal("worker generation survived cleared credentials")
	}
}

func TestDisconnectCancelsPairingReconnectDelayAndJoinsAuxiliaryPoll(t *testing.T) {
	client := lifecycleTestClient()
	pollStarted := make(chan struct{})
	pollExited := make(chan struct{})
	reconnectTimer := newManualLifecycleTimer()
	timerStarted := make(chan struct{})
	client.lifecycleDeps.auxLongPoll = func(ctx context.Context, ready func()) bool {
		close(pollStarted)
		ready()
		<-ctx.Done()
		close(pollExited)
		return true
	}
	client.lifecycleDeps.newTimer = func(time.Duration) lifecycleTimer {
		close(timerStarted)
		return reconnectTimer
	}

	client.startAuxiliaryLongPoll(context.Background())
	receiveLifecycle(t, pollStarted, "auxiliary poll start")
	if !client.requestAuxiliaryReconnect(time.Hour) {
		t.Fatal("pairing reconnect request was not accepted")
	}
	receiveLifecycle(t, timerStarted, "pairing reconnect timer")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("DisconnectContext() during pairing reconnect delay = %v", err)
	}
	receiveLifecycle(t, pollExited, "auxiliary poll cleanup")
	if !reconnectTimer.stopped.Load() {
		t.Fatal("pairing reconnect timer was not stopped")
	}
	if client.lifecycleRunning() {
		t.Fatal("main lifecycle started after pairing reconnect cancellation")
	}
}

func TestConnectReplacesFullyStoppedGenerationBeforeReturning(t *testing.T) {
	client := lifecycleTestClient()
	started := make(chan struct{})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, ready func()) bool {
			close(started)
			ready()
			<-ctx.Done()
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	oldStarted, oldDone := make(chan struct{}), make(chan struct{})
	close(oldStarted)
	close(oldDone)
	old := &clientLifecycle{started: oldStarted, done: oldDone, state: lifecycleStopping}
	client.lifecycle = old

	if err := client.ConnectContext(context.Background()); err != nil {
		t.Fatalf("ConnectContext() error = %v", err)
	}
	client.lifecycleMu.Lock()
	current := client.lifecycle
	client.lifecycleMu.Unlock()
	if current == nil || current == old {
		t.Fatal("Connect returned the fully stopped generation")
	}
	receiveLifecycle(t, started, "replacement poll start")
	client.Disconnect()
}

func TestDisconnectCancelsAndJoinsBlockedBrowserPresenceACK(t *testing.T) {
	client := lifecycleTestClient()
	pollStarted := make(chan struct{})
	ackStarted := make(chan struct{})
	ackExited := make(chan struct{})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, ready func()) bool {
			close(pollStarted)
			ready()
			<-ctx.Done()
			return true
		},
		browserPresence: func(ctx context.Context) {
			close(ackStarted)
			<-ctx.Done()
			close(ackExited)
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, pollStarted, "poll start")
	client.startBrowserPresenceACK()
	receiveLifecycle(t, ackStarted, "browser presence ACK start")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.DisconnectContext(ctx); err != nil {
		t.Fatalf("DisconnectContext() error = %v", err)
	}
	receiveLifecycle(t, ackExited, "browser presence ACK cleanup")
}

func TestCredentialResetCancelsAndJoinsBlockedBrowserPresenceACKBeforeClearing(t *testing.T) {
	client := lifecycleTestClient()
	pollStarted := make(chan struct{})
	ackStarted := make(chan struct{})
	ackExited := make(chan struct{})
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, ready func()) bool {
			close(pollStarted)
			ready()
			<-ctx.Done()
			return true
		},
		browserPresence: func(ctx context.Context) {
			close(ackStarted)
			<-ctx.Done()
			close(ackExited)
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, pollStarted, "poll start")
	client.startBrowserPresenceACK()
	receiveLifecycle(t, ackStarted, "browser presence ACK start")
	resetDone := make(chan struct{})
	go func() {
		client.ClearSessionSecrets()
		close(resetDone)
	}()
	receiveLifecycle(t, ackExited, "browser presence ACK reset cleanup")
	receiveLifecycle(t, resetDone, "credential reset completion")
	if client.lifecycleRunning() {
		t.Fatal("credential reset left ACK lifecycle active")
	}
}

func TestConnectPreservesInitialRefreshAuthorizationCause(t *testing.T) {
	client := lifecycleTestClient()
	client.lifecycleDeps.refresh = func(context.Context) error {
		return events.HTTPError{Action: "refreshing authentication", StatusCode: http.StatusUnauthorized, Classification: "authorization"}
	}
	err := client.ConnectContext(context.Background())
	var httpError events.HTTPError
	if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusUnauthorized || httpError.Classification != "authorization" {
		t.Fatalf("ConnectContext() error = %#v, want wrapped authorization HTTPError", err)
	}
}

func TestFailedInitialRefreshDrainsQueuedSessionBeforeDone(t *testing.T) {
	client := lifecycleTestClient()
	refreshErr := errors.New("refresh failed after rotating session")
	var sessionCallbacks atomic.Int32
	client.lifecycleDeps.refresh = func(context.Context) error {
		client.emitLifecycleActivity(lifecycleActivitySessionChange)
		return refreshErr
	}
	client.SetLifecycleHooks(LifecycleHooks{OnSessionChange: func() {
		sessionCallbacks.Add(1)
	}})
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.ConnectContext(context.Background()) }()
	err := receiveLifecycle(t, connectResult, "failed initial refresh completion")
	if !errors.Is(err, refreshErr) {
		t.Fatalf("ConnectContext() error = %v, want wrapped refresh cause", err)
	}
	if got := sessionCallbacks.Load(); got != 1 {
		t.Fatalf("session callbacks before failed generation done = %d, want 1", got)
	}
}

func TestDisconnectDuringPostRefreshStartupDrainsQueuedSessionBeforeDone(t *testing.T) {
	client := lifecycleTestClient()
	refreshReturned := make(chan struct{})
	releaseStartup := make(chan struct{})
	var sessionCallbacks atomic.Int32
	var polls atomic.Int32
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error {
			client.emitLifecycleActivity(lifecycleActivitySessionChange)
			return nil
		},
		afterRefresh: func() {
			close(refreshReturned)
			<-releaseStartup
		},
		longPoll: func(context.Context, func()) bool {
			polls.Add(1)
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	client.SetLifecycleHooks(LifecycleHooks{OnSessionChange: func() { sessionCallbacks.Add(1) }})
	connectResult := make(chan error, 1)
	go func() { connectResult <- client.ConnectContext(context.Background()) }()
	receiveLifecycle(t, refreshReturned, "successful refresh startup barrier")
	client.lifecycleMu.Lock()
	generation := client.lifecycle
	client.lifecycleMu.Unlock()
	if generation == nil {
		t.Fatal("startup generation missing at post-refresh barrier")
	}
	disconnectResult := make(chan error, 1)
	go func() { disconnectResult <- client.DisconnectContext(context.Background()) }()
	receiveLifecycle(t, generation.ctx.Done(), "disconnect startup cancellation")
	close(releaseStartup)
	if err := receiveLifecycle(t, connectResult, "canceled startup completion"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ConnectContext() error = %v, want context cancellation", err)
	}
	if err := receiveLifecycle(t, disconnectResult, "disconnect startup completion"); err != nil {
		t.Fatalf("DisconnectContext() error = %v", err)
	}
	if got := sessionCallbacks.Load(); got != 1 {
		t.Fatalf("session callbacks before stopped generation done = %d, want 1", got)
	}
	if got := polls.Load(); got != 0 {
		t.Fatalf("long polls started after disconnect claimed startup = %d, want 0", got)
	}
}

func TestSessionChangeNotificationSurvivesSaturatedLivenessMailbox(t *testing.T) {
	client := lifecycleTestClient()
	pollStarted := make(chan struct{})
	frameEntered := make(chan struct{})
	releaseFrame := make(chan struct{})
	sessionChanged := make(chan struct{}, 1)
	var blockOnce sync.Once
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(ctx context.Context, ready func()) bool {
			close(pollStarted)
			ready()
			<-ctx.Done()
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	client.SetLifecycleHooks(LifecycleHooks{
		OnFrame: func() {
			blockOnce.Do(func() {
				close(frameEntered)
				<-releaseFrame
			})
		},
		OnSessionChange: func() { sessionChanged <- struct{}{} },
	})
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(client.Disconnect)
	receiveLifecycle(t, pollStarted, "poll start")
	client.emitLifecycleActivity(lifecycleActivityFrame)
	receiveLifecycle(t, frameEntered, "blocked frame callback")
	for range lifecycleActivityBuffer {
		client.emitLifecycleActivity(lifecycleActivityFrame)
	}
	client.emitLifecycleActivity(lifecycleActivitySessionChange)
	close(releaseFrame)
	receiveLifecycle(t, sessionChanged, "session-change callback after liveness saturation")
}

func TestInitialRefreshSessionChangeIsDeliveredBeforeImmediatePollTeardown(t *testing.T) {
	client := lifecycleTestClient()
	activityEntered := make(chan struct{})
	releaseActivity := make(chan struct{})
	pollExited := make(chan struct{})
	var sessionCallbacks atomic.Int32
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error {
			client.emitLifecycleActivity(lifecycleActivitySessionChange)
			return nil
		},
		longPoll: func(context.Context, func()) bool {
			close(pollExited)
			return true
		},
		beforeActivityLoop: func() {
			close(activityEntered)
			<-releaseActivity
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	client.SetLifecycleHooks(LifecycleHooks{OnSessionChange: func() { sessionCallbacks.Add(1) }})
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, activityEntered, "blocked activity worker")
	receiveLifecycle(t, pollExited, "immediate poll exit")
	client.lifecycleMu.Lock()
	generation := client.lifecycle
	client.lifecycleMu.Unlock()
	if generation == nil {
		t.Fatal("generation cleaned up before its blocked activity worker")
	}
	close(releaseActivity)
	receiveLifecycle(t, generation.done, "generation teardown")
	if got := sessionCallbacks.Load(); got != 1 {
		t.Fatalf("session callbacks = %d, want exactly 1 before teardown", got)
	}
}

func TestFinalSessionChangeQueuedDuringPollTeardownIsLastCallback(t *testing.T) {
	client := lifecycleTestClient()
	pollStarted := make(chan struct{})
	exitPoll := make(chan struct{})
	frameEntered := make(chan struct{})
	releaseFrame := make(chan struct{})
	var callbackMu sync.Mutex
	var callbacks []string
	client.lifecycleDeps = lifecycleDependencies{
		refresh: func(context.Context) error { return nil },
		longPoll: func(context.Context, func()) bool {
			close(pollStarted)
			<-exitPoll
			return true
		},
		newTicker: func(time.Duration) lifecycleTicker { return newManualLifecycleTicker() },
	}
	client.SetLifecycleHooks(LifecycleHooks{
		OnFrame: func() {
			callbackMu.Lock()
			callbacks = append(callbacks, "frame")
			callbackMu.Unlock()
			close(frameEntered)
			<-releaseFrame
		},
		OnSessionChange: func() {
			callbackMu.Lock()
			callbacks = append(callbacks, "session")
			callbackMu.Unlock()
		},
	})
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	receiveLifecycle(t, pollStarted, "poll start")
	client.lifecycleMu.Lock()
	generation := client.lifecycle
	client.lifecycleMu.Unlock()
	client.emitLifecycleActivity(lifecycleActivityFrame)
	receiveLifecycle(t, frameEntered, "blocked frame callback")
	client.emitLifecycleActivity(lifecycleActivitySessionChange)
	close(exitPoll)
	receiveLifecycle(t, generation.ctx.Done(), "poll teardown cancellation")
	close(releaseFrame)
	receiveLifecycle(t, generation.done, "generation teardown")
	callbackMu.Lock()
	defer callbackMu.Unlock()
	if len(callbacks) != 2 || callbacks[0] != "frame" || callbacks[1] != "session" {
		t.Fatalf("callbacks = %v, want [frame session]", callbacks)
	}
}

func lifecycleTestClient() *Client {
	auth := NewAuthData()
	auth.SetDevices(&gmproto.Device{SourceID: "browser"}, &gmproto.Device{SourceID: "mobile"})
	auth.SetTachyonAuth([]byte("token"), time.Now().Add(24*time.Hour), int64(24*time.Hour))
	return NewClient(auth, nil, zerolog.Nop())
}

func connectMany(t *testing.T, client *Client, count int) {
	t.Helper()
	var wait sync.WaitGroup
	for range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := client.Connect(); err != nil {
				t.Errorf("Connect() error = %v", err)
			}
		}()
	}
	wait.Wait()
}

type manualLifecycleTimer struct {
	channel chan time.Time
	stopped atomic.Bool
}

func newManualLifecycleTimer() *manualLifecycleTimer {
	return &manualLifecycleTimer{channel: make(chan time.Time)}
}
func (timer *manualLifecycleTimer) C() <-chan time.Time { return timer.channel }
func (timer *manualLifecycleTimer) Stop() bool          { return timer.stopped.CompareAndSwap(false, true) }

type manualLifecycleTicker struct {
	channel chan time.Time
	stopped atomic.Bool
}

func newManualLifecycleTicker() *manualLifecycleTicker {
	return &manualLifecycleTicker{channel: make(chan time.Time)}
}
func (ticker *manualLifecycleTicker) C() <-chan time.Time { return ticker.channel }
func (ticker *manualLifecycleTicker) Stop()               { ticker.stopped.Store(true) }

func receiveLifecycle[T any](t *testing.T, channel <-chan T, name string) T {
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
