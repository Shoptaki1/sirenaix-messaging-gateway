package connectionactor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestManagerConcurrentStartCreatesOneLocalActorAndStopWaits(t *testing.T) {
	started := make(chan struct{})
	exited := make(chan struct{})
	var runs atomic.Int32
	manager, err := NewManager(ManagerConfig{Run: func(ctx context.Context, key Key) {
		if key != (Key{TenantID: "tenant-a", ConnectionID: "connection-1"}) {
			t.Errorf("actor key = %#v", key)
		}
		runs.Add(1)
		close(started)
		<-ctx.Done()
		close(exited)
	}})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	key := Key{TenantID: domain.TenantID("tenant-a"), ConnectionID: domain.ConnectionID("connection-1")}

	var callers sync.WaitGroup
	for range 32 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if startErr := manager.Start(context.Background(), key); startErr != nil {
				t.Errorf("Start() error = %v", startErr)
			}
		}()
	}
	callers.Wait()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("actor did not start")
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("actor runs = %d, want 1", got)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Stop(stopCtx, key); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("Stop returned before actor exited")
	}
	if err := manager.Stop(stopCtx, key); err != nil {
		t.Fatalf("repeated Stop() error = %v", err)
	}
}

func TestManagerStartReplacesExitedGenerationBeforeReturning(t *testing.T) {
	key := Key{TenantID: domain.TenantID("tenant-a"), ConnectionID: domain.ConnectionID("connection-1")}
	firstStarted := make(chan struct{})
	returnFirst := make(chan struct{})
	beforeFirstCleanup := make(chan struct{})
	releaseFirstCleanup := make(chan struct{})
	startObservedFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var runs atomic.Int32
	manager, err := NewManager(ManagerConfig{
		Run: func(ctx context.Context, got Key) {
			if got != key {
				t.Errorf("actor key = %#v", got)
			}
			switch runs.Add(1) {
			case 1:
				close(firstStarted)
				<-returnFirst
			case 2:
				close(secondStarted)
				<-ctx.Done()
			default:
				t.Errorf("unexpected actor generation")
			}
		},
		beforeCleanup: func(got Key) {
			if runs.Load() != 1 {
				return
			}
			close(beforeFirstCleanup)
			<-releaseFirstCleanup
		},
		beforeStartDecision: func(got Key) {
			close(startObservedFirst)
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.Start(context.Background(), key); err != nil {
		t.Fatalf("initial Start() error = %v", err)
	}
	receiveManagerSignal(t, firstStarted, "first actor start")
	close(returnFirst)
	receiveManagerSignal(t, beforeFirstCleanup, "first actor cleanup barrier")

	startResult := make(chan error, 1)
	go func() { startResult <- manager.Start(context.Background(), key) }()
	receiveManagerSignal(t, startObservedFirst, "Start observation of exited generation")
	close(releaseFirstCleanup)
	select {
	case startErr := <-startResult:
		if startErr != nil {
			t.Fatalf("replacement Start() error = %v", startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement Start() did not return")
	}
	receiveManagerSignal(t, secondStarted, "replacement actor start")
	if got := runs.Load(); got != 2 {
		t.Fatalf("actor runs = %d, want 2", got)
	}
	if err := manager.Stop(context.Background(), key); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func receiveManagerSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
