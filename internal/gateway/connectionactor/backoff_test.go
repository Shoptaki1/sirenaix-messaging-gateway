package connectionactor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffUsesBoundedFullJitterAndOnlyReadyResetsFailures(t *testing.T) {
	values := []int64{0, 9, 39}
	index := 0
	backoff, err := NewBackoff(BackoffConfig{
		Base: time.Second,
		Cap:  4 * time.Second,
		Int63n: func(limit int64) int64 {
			if index >= len(values) {
				t.Fatal("unexpected random draw")
			}
			value := values[index]
			index++
			if value >= limit {
				t.Fatalf("random limit = %d, value = %d", limit, value)
			}
			return value
		},
	})
	if err != nil {
		t.Fatalf("NewBackoff() error = %v", err)
	}

	if got := backoff.Fail(); got != 0 {
		t.Fatalf("first delay = %s, want 0", got)
	}
	if got := backoff.Fail(); got != 9*time.Nanosecond {
		t.Fatalf("second delay = %s, want 9ns", got)
	}
	if got := backoff.Fail(); got != 39*time.Nanosecond {
		t.Fatalf("capped delay = %s, want 39ns", got)
	}
	if got := backoff.Failures(); got != 3 {
		t.Fatalf("failures = %d, want 3", got)
	}
	backoff.Ready()
	if got := backoff.Failures(); got != 0 {
		t.Fatalf("failures after Ready = %d, want 0", got)
	}
}

func TestWaitIsContextCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := Wait(ctx, time.Hour, func(time.Duration) Timer {
		called = true
		return blockingTimer{}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("timer was created after context was already cancelled")
	}
}

type blockingTimer struct{}

func (blockingTimer) C() <-chan time.Time { return make(chan time.Time) }
func (blockingTimer) Stop() bool          { return true }
