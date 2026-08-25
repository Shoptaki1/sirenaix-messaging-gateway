package connectionactor

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"time"
)

const (
	defaultBackoffBase = 2 * time.Second
	defaultBackoffCap  = 5 * time.Minute
)

var ErrInvalidConfig = errors.New("invalid connection actor configuration")

type BackoffConfig struct {
	Base   time.Duration
	Cap    time.Duration
	Int63n func(int64) int64
}

type Backoff struct {
	base     time.Duration
	cap      time.Duration
	int63n   func(int64) int64
	failures uint
}

func NewBackoff(config BackoffConfig) (*Backoff, error) {
	if config.Base == 0 {
		config.Base = defaultBackoffBase
	}
	if config.Cap == 0 {
		config.Cap = defaultBackoffCap
	}
	if config.Base <= 0 || config.Cap < config.Base {
		return nil, ErrInvalidConfig
	}
	if config.Int63n == nil {
		config.Int63n = cryptoInt63n
	}
	return &Backoff{base: config.Base, cap: config.Cap, int63n: config.Int63n}, nil
}

// Fail records one unsuccessful connection generation and returns a full-jitter
// delay in [0, min(cap, base*2^(failures-1))).
func (backoff *Backoff) Fail() time.Duration {
	backoff.failures++
	limit := backoff.base
	for exponent := uint(1); exponent < backoff.failures && limit < backoff.cap; exponent++ {
		if limit > backoff.cap/2 {
			limit = backoff.cap
			break
		}
		limit *= 2
	}
	if limit > backoff.cap {
		limit = backoff.cap
	}
	return time.Duration(backoff.int63n(int64(limit)))
}

func (backoff *Backoff) Ready() { backoff.failures = 0 }

func (backoff *Backoff) Failures() uint { return backoff.failures }

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type timer struct{ *time.Timer }

func (timer timer) C() <-chan time.Time { return timer.Timer.C }

type TimerFactory func(time.Duration) Timer

func NewTimer(after time.Duration) Timer { return timer{Timer: time.NewTimer(after)} }

func Wait(ctx context.Context, delay time.Duration, newTimer TimerFactory) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if newTimer == nil {
		newTimer = NewTimer
	}
	wait := newTimer(delay)
	defer wait.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait.C():
		return nil
	}
}

func cryptoInt63n(limit int64) int64 {
	if limit <= 1 {
		return 0
	}
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return time.Now().UnixNano() % limit
	}
	return int64(binary.LittleEndian.Uint64(bytes[:])&(^uint64(0)>>1)) % limit
}
