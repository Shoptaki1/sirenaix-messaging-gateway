package webhook

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const (
	HeaderVersion   = "SirenaIX-Webhook-Version"
	HeaderTimestamp = "SirenaIX-Webhook-Timestamp"
	HeaderEventID   = "SirenaIX-Webhook-Event-ID"
	HeaderKeyID     = "SirenaIX-Webhook-Key-ID"
	HeaderSignature = "SirenaIX-Webhook-Signature"

	defaultMaxConcurrent = 4
	defaultMaxAttempts   = 8
	defaultClaimLimit    = 32
	maxRetryAfter        = time.Hour
)

var ErrInvalidWorker = errors.New("invalid webhook worker configuration")
var ErrWebhookClaimLost = errors.New("webhook delivery claim lost")

type Delivery struct {
	TenantID           domain.TenantID
	DeliveryID         string
	EndpointID         string
	EventID            string
	Destination        string
	KeyID              string
	CanonicalBody      []byte
	Attempt            int
	OwnerID            string
	ClaimToken         uint64
	EndpointGeneration uint64
	Secret             session.Envelope
}

type AttemptResult struct {
	TenantID        domain.TenantID
	DeliveryID      string
	Attempt         int
	OwnerID         string
	ClaimToken      uint64
	Succeeded       bool
	Dead            bool
	StatusCode      int
	SafeError       string
	NextAvailableAt time.Time
}

type HTTPResult struct {
	StatusCode int
	RetryAfter time.Duration
}

type Store interface {
	PurgeExpiredSecrets(context.Context, domain.TenantID) error
	Claim(context.Context, domain.TenantID, string, int) ([]Delivery, error)
	AdmitClaim(context.Context, Delivery) (bool, error)
	CompleteAttempt(context.Context, AttemptResult) error
	RenewClaim(context.Context, domain.TenantID, string, string, uint64) (bool, error)
}

type SecretOpener interface {
	Open(context.Context, session.Scope, session.Envelope) ([]byte, error)
}

type Deliverer interface {
	Deliver(context.Context, string, http.Header, []byte) (HTTPResult, error)
}

type WorkerConfig struct {
	Store         Store
	Secrets       SecretOpener
	Deliverer     Deliverer
	OwnerID       string
	TenantID      domain.TenantID
	Now           func() time.Time
	MaxConcurrent int
	MaxAttempts   int
	ClaimLimit    int
	Jitter        func(time.Duration) time.Duration
	RenewInterval time.Duration
}

type Worker struct {
	store         Store
	secrets       SecretOpener
	deliverer     Deliverer
	ownerID       string
	tenantID      domain.TenantID
	now           func() time.Time
	maxConcurrent int
	maxAttempts   int
	claimLimit    int
	jitter        func(time.Duration) time.Duration
	renewInterval time.Duration
}

func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.Store == nil || config.Secrets == nil || config.Deliverer == nil || config.OwnerID == "" || config.TenantID == "" {
		return nil, ErrInvalidWorker
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultMaxConcurrent
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.ClaimLimit == 0 {
		config.ClaimLimit = defaultClaimLimit
	}
	if config.Jitter == nil {
		config.Jitter = fullJitter
	}
	if config.RenewInterval == 0 {
		config.RenewInterval = 10 * time.Second
	}
	if config.MaxConcurrent < 1 || config.MaxConcurrent > 64 || config.MaxAttempts < 1 || config.MaxAttempts > 32 ||
		config.ClaimLimit < 1 || config.ClaimLimit > 256 || config.RenewInterval < time.Millisecond || config.RenewInterval > 15*time.Second {
		return nil, ErrInvalidWorker
	}
	return &Worker{
		store: config.Store, secrets: config.Secrets, deliverer: config.Deliverer, ownerID: config.OwnerID, tenantID: config.TenantID,
		now: config.Now, maxConcurrent: config.MaxConcurrent, maxAttempts: config.MaxAttempts,
		claimLimit: config.ClaimLimit, jitter: config.Jitter,
		renewInterval: config.RenewInterval,
	}, nil
}

func fullJitter(maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(maximum)+1))
	if err != nil {
		return maximum / 2
	}
	return time.Duration(value.Int64())
}

func (worker *Worker) RunBatch(ctx context.Context) error {
	if err := worker.store.PurgeExpiredSecrets(ctx, worker.tenantID); err != nil {
		return err
	}
	deliveries, err := worker.store.Claim(ctx, worker.tenantID, worker.ownerID, worker.claimLimit)
	if err != nil {
		return err
	}
	sem := make(chan struct{}, worker.maxConcurrent)
	var wg sync.WaitGroup
	var resultErr error
	var resultMu sync.Mutex
	for _, delivery := range deliveries {
		if delivery.TenantID != worker.tenantID || delivery.OwnerID != worker.ownerID || delivery.ClaimToken == 0 {
			return errors.New("webhook store returned a cross-tenant delivery")
		}
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(delivery Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			if attemptErr := worker.attempt(ctx, delivery); attemptErr != nil {
				resultMu.Lock()
				resultErr = errors.Join(resultErr, attemptErr)
				resultMu.Unlock()
			}
		}(delivery)
	}
	wg.Wait()
	return errors.Join(ctx.Err(), resultErr)
}

func (worker *Worker) attempt(ctx context.Context, delivery Delivery) error {
	now := worker.now().UTC()
	secret, err := worker.secrets.Open(ctx, session.Scope{
		TenantID: string(delivery.TenantID), ConnectionID: delivery.EndpointID, Provider: "webhook",
	}, delivery.Secret)
	if err != nil {
		return worker.completeFailure(ctx, delivery, now, 0, "secret unavailable")
	}
	defer zero(secret)
	headers := make(http.Header)
	headers.Set(HeaderVersion, "v1")
	headers.Set(HeaderTimestamp, strconv.FormatInt(now.Unix(), 10))
	headers.Set(HeaderEventID, delivery.EventID)
	headers.Set(HeaderKeyID, delivery.KeyID)
	headers.Set(HeaderSignature, Sign(secret, now, delivery.EventID, delivery.CanonicalBody))
	admitted, err := worker.store.AdmitClaim(ctx, delivery)
	if err != nil {
		return err
	}
	if !admitted {
		return ErrWebhookClaimLost
	}
	deliveryCtx, cancelDelivery := context.WithCancel(ctx)
	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewResult := make(chan error, 1)
	go worker.renewClaim(renewCtx, cancelDelivery, delivery, renewResult)
	result, err := worker.deliverer.Deliver(deliveryCtx, delivery.Destination, headers, delivery.CanonicalBody)
	stopRenew()
	cancelDelivery()
	if renewErr := <-renewResult; renewErr != nil {
		return renewErr
	}
	if err == nil && result.StatusCode >= http.StatusOK && result.StatusCode < http.StatusMultipleChoices {
		return worker.store.CompleteAttempt(ctx, AttemptResult{
			TenantID: delivery.TenantID, DeliveryID: delivery.DeliveryID, Attempt: delivery.Attempt,
			OwnerID: delivery.OwnerID, ClaimToken: delivery.ClaimToken, Succeeded: true, StatusCode: result.StatusCode,
		})
	}
	safeError := "delivery failed"
	if err == nil {
		safeError = fmt.Sprintf("webhook returned HTTP %d", result.StatusCode)
	}
	return worker.completeFailureWithRetryAfter(ctx, delivery, now, result.StatusCode, safeError, result.RetryAfter)
}

func (worker *Worker) renewClaim(ctx context.Context, cancelDelivery context.CancelFunc, delivery Delivery, result chan<- error) {
	ticker := time.NewTicker(worker.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			result <- nil
			return
		case <-ticker.C:
			owned, err := worker.store.RenewClaim(ctx, delivery.TenantID, delivery.DeliveryID, delivery.OwnerID, delivery.ClaimToken)
			if err != nil || !owned {
				cancelDelivery()
				if err == nil {
					err = ErrWebhookClaimLost
				}
				result <- err
				return
			}
		}
	}
}

func (worker *Worker) completeFailure(ctx context.Context, delivery Delivery, now time.Time, statusCode int, safeError string) error {
	return worker.completeFailureWithRetryAfter(ctx, delivery, now, statusCode, safeError, 0)
}

func (worker *Worker) completeFailureWithRetryAfter(ctx context.Context, delivery Delivery, now time.Time, statusCode int, safeError string, retryAfter time.Duration) error {
	dead := delivery.Attempt >= worker.maxAttempts
	result := AttemptResult{
		TenantID: delivery.TenantID, DeliveryID: delivery.DeliveryID, Attempt: delivery.Attempt,
		OwnerID: delivery.OwnerID, ClaimToken: delivery.ClaimToken, Dead: dead, StatusCode: statusCode, SafeError: safeError,
	}
	if !dead {
		if retryAfter < 0 {
			retryAfter = 0
		}
		if retryAfter > maxRetryAfter {
			retryAfter = maxRetryAfter
		}
		backoff := time.Second << min(delivery.Attempt-1, 10)
		jitter := worker.jitter(backoff)
		if jitter < 0 || jitter > maxRetryAfter {
			jitter = maxRetryAfter
		}
		result.NextAvailableAt = now.Add(max(retryAfter, jitter))
	}
	return worker.store.CompleteAttempt(ctx, result)
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
