package webhook_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

type deliveryStore struct {
	mu         sync.Mutex
	deliveries []webhook.Delivery
	results    []webhook.AttemptResult
	claimErr   error
	resultErr  error
	renewOK    bool
	renewCalls int
	purgeCalls int
	purgeErr   error
	admit      func(webhook.Delivery) bool
	admitCalls []string
}

func (store *deliveryStore) AdmitClaim(_ context.Context, delivery webhook.Delivery) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.admitCalls = append(store.admitCalls, delivery.DeliveryID)
	if store.admit != nil {
		return store.admit(delivery), nil
	}
	return true, nil
}

func (store *deliveryStore) PurgeExpiredSecrets(context.Context, domain.TenantID) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.purgeCalls++
	return store.purgeErr
}

func (store *deliveryStore) Claim(context.Context, domain.TenantID, string, int) ([]webhook.Delivery, error) {
	return append([]webhook.Delivery(nil), store.deliveries...), store.claimErr
}

func (store *deliveryStore) CompleteAttempt(_ context.Context, result webhook.AttemptResult) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.results = append(store.results, result)
	return store.resultErr
}

func (store *deliveryStore) RenewClaim(context.Context, domain.TenantID, string, string, uint64) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.renewCalls++
	return store.renewOK, nil
}

type secretOpener struct{ secret []byte }

func (opener secretOpener) Open(context.Context, session.Scope, session.Envelope) ([]byte, error) {
	return append([]byte(nil), opener.secret...), nil
}

type keyedSecretOpener map[string][]byte

func (opener keyedSecretOpener) Open(_ context.Context, _ session.Scope, envelope session.Envelope) ([]byte, error) {
	return append([]byte(nil), opener[envelope.KeyID]...), nil
}

type capturedRequest struct {
	url     string
	headers http.Header
	body    []byte
}

type fakeDeliverer struct {
	requests []capturedRequest
	result   webhook.HTTPResult
	err      error
}

func (deliverer *fakeDeliverer) Deliver(_ context.Context, destination string, headers http.Header, body []byte) (webhook.HTTPResult, error) {
	deliverer.requests = append(deliverer.requests, capturedRequest{url: destination, headers: headers.Clone(), body: append([]byte(nil), body...)})
	return deliverer.result, deliverer.err
}

func TestWorkerSignsStoredCanonicalBodyAndOnlyTwoHundredSucceeds(t *testing.T) {
	body := []byte("{\"type\":\"message.sent\",\"event_id\":\"event-a\"}")
	store := &deliveryStore{deliveries: []webhook.Delivery{{
		TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a",
		Destination: "https://hooks.example/receive", KeyID: "key-a", CanonicalBody: body, Attempt: 1,
		OwnerID: "worker-a", ClaimToken: 1,
		Secret: session.Envelope{Version: 1, Provider: "webhook"},
	}}}
	deliverer := &fakeDeliverer{result: webhook.HTTPResult{StatusCode: http.StatusNoContent}}
	now := time.Unix(1700000000, 0).UTC()
	worker, err := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: secretOpener{secret: []byte("secret-a")}, Deliverer: deliverer,
		OwnerID: "worker-a", TenantID: "tenant-a", Now: func() time.Time { return now }, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deliverer.requests) != 1 || !bytes.Equal(deliverer.requests[0].body, body) {
		t.Fatalf("requests = %+v", deliverer.requests)
	}
	request := deliverer.requests[0]
	if request.headers.Get(webhook.HeaderVersion) != "v1" || request.headers.Get(webhook.HeaderTimestamp) != "1700000000" ||
		request.headers.Get(webhook.HeaderEventID) != "event-a" || request.headers.Get(webhook.HeaderKeyID) != "key-a" ||
		!webhook.Verify([]byte("secret-a"), now, "event-a", body, request.headers.Get(webhook.HeaderSignature)) {
		t.Fatalf("signature headers = %v", request.headers)
	}
	if len(store.results) != 1 || !store.results[0].Succeeded || store.results[0].Dead {
		t.Fatalf("attempt results = %+v", store.results)
	}
}

func TestWorkerPurgesExpiredPreviousSecretWithoutAnyDelivery(t *testing.T) {
	store := &deliveryStore{}
	worker, err := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: secretOpener{secret: []byte("unused")}, Deliverer: &fakeDeliverer{},
		OwnerID: "worker-a", TenantID: "tenant-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.purgeCalls != 1 {
		t.Fatalf("PurgeExpiredSecrets calls = %d, want 1 even with no delivery", store.purgeCalls)
	}
}

func TestWebhookRetryAfterRotationUsesExactBodyAndCurrentClaimedKey(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	body := []byte(`{"event_id":"event-a","type":"message.sent"}`)
	store := &deliveryStore{deliveries: []webhook.Delivery{{
		TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a",
		Destination: "https://hooks.example", KeyID: "key-old", CanonicalBody: body, Attempt: 1,
		OwnerID: "worker-a", ClaimToken: 1, Secret: session.Envelope{Version: 1, Provider: "webhook", KeyID: "kms-old"},
	}}}
	deliverer := &fakeDeliverer{result: webhook.HTTPResult{StatusCode: http.StatusServiceUnavailable}}
	worker, _ := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: keyedSecretOpener{"kms-old": []byte("old-secret"), "kms-new": []byte("new-secret")},
		Deliverer: deliverer, OwnerID: "worker-a", TenantID: "tenant-a", Now: func() time.Time { return now },
	})
	if err := worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.deliveries[0].KeyID = "key-new"
	store.deliveries[0].Secret.KeyID = "kms-new"
	store.deliveries[0].Attempt = 2
	store.deliveries[0].ClaimToken = 2
	if err := worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deliverer.requests) != 2 || !bytes.Equal(deliverer.requests[0].body, body) || !bytes.Equal(deliverer.requests[1].body, body) {
		t.Fatalf("requests = %+v", deliverer.requests)
	}
	if !webhook.Verify([]byte("old-secret"), now, "event-a", body, deliverer.requests[0].headers.Get(webhook.HeaderSignature)) ||
		deliverer.requests[0].headers.Get(webhook.HeaderKeyID) != "key-old" {
		t.Fatal("pre-rotation attempt was not signed by the claimed previous key")
	}
	if !webhook.Verify([]byte("new-secret"), now, "event-a", body, deliverer.requests[1].headers.Get(webhook.HeaderSignature)) ||
		deliverer.requests[1].headers.Get(webhook.HeaderKeyID) != "key-new" {
		t.Fatal("post-rotation retry was not signed by the newly claimed key")
	}
}

func TestWorkerRetriesRedirectsAndDeadLettersAtExhaustion(t *testing.T) {
	for name, result := range map[string]webhook.HTTPResult{
		"redirect": {StatusCode: http.StatusFound},
		"server":   {StatusCode: http.StatusServiceUnavailable, RetryAfter: 24 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			store := &deliveryStore{deliveries: []webhook.Delivery{{
				TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a",
				Destination: "https://hooks.example/receive", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 8,
				OwnerID: "worker-a", ClaimToken: 1,
				Secret: session.Envelope{Version: 1, Provider: "webhook"},
			}}}
			worker, _ := webhook.NewWorker(webhook.WorkerConfig{
				Store: store, Secrets: secretOpener{secret: []byte("secret-a")}, Deliverer: &fakeDeliverer{result: result},
				OwnerID: "worker-a", TenantID: "tenant-a", MaxAttempts: 8, Jitter: func(time.Duration) time.Duration { return time.Second },
			})
			if err := worker.RunBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(store.results) != 1 || store.results[0].Succeeded || !store.results[0].Dead || store.results[0].SafeError == "" {
				t.Fatalf("attempt results = %+v", store.results)
			}
		})
	}
}

func TestWorkerPreservesSuccessCrashForAtLeastOnceReplay(t *testing.T) {
	crash := errors.New("database crashed after HTTP success")
	delivery := webhook.Delivery{TenantID: domain.TenantID("tenant-a"), DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a", Destination: "https://hooks.example", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 1, OwnerID: "worker-a", ClaimToken: 1, Secret: session.Envelope{Version: 1, Provider: "webhook"}}
	store := &deliveryStore{deliveries: []webhook.Delivery{delivery}, resultErr: crash}
	deliverer := &fakeDeliverer{result: webhook.HTTPResult{StatusCode: http.StatusOK}}
	worker, _ := webhook.NewWorker(webhook.WorkerConfig{Store: store, Secrets: secretOpener{secret: []byte("secret")}, Deliverer: deliverer, OwnerID: "worker-a", TenantID: "tenant-a"})
	if err := worker.RunBatch(context.Background()); !errors.Is(err, crash) {
		t.Fatalf("first RunBatch() error = %v", err)
	}
	store.resultErr = nil
	if err := worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deliverer.requests) != 2 || !bytes.Equal(deliverer.requests[0].body, deliverer.requests[1].body) {
		t.Fatalf("at-least-once requests = %+v", deliverer.requests)
	}
}

func TestWorkerTreatsRetryAfterAsMinimumNotJitterCeiling(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	store := &deliveryStore{deliveries: []webhook.Delivery{{
		TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a",
		Destination: "https://hooks.example", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 2,
		OwnerID: "worker-a", ClaimToken: 7, Secret: session.Envelope{Version: 1, Provider: "webhook"},
	}}}
	worker, _ := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: secretOpener{secret: []byte("secret")},
		Deliverer: &fakeDeliverer{result: webhook.HTTPResult{StatusCode: http.StatusTooManyRequests, RetryAfter: 30 * time.Second}},
		OwnerID:   "worker-a", TenantID: "tenant-a", Now: func() time.Time { return now },
		Jitter: func(time.Duration) time.Duration { return time.Second },
	})
	if err := worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.results[0].NextAvailableAt.Sub(now); got != 30*time.Second {
		t.Fatalf("retry delay = %s, want Retry-After minimum 30s", got)
	}
}

type blockingDeliverer struct{ started chan struct{} }

func (deliverer *blockingDeliverer) Deliver(ctx context.Context, _ string, _ http.Header, _ []byte) (webhook.HTTPResult, error) {
	close(deliverer.started)
	<-ctx.Done()
	return webhook.HTTPResult{}, ctx.Err()
}

func TestWorkerRenewsSlowClaimAndStaleTokenCannotComplete(t *testing.T) {
	store := &deliveryStore{deliveries: []webhook.Delivery{{
		TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a",
		Destination: "https://hooks.example", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 1,
		OwnerID: "worker-a", ClaimToken: 7, Secret: session.Envelope{Version: 1, Provider: "webhook"},
	}}}
	deliverer := &blockingDeliverer{started: make(chan struct{})}
	worker, _ := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: secretOpener{secret: []byte("secret")}, Deliverer: deliverer,
		OwnerID: "worker-a", TenantID: "tenant-a", RenewInterval: time.Millisecond,
	})
	done := make(chan error, 1)
	go func() { done <- worker.RunBatch(context.Background()) }()
	<-deliverer.started
	select {
	case err := <-done:
		if !errors.Is(err, webhook.ErrWebhookClaimLost) {
			t.Fatalf("RunBatch error = %v, want ErrWebhookClaimLost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow delivery was not canceled after claim renewal loss")
	}
	if store.renewCalls != 1 || len(store.results) != 0 {
		t.Fatalf("renewals=%d stale completions=%+v", store.renewCalls, store.results)
	}
}

type gatedDeliverer struct {
	mu       sync.Mutex
	started  chan string
	releases map[string]chan struct{}
	requests []string
}

func (deliverer *gatedDeliverer) Deliver(_ context.Context, destination string, _ http.Header, _ []byte) (webhook.HTTPResult, error) {
	deliverer.mu.Lock()
	deliverer.requests = append(deliverer.requests, destination)
	release := deliverer.releases[destination]
	deliverer.mu.Unlock()
	deliverer.started <- destination
	<-release
	return webhook.HTTPResult{StatusCode: http.StatusNoContent}, nil
}

func TestWorkerRevalidatesClaimAfterSemaphoreBeforeHTTP(t *testing.T) {
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	deliverer := &gatedDeliverer{
		started: make(chan string, 2),
		releases: map[string]chan struct{}{
			"https://hooks.example/first":  firstRelease,
			"https://hooks.example/second": secondRelease,
		},
	}
	active := map[string]bool{"delivery-a": true, "delivery-b": true}
	store := &deliveryStore{deliveries: []webhook.Delivery{
		{TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", EventID: "event-a", Destination: "https://hooks.example/first", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 1, OwnerID: "worker-a", ClaimToken: 1, EndpointGeneration: 1, Secret: session.Envelope{Version: 1, Provider: "webhook"}},
		{TenantID: "tenant-a", DeliveryID: "delivery-b", EndpointID: "endpoint-a", EventID: "event-b", Destination: "https://hooks.example/second", KeyID: "key-a", CanonicalBody: []byte("{}"), Attempt: 1, OwnerID: "worker-a", ClaimToken: 1, EndpointGeneration: 1, Secret: session.Envelope{Version: 1, Provider: "webhook"}},
	}, admit: func(delivery webhook.Delivery) bool { return active[delivery.DeliveryID] }}
	worker, err := webhook.NewWorker(webhook.WorkerConfig{
		Store: store, Secrets: secretOpener{secret: []byte("secret")}, Deliverer: deliverer,
		OwnerID: "worker-a", TenantID: "tenant-a", MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- worker.RunBatch(context.Background()) }()
	if got := <-deliverer.started; got != "https://hooks.example/first" {
		t.Fatalf("first request = %q", got)
	}
	store.mu.Lock()
	active["delivery-b"] = false
	store.mu.Unlock()
	close(firstRelease)
	select {
	case got := <-deliverer.started:
		close(secondRelease)
		t.Fatalf("revoked claimed delivery reached HTTP: %q", got)
	case err := <-done:
		if !errors.Is(err, webhook.ErrWebhookClaimLost) {
			t.Fatalf("RunBatch error = %v, want claim lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after revoked queued claim")
	}
	deliverer.mu.Lock()
	defer deliverer.mu.Unlock()
	if len(deliverer.requests) != 1 || len(store.admitCalls) != 2 {
		t.Fatalf("HTTP requests=%v admissions=%v", deliverer.requests, store.admitCalls)
	}
}
