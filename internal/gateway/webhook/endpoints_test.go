package webhook_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

type endpointStore struct {
	created      webhook.EndpointRecord
	rotated      webhook.EndpointRotation
	list         []webhook.Endpoint
	deleted      string
	replayed     string
	deleting     bool
	maxEndpoints int
}

func (store *endpointStore) CreateEndpoint(_ context.Context, record webhook.EndpointRecord, maxEndpoints int) error {
	store.created = record
	store.maxEndpoints = maxEndpoints
	return nil
}
func (store *endpointStore) RotateEndpoint(_ context.Context, rotation webhook.EndpointRotation) error {
	store.rotated = rotation
	return nil
}
func (store *endpointStore) ListEndpoints(_ context.Context, _ domain.TenantID, options webhook.EndpointListOptions) (webhook.EndpointPage, error) {
	return webhook.EndpointPage{Endpoints: append([]webhook.Endpoint(nil), store.list...), NextCursor: options.After}, nil
}
func (store *endpointStore) DeleteEndpoint(_ context.Context, _ domain.TenantID, endpointID string) (webhook.DeleteResult, error) {
	store.deleted = endpointID
	return webhook.DeleteResult{Deleting: store.deleting}, nil
}

func TestEndpointDeleteReportsAlreadyOnWireDelivery(t *testing.T) {
	store := &endpointStore{deleting: true}
	service, err := webhook.NewEndpointService(webhook.EndpointConfig{
		Store: store, Secrets: &secretSealer{}, Destinations: safeEndpointGuard{}, NewID: func() string { return "id-a" },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Delete(context.Background(), "tenant-a", "endpoint-a")
	if err != nil || !result.Deleting || store.deleted != "endpoint-a" {
		t.Fatalf("Delete = (%+v, %v), stored=%q", result, err, store.deleted)
	}
}
func (store *endpointStore) ReplayDLQ(_ context.Context, _ domain.TenantID, dlqID string) error {
	store.replayed = dlqID
	return nil
}

type secretSealer struct {
	plaintext []byte
	scope     session.Scope
}

func (sealer *secretSealer) Seal(_ context.Context, scope session.Scope, plaintext []byte) (session.Envelope, error) {
	sealer.scope = scope
	sealer.plaintext = append([]byte(nil), plaintext...)
	return session.Envelope{Version: 1, Provider: "webhook", Ciphertext: []byte{1}, WrappedDEK: []byte{2}, Nonce: make([]byte, 12), KeyID: "kms-a", KeyVersion: 1}, nil
}

type safeEndpointGuard struct{ err error }

func (guard safeEndpointGuard) ClientFor(context.Context, string) (*http.Client, error) {
	if guard.err != nil {
		return nil, guard.err
	}
	return &http.Client{}, nil
}

func TestEndpointServiceGeneratesAndSealsSecretRevealedOnlyAtCreation(t *testing.T) {
	store := &endpointStore{}
	sealer := &secretSealer{}
	ids := []string{"endpoint-a", "key-a"}
	service, err := webhook.NewEndpointService(webhook.EndpointConfig{
		Store: store, Secrets: sealer, Destinations: safeEndpointGuard{},
		NewID:  func() string { id := ids[0]; ids = ids[1:]; return id },
		Random: strings.NewReader(strings.Repeat("s", 32)),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(context.Background(), "tenant-a", "https://hooks.example/receive")
	if err != nil {
		t.Fatal(err)
	}
	if created.Endpoint.ID != "endpoint-a" || created.Secret == "" || strings.Contains(created.Secret, " ") ||
		store.created.Secret.Ciphertext == nil || sealer.scope.ConnectionID != "endpoint-a" || sealer.scope.Provider != "webhook" || len(sealer.plaintext) != 32 || store.maxEndpoints != webhook.DefaultMaxEndpointsPerTenant {
		t.Fatalf("created=%+v record=%+v scope=%+v", created, store.created, sealer.scope)
	}
	if strings.Contains(string(store.created.Secret.Ciphertext), created.Secret) {
		t.Fatal("plaintext secret was persisted")
	}
	listed, err := service.List(context.Background(), "tenant-a", webhook.EndpointListOptions{Limit: 50})
	if err != nil || len(listed.Endpoints) != 0 {
		t.Fatalf("List = (%+v, %v)", listed, err)
	}
}

func TestEndpointServiceRejectsUnsafeDestinationBeforeSecretGeneration(t *testing.T) {
	sentinel := errors.New("private destination")
	store := &endpointStore{}
	service, _ := webhook.NewEndpointService(webhook.EndpointConfig{
		Store: store, Secrets: &secretSealer{}, Destinations: safeEndpointGuard{err: sentinel},
		NewID: func() string { return "unused" }, Random: strings.NewReader(strings.Repeat("s", 32)),
	})
	if _, err := service.Create(context.Background(), "tenant-a", "https://127.0.0.1/hook"); err == nil {
		t.Fatal("Create accepted unsafe destination")
	}
	if store.created.Endpoint.ID != "" {
		t.Fatal("unsafe endpoint was persisted")
	}
}
