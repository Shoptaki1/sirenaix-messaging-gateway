package session

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestVaultRestartLoadAndKeyRotation(t *testing.T) {
	wrapper := &fakeWrapper{keyID: "test-key", version: 1}
	manager := mustManager(t, wrapper)
	store := &memoryEnvelopeStore{values: make(map[string]Envelope)}
	vault, err := NewVault(manager, store)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	if err := vault.Save(context.Background(), scope, []byte("restart-session")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	restarted, err := NewVault(manager, store)
	if err != nil {
		t.Fatalf("restart NewVault: %v", err)
	}
	plaintext, err := restarted.Load(context.Background(), scope)
	if err != nil || string(plaintext) != "restart-session" {
		t.Fatalf("Load after restart = %q, %v", plaintext, err)
	}

	wrapper.version = 2
	changed, err := restarted.Rotate(context.Background(), scope)
	if err != nil || !changed {
		t.Fatalf("Rotate = %v, %v", changed, err)
	}
	if got := store.values[storeKey(scope)].KeyVersion; got != 2 {
		t.Fatalf("persisted key version = %d", got)
	}
}

func TestVaultDoesNotOverwriteEnvelopeWhenRotationSaveFails(t *testing.T) {
	wrapper := &fakeWrapper{keyID: "test-key", version: 1}
	manager := mustManager(t, wrapper)
	store := &memoryEnvelopeStore{values: make(map[string]Envelope)}
	vault, _ := NewVault(manager, store)
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	if err := vault.Save(context.Background(), scope, []byte("session")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	wrapper.version = 2
	store.saveErr = errors.New("storage failed")
	if _, err := vault.Rotate(context.Background(), scope); !errors.Is(err, ErrStore) {
		t.Fatalf("Rotate error = %v", err)
	}
	if got := store.values[storeKey(scope)].KeyVersion; got != 1 {
		t.Fatalf("failed rotation overwrote key version with %d", got)
	}
}

func TestVaultRotationCASDoesNotOverwriteConcurrentReplacement(t *testing.T) {
	wrapper := &fakeWrapper{keyID: "test-key", version: 1}
	manager := mustManager(t, wrapper)
	store := &memoryEnvelopeStore{values: make(map[string]Envelope)}
	vault, _ := NewVault(manager, store)
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	if err := vault.Save(context.Background(), scope, []byte("old-session")); err != nil {
		t.Fatalf("Save old: %v", err)
	}
	replacement, err := manager.Seal(context.Background(), scope, []byte("replacement-session"))
	if err != nil {
		t.Fatalf("Seal replacement: %v", err)
	}
	store.casEntered = make(chan struct{}, 1)
	store.casRelease = make(chan struct{})
	store.barrierKeyVersion = 2
	wrapper.version = 2
	rotated := make(chan error, 1)
	go func() {
		_, rotateErr := vault.Rotate(context.Background(), scope)
		rotated <- rotateErr
	}()
	<-store.casEntered
	if err := store.SaveEncryptedSession(context.Background(), "tenant-a", "connection-1", replacement); err != nil {
		t.Fatalf("concurrent replacement: %v", err)
	}
	close(store.casRelease)
	if err := <-rotated; err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	plaintext, err := vault.Load(context.Background(), scope)
	if err != nil || string(plaintext) != "replacement-session" {
		t.Fatalf("Load after race = %q, %v", plaintext, err)
	}
}

func TestVaultLoadsRevisionAndFencedCASDoesNotBypassEncryption(t *testing.T) {
	wrapper := &fakeWrapper{keyID: "test-key", version: 1}
	manager := mustManager(t, wrapper)
	store := &memoryEnvelopeStore{values: make(map[string]Envelope), allowFence: true}
	vault, err := NewVault(manager, store)
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	if err = vault.Save(context.Background(), scope, []byte("session-one")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	plaintext, revision, err := vault.LoadVersioned(context.Background(), scope)
	if err != nil || string(plaintext) != "session-one" || revision != 1 {
		t.Fatalf("LoadVersioned = %q, %d, %v", plaintext, revision, err)
	}
	zero(plaintext)

	swapped, err := vault.CompareAndSwapFenced(context.Background(), scope, "owner-a", 9, revision, []byte("session-two"))
	if err != nil || !swapped {
		t.Fatalf("CompareAndSwapFenced = %v, %v", swapped, err)
	}
	stored := store.values[storeKey(scope)]
	if bytes.Contains(stored.Ciphertext, []byte("session-two")) {
		t.Fatal("fenced session CAS persisted plaintext")
	}
	store.allowFence = false
	if swapped, err = vault.CompareAndSwapFenced(context.Background(), scope, "owner-a", 9, revision+1, []byte("stale")); err != nil || swapped {
		t.Fatalf("stale fenced CAS = %v, %v", swapped, err)
	}
}

type memoryEnvelopeStore struct {
	mu                sync.Mutex
	values            map[string]Envelope
	saveErr           error
	casEntered        chan struct{}
	casRelease        chan struct{}
	barrierKeyVersion int
	allowFence        bool
}

func (store *memoryEnvelopeStore) CompareAndSwapEncryptedSessionFenced(_ context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, _ string, _ uint64, expectedRevision uint64, envelope Envelope) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := string(tenantID) + "/" + string(connectionID)
	current, ok := store.values[key]
	if !store.allowFence || !ok || current.Revision != expectedRevision {
		return false, nil
	}
	envelope.Revision = current.Revision + 1
	store.values[key] = envelope.Clone()
	return true, nil
}

func storeKey(scope Scope) string { return scope.TenantID + "/" + scope.ConnectionID }
func (store *memoryEnvelopeStore) SaveEncryptedSession(_ context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, envelope Envelope) error {
	if envelope.KeyVersion == store.barrierKeyVersion && store.casEntered != nil {
		store.casEntered <- struct{}{}
		<-store.casRelease
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	key := string(tenantID) + "/" + string(connectionID)
	envelope.Revision = store.values[key].Revision + 1
	store.values[key] = envelope.Clone()
	return nil
}
func (store *memoryEnvelopeStore) LoadEncryptedSession(_ context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (Envelope, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	envelope, ok := store.values[string(tenantID)+"/"+string(connectionID)]
	if !ok {
		return Envelope{}, errors.New("not found")
	}
	return envelope.Clone(), nil
}

func (store *memoryEnvelopeStore) CompareAndSwapEncryptedSession(_ context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, expectedRevision uint64, envelope Envelope) (bool, error) {
	if envelope.KeyVersion == store.barrierKeyVersion && store.casEntered != nil {
		store.casEntered <- struct{}{}
		<-store.casRelease
		store.casEntered = nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return false, store.saveErr
	}
	key := string(tenantID) + "/" + string(connectionID)
	if store.values[key].Revision != expectedRevision {
		return false, nil
	}
	envelope.Revision = expectedRevision + 1
	store.values[key] = envelope.Clone()
	return true, nil
}
