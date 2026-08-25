package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestEnvelopeRoundTripUsesRandomAuthenticatedEncryption(t *testing.T) {
	manager := mustManager(t, &fakeWrapper{keyID: "test-key", version: 3})
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	plaintext := []byte(`{"credential":"session-secret"}`)

	first, err := manager.Seal(context.Background(), scope, plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	second, err := manager.Seal(context.Background(), scope, plaintext)
	if err != nil {
		t.Fatalf("second Seal: %v", err)
	}
	if bytes.Equal(first.Nonce, second.Nonce) || bytes.Equal(first.Ciphertext, second.Ciphertext) || bytes.Equal(first.WrappedDEK, second.WrappedDEK) {
		t.Fatal("two envelopes reused randomized encryption material")
	}
	restored, err := manager.Open(context.Background(), scope, first)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(restored, plaintext) {
		t.Fatalf("restored plaintext = %q", restored)
	}
	if got := fmt.Sprint(first); strings.Contains(got, "session-secret") || strings.Contains(got, fmt.Sprint(first.Ciphertext)) {
		t.Fatalf("formatted envelope exposed protected material: %s", got)
	}
}

func TestEnvelopePrevalidatesLimitsBeforeKMSUnwrap(t *testing.T) {
	wrapper := &countingWrapper{fakeWrapper: fakeWrapper{keyID: "test-key", version: 1}}
	manager := mustManager(t, wrapper)
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	valid, err := manager.Seal(context.Background(), scope, []byte("protected"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	wrapper.unwrapCalls = 0
	tests := []struct {
		name   string
		scope  Scope
		mutate func(*Envelope)
	}{
		{name: "oversized tenant", scope: Scope{TenantID: strings.Repeat("t", 129), ConnectionID: scope.ConnectionID, Provider: scope.Provider}},
		{name: "oversized connection", scope: Scope{TenantID: scope.TenantID, ConnectionID: strings.Repeat("c", 129), Provider: scope.Provider}},
		{name: "oversized provider", scope: scope, mutate: func(value *Envelope) { value.Provider = strings.Repeat("p", 65) }},
		{name: "oversized key ID", scope: scope, mutate: func(value *Envelope) { value.KeyID = strings.Repeat("k", 257) }},
		{name: "oversized wrapped key", scope: scope, mutate: func(value *Envelope) { value.WrappedDEK = make([]byte, 16*1024+1) }},
		{name: "oversized ciphertext", scope: scope, mutate: func(value *Envelope) { value.Ciphertext = make([]byte, 4*1024*1024+1) }},
		{name: "short ciphertext", scope: scope, mutate: func(value *Envelope) { value.Ciphertext = make([]byte, 15) }},
		{name: "wrong nonce length", scope: scope, mutate: func(value *Envelope) { value.Nonce = make([]byte, 11) }},
		{name: "unsupported version", scope: scope, mutate: func(value *Envelope) { value.Version++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope := valid.Clone()
			if test.mutate != nil {
				test.mutate(&envelope)
			}
			testScope := test.scope
			if testScope == (Scope{}) {
				testScope = scope
			}
			before := wrapper.unwrapCalls
			if _, err := manager.Open(context.Background(), testScope, envelope); !errors.Is(err, ErrDecrypt) {
				t.Fatalf("Open error = %v", err)
			}
			if wrapper.unwrapCalls != before {
				t.Fatal("invalid envelope reached KMS unwrap")
			}
		})
	}
	if _, err := manager.Seal(context.Background(), scope, make([]byte, 4*1024*1024+1)); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("oversized plaintext Seal = %v", err)
	}
}

func TestEnvelopeFormattingAndJSONAlwaysRedactFields(t *testing.T) {
	envelope := Envelope{Version: 1, Provider: "provider-sentinel", Ciphertext: []byte("ciphertext-sentinel"), WrappedDEK: []byte("wrapped-sentinel"), Nonce: []byte("nonce-sentinel"), KeyID: "key-sentinel", KeyVersion: 1}
	formatted := []string{fmt.Sprintf("%v", envelope), fmt.Sprintf("%+v", envelope), fmt.Sprintf("%#v", envelope)}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	formatted = append(formatted, string(encoded))
	for _, output := range formatted {
		for _, sentinel := range []string{"provider-sentinel", "ciphertext-sentinel", "wrapped-sentinel", "nonce-sentinel", "key-sentinel"} {
			if strings.Contains(output, sentinel) {
				t.Fatalf("formatted envelope exposed %q in %q", sentinel, output)
			}
		}
	}
}

func TestEnvelopeRejectsWrongScopeAndTampering(t *testing.T) {
	manager := mustManager(t, &fakeWrapper{keyID: "test-key", version: 1})
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	envelope, err := manager.Seal(context.Background(), scope, []byte("protected-marker"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	wrongScopes := []Scope{
		{TenantID: "tenant-b", ConnectionID: scope.ConnectionID, Provider: scope.Provider},
		{TenantID: scope.TenantID, ConnectionID: "connection-2", Provider: scope.Provider},
		{TenantID: scope.TenantID, ConnectionID: scope.ConnectionID, Provider: "other"},
	}
	for _, wrong := range wrongScopes {
		if _, err := manager.Open(context.Background(), wrong, envelope); !errors.Is(err, ErrDecrypt) {
			t.Errorf("Open(%+v) error = %v, want ErrDecrypt", wrong, err)
		} else if strings.Contains(err.Error(), "protected-marker") {
			t.Errorf("scope error exposed plaintext: %v", err)
		}
	}

	mutations := []func(*Envelope){
		func(value *Envelope) { value.Version++ },
		func(value *Envelope) { value.Provider = "other" },
		func(value *Envelope) { value.Nonce[0] ^= 1 },
		func(value *Envelope) { value.Ciphertext[0] ^= 1 },
		func(value *Envelope) { value.WrappedDEK[0] ^= 1 },
		func(value *Envelope) { value.KeyVersion++ },
	}
	for index, mutate := range mutations {
		copyEnvelope := envelope.Clone()
		mutate(&copyEnvelope)
		if _, err := manager.Open(context.Background(), scope, copyEnvelope); err == nil {
			t.Errorf("tampering mutation %d was accepted", index)
		} else if strings.Contains(err.Error(), "protected-marker") {
			t.Errorf("tampering error exposed plaintext: %v", err)
		}
	}
}

func TestEnvelopeReturnsSafeKeyWrapperErrors(t *testing.T) {
	secretError := errors.New("KMS failed around protected-marker")
	wrapper := &fakeWrapper{keyID: "test-key", version: 1, wrapErr: secretError}
	manager := mustManager(t, wrapper)
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	if _, err := manager.Seal(context.Background(), scope, []byte("protected-marker")); !errors.Is(err, ErrKeyWrapper) || strings.Contains(err.Error(), "protected-marker") {
		t.Fatalf("Seal error = %v, want safe ErrKeyWrapper", err)
	}

	wrapper.wrapErr = nil
	envelope, err := manager.Seal(context.Background(), scope, []byte("protected-marker"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	wrapper.unwrapErr = secretError
	if _, err := manager.Open(context.Background(), scope, envelope); !errors.Is(err, ErrKeyWrapper) || strings.Contains(err.Error(), "protected-marker") {
		t.Fatalf("Open error = %v, want safe ErrKeyWrapper", err)
	}
}

func TestEnvelopeRewrapsToCurrentKeyVersionWithoutProviderPairing(t *testing.T) {
	wrapper := &fakeWrapper{keyID: "test-key", version: 1}
	manager := mustManager(t, wrapper)
	scope := Scope{TenantID: "tenant-a", ConnectionID: "connection-1", Provider: "gmessages"}
	envelope, err := manager.Seal(context.Background(), scope, []byte("durable-session"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	originalCiphertext := append([]byte(nil), envelope.Ciphertext...)
	wrapper.version = 2
	rotated, changed, err := manager.Rewrap(context.Background(), scope, envelope)
	if err != nil {
		t.Fatalf("Rewrap: %v", err)
	}
	if !changed || rotated.KeyVersion != 2 || !bytes.Equal(rotated.Ciphertext, originalCiphertext) {
		t.Fatalf("rotated envelope = %#v, changed=%v", rotated, changed)
	}
	restored, err := manager.Open(context.Background(), scope, rotated)
	if err != nil || string(restored) != "durable-session" {
		t.Fatalf("Open rotated = %q, %v", restored, err)
	}
}

func mustManager(t *testing.T, wrapper KeyWrapper) *Manager {
	t.Helper()
	manager, err := NewManager(wrapper)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return manager
}

type fakeWrapper struct {
	keyID              string
	version            int
	wrapErr, unwrapErr error
}

type countingWrapper struct {
	fakeWrapper
	unwrapCalls int
}

func (wrapper *countingWrapper) UnwrapKey(ctx context.Context, wrapped WrappedKey) ([]byte, error) {
	wrapper.unwrapCalls++
	return wrapper.fakeWrapper.UnwrapKey(ctx, wrapped)
}

func (wrapper *fakeWrapper) WrapKey(_ context.Context, key []byte) (WrappedKey, error) {
	if wrapper.wrapErr != nil {
		return WrappedKey{}, wrapper.wrapErr
	}
	wrapped := make([]byte, len(key)+1)
	wrapped[0] = byte(wrapper.version)
	for index := range key {
		wrapped[index+1] = key[index] ^ byte(wrapper.version)
	}
	return WrappedKey{KeyID: wrapper.keyID, KeyVersion: wrapper.version, Ciphertext: wrapped}, nil
}

func (wrapper *fakeWrapper) UnwrapKey(_ context.Context, wrapped WrappedKey) ([]byte, error) {
	if wrapper.unwrapErr != nil {
		return nil, wrapper.unwrapErr
	}
	if wrapped.KeyID != wrapper.keyID || len(wrapped.Ciphertext) != 33 || int(wrapped.Ciphertext[0]) != wrapped.KeyVersion {
		return nil, errors.New("invalid wrapped key")
	}
	key := make([]byte, 32)
	for index := range key {
		key[index] = wrapped.Ciphertext[index+1] ^ byte(wrapped.KeyVersion)
	}
	return key, nil
}
