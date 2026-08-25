// Package session provides provider-neutral envelope encryption for durable
// provider sessions. It intentionally has no production key-management
// default: callers must supply a KeyWrapper backed by their configured KMS.
package session

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

const (
	EnvelopeVersion   = 1
	dekSize           = 32
	gcmNonceSize      = 12
	gcmTagSize        = 16
	maxScopeIDSize    = 128
	maxProviderSize   = 64
	maxKeyIDSize      = 256
	maxWrappedSize    = 16 * 1024
	maxCiphertextSize = 4 * 1024 * 1024
	maxPlaintextSize  = maxCiphertextSize - gcmTagSize
)

var (
	ErrInvalidManager  = errors.New("session key wrapper is required")
	ErrInvalidScope    = errors.New("invalid encrypted session scope")
	ErrInvalidEnvelope = errors.New("invalid encrypted session envelope")
	ErrEncrypt         = errors.New("session encryption failed")
	ErrDecrypt         = errors.New("session decryption failed")
	ErrKeyWrapper      = errors.New("session key operation failed")
)

type Scope struct {
	TenantID     string
	ConnectionID string
	Provider     string
}

type WrappedKey struct {
	KeyID      string
	KeyVersion int
	Ciphertext []byte
}

// KeyWrapper is the complete KMS boundary. Implementations must wrap using
// their current key version and unwrap historical versions identified by the
// supplied metadata.
type KeyWrapper interface {
	WrapKey(ctx context.Context, plaintextKey []byte) (WrappedKey, error)
	UnwrapKey(ctx context.Context, wrapped WrappedKey) ([]byte, error)
}

type Envelope struct {
	Revision   uint64
	Version    int
	Provider   string
	Ciphertext []byte
	WrappedDEK []byte
	Nonce      []byte
	KeyID      string
	KeyVersion int
}

func (envelope Envelope) String() string { return "<encrypted session envelope>" }

func (envelope Envelope) GoString() string { return envelope.String() }

// MarshalJSON deliberately refuses to serialize persistence fields. Envelopes
// are stored through typed repository columns and may only be logged as a
// redacted marker.
func (envelope Envelope) MarshalJSON() ([]byte, error) {
	return []byte(`{"encrypted_session":"redacted"}`), nil
}

func (envelope Envelope) Clone() Envelope {
	envelope.Ciphertext = append([]byte(nil), envelope.Ciphertext...)
	envelope.WrappedDEK = append([]byte(nil), envelope.WrappedDEK...)
	envelope.Nonce = append([]byte(nil), envelope.Nonce...)
	return envelope
}

func (envelope Envelope) Validate() error {
	if envelope.Version != EnvelopeVersion || len(envelope.Provider) == 0 || len(envelope.Provider) > maxProviderSize ||
		len(envelope.Ciphertext) < gcmTagSize || len(envelope.Ciphertext) > maxCiphertextSize ||
		len(envelope.WrappedDEK) == 0 || len(envelope.WrappedDEK) > maxWrappedSize || len(envelope.Nonce) != gcmNonceSize ||
		len(envelope.KeyID) == 0 || len(envelope.KeyID) > maxKeyIDSize || envelope.KeyVersion <= 0 {
		return ErrInvalidEnvelope
	}
	return nil
}

type Manager struct{ wrapper KeyWrapper }

func NewManager(wrapper KeyWrapper) (*Manager, error) {
	if wrapper == nil {
		return nil, ErrInvalidManager
	}
	return &Manager{wrapper: wrapper}, nil
}

func (manager *Manager) Seal(ctx context.Context, scope Scope, plaintext []byte) (Envelope, error) {
	if !scope.valid() || len(plaintext) == 0 || len(plaintext) > maxPlaintextSize {
		return Envelope{}, ErrInvalidScope
	}
	dek := make([]byte, dekSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return Envelope{}, ErrEncrypt
	}
	defer zero(dek)

	wrapped, err := manager.wrapper.WrapKey(ctx, dek)
	if err != nil || len(wrapped.KeyID) == 0 || len(wrapped.KeyID) > maxKeyIDSize || wrapped.KeyVersion <= 0 ||
		len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > maxWrappedSize {
		return Envelope{}, ErrKeyWrapper
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return Envelope{}, ErrEncrypt
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return Envelope{}, ErrEncrypt
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return Envelope{}, ErrEncrypt
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, associatedData(EnvelopeVersion, scope))
	envelope := Envelope{
		Version: EnvelopeVersion, Provider: scope.Provider, Ciphertext: ciphertext,
		WrappedDEK: append([]byte(nil), wrapped.Ciphertext...), Nonce: nonce,
		KeyID: wrapped.KeyID, KeyVersion: wrapped.KeyVersion,
	}
	if envelope.Validate() != nil {
		return Envelope{}, ErrEncrypt
	}
	return envelope, nil
}

func (manager *Manager) Open(ctx context.Context, scope Scope, envelope Envelope) ([]byte, error) {
	plaintext, dek, err := manager.open(ctx, scope, envelope)
	zero(dek)
	return plaintext, err
}

func (manager *Manager) open(ctx context.Context, scope Scope, envelope Envelope) ([]byte, []byte, error) {
	if !scope.valid() || envelope.Validate() != nil || envelope.Provider != scope.Provider {
		return nil, nil, ErrDecrypt
	}
	dek, err := manager.wrapper.UnwrapKey(ctx, WrappedKey{
		KeyID: envelope.KeyID, KeyVersion: envelope.KeyVersion, Ciphertext: append([]byte(nil), envelope.WrappedDEK...),
	})
	if err != nil || len(dek) != dekSize {
		zero(dek)
		return nil, nil, ErrKeyWrapper
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		zero(dek)
		return nil, nil, ErrDecrypt
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(envelope.Nonce) != aead.NonceSize() {
		zero(dek)
		return nil, nil, ErrDecrypt
	}
	plaintext, err := aead.Open(nil, envelope.Nonce, envelope.Ciphertext, associatedData(envelope.Version, scope))
	if err != nil {
		zero(dek)
		return nil, nil, ErrDecrypt
	}
	if len(plaintext) == 0 || len(plaintext) > maxPlaintextSize {
		zero(plaintext)
		zero(dek)
		return nil, nil, ErrDecrypt
	}
	return plaintext, dek, nil
}

// Rewrap authenticates the envelope before asking the wrapper to wrap the DEK
// with its current key. Provider credentials are not refreshed or re-paired.
func (manager *Manager) Rewrap(ctx context.Context, scope Scope, envelope Envelope) (Envelope, bool, error) {
	plaintext, dek, err := manager.open(ctx, scope, envelope)
	if err != nil {
		return Envelope{}, false, err
	}
	zero(plaintext)
	defer zero(dek)
	wrapped, err := manager.wrapper.WrapKey(ctx, dek)
	if err != nil || len(wrapped.KeyID) == 0 || len(wrapped.KeyID) > maxKeyIDSize || wrapped.KeyVersion <= 0 ||
		len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > maxWrappedSize {
		return Envelope{}, false, ErrKeyWrapper
	}
	if wrapped.KeyID == envelope.KeyID && wrapped.KeyVersion == envelope.KeyVersion {
		return envelope.Clone(), false, nil
	}
	rotated := envelope.Clone()
	rotated.KeyID = wrapped.KeyID
	rotated.KeyVersion = wrapped.KeyVersion
	rotated.WrappedDEK = append([]byte(nil), wrapped.Ciphertext...)
	return rotated, true, nil
}

func (scope Scope) valid() bool {
	return len(scope.TenantID) > 0 && len(scope.TenantID) <= maxScopeIDSize &&
		len(scope.ConnectionID) > 0 && len(scope.ConnectionID) <= maxScopeIDSize &&
		len(scope.Provider) > 0 && len(scope.Provider) <= maxProviderSize
}

func associatedData(version int, scope Scope) []byte {
	values := []string{scope.TenantID, scope.ConnectionID, scope.Provider}
	length := 4
	for _, value := range values {
		length += 4 + len(value)
	}
	result := make([]byte, 0, length)
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(version))
	result = append(result, buffer...)
	for _, value := range values {
		binary.BigEndian.PutUint32(buffer, uint32(len(value)))
		result = append(result, buffer...)
		result = append(result, value...)
	}
	return result
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
