// Package localkey implements an explicitly development-only DEK wrapper.
// Production runtime validation rejects this type in favor of AWS KMS.
package localkey

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const (
	keyID      = "local-development"
	keyVersion = 1
	keyBytes   = 32
	dekBytes   = 32
	nonceBytes = 12
)

var ErrInvalidConfig = errors.New("invalid local development key wrapper configuration")

type Config struct {
	MasterKey []byte
	Random    io.Reader
}

type Wrapper struct {
	aead   cipher.AEAD
	random io.Reader
}

func New(config Config) (*Wrapper, error) {
	if len(config.MasterKey) != keyBytes {
		return nil, ErrInvalidConfig
	}
	block, err := aes.NewCipher(append([]byte(nil), config.MasterKey...))
	if err != nil {
		return nil, ErrInvalidConfig
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || aead.NonceSize() != nonceBytes {
		return nil, ErrInvalidConfig
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	return &Wrapper{aead: aead, random: config.Random}, nil
}

func (wrapper *Wrapper) WrapKey(ctx context.Context, plaintext []byte) (session.WrappedKey, error) {
	if wrapper == nil || wrapper.aead == nil || len(plaintext) != dekBytes || ctx == nil || ctx.Err() != nil {
		return session.WrappedKey{}, session.ErrKeyWrapper
	}
	nonce := make([]byte, wrapper.aead.NonceSize())
	if _, err := io.ReadFull(wrapper.random, nonce); err != nil {
		zero(nonce)
		return session.WrappedKey{}, session.ErrKeyWrapper
	}
	ciphertext := wrapper.aead.Seal(nonce, nonce, plaintext, []byte(keyID))
	return session.WrappedKey{KeyID: keyID, KeyVersion: keyVersion, Ciphertext: ciphertext}, nil
}

func (wrapper *Wrapper) UnwrapKey(ctx context.Context, wrapped session.WrappedKey) ([]byte, error) {
	if wrapper == nil || wrapper.aead == nil || ctx == nil || ctx.Err() != nil || wrapped.KeyID != keyID || wrapped.KeyVersion != keyVersion ||
		len(wrapped.Ciphertext) != wrapper.aead.NonceSize()+dekBytes+wrapper.aead.Overhead() {
		return nil, session.ErrKeyWrapper
	}
	nonce := wrapped.Ciphertext[:wrapper.aead.NonceSize()]
	plaintext, err := wrapper.aead.Open(nil, nonce, wrapped.Ciphertext[wrapper.aead.NonceSize():], []byte(keyID))
	if err != nil || len(plaintext) != dekBytes {
		zero(plaintext)
		return nil, session.ErrKeyWrapper
	}
	return plaintext, nil
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ session.KeyWrapper = (*Wrapper)(nil)
