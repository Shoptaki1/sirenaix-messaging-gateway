package localkey_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/localkey"
)

func TestDevelopmentWrapperRoundTripsAndAuthenticatesDEKs(t *testing.T) {
	master := bytes.Repeat([]byte{1}, 32)
	wrapper, err := localkey.New(localkey.Config{MasterKey: master})
	if err != nil {
		t.Fatal(err)
	}
	for index := range master {
		master[index] = 0
	}
	plaintext := bytes.Repeat([]byte{2}, 32)
	wrapped, err := wrapper.WrapKey(context.Background(), plaintext)
	if err != nil || wrapped.KeyID != "local-development" || wrapped.KeyVersion != 1 || bytes.Contains(wrapped.Ciphertext, plaintext) {
		t.Fatalf("WrapKey() = (%+v, %v)", wrapped, err)
	}
	opened, err := wrapper.UnwrapKey(context.Background(), wrapped)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("UnwrapKey() = (%x, %v)", opened, err)
	}
	wrapped.Ciphertext[len(wrapped.Ciphertext)-1] ^= 1
	if _, err = wrapper.UnwrapKey(context.Background(), wrapped); !errors.Is(err, session.ErrKeyWrapper) || strings.Contains(err.Error(), string(plaintext)) {
		t.Fatalf("tampered UnwrapKey() error = %v", err)
	}
}

func TestDevelopmentWrapperRejectsInvalidConfigurationAndMetadata(t *testing.T) {
	if _, err := localkey.New(localkey.Config{MasterKey: make([]byte, 31)}); err == nil {
		t.Fatal("short development master key accepted")
	}
	wrapper, err := localkey.New(localkey.Config{MasterKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	for _, wrapped := range []session.WrappedKey{
		{KeyID: "wrong", KeyVersion: 1, Ciphertext: make([]byte, 60)},
		{KeyID: "local-development", KeyVersion: 2, Ciphertext: make([]byte, 60)},
		{KeyID: "local-development", KeyVersion: 1, Ciphertext: make([]byte, 1)},
	} {
		if _, err = wrapper.UnwrapKey(context.Background(), wrapped); !errors.Is(err, session.ErrKeyWrapper) {
			t.Fatalf("invalid wrapped metadata error = %v", err)
		}
	}
}
