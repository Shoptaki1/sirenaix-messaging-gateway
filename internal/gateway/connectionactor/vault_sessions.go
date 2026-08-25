package connectionactor

import (
	"context"

	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

type VaultSessionStore struct {
	vault    *session.Vault
	provider string
}

func NewVaultSessionStore(vault *session.Vault, provider string) (*VaultSessionStore, error) {
	if vault == nil || provider == "" || len(provider) > 64 {
		return nil, ErrInvalidConfig
	}
	return &VaultSessionStore{vault: vault, provider: provider}, nil
}

func (store *VaultSessionStore) scope(key Key) session.Scope {
	return session.Scope{TenantID: string(key.TenantID), ConnectionID: string(key.ConnectionID), Provider: store.provider}
}

func (store *VaultSessionStore) LoadVersioned(ctx context.Context, key Key) (SessionSnapshot, error) {
	plaintext, revision, err := store.vault.LoadVersioned(ctx, store.scope(key))
	if err != nil {
		return SessionSnapshot{}, err
	}
	return SessionSnapshot{Plaintext: plaintext, Revision: revision}, nil
}

func (store *VaultSessionStore) CompareAndSwapFenced(ctx context.Context, key Key, ownerID string, fencingToken, expectedRevision uint64, plaintext []byte) (bool, error) {
	return store.vault.CompareAndSwapFenced(ctx, store.scope(key), ownerID, fencingToken, expectedRevision, plaintext)
}

var _ SessionStore = (*VaultSessionStore)(nil)
var _ Store = (*postgres.Repository)(nil)
