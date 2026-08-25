package session

import (
	"context"
	"errors"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var ErrStore = errors.New("encrypted session store operation failed")

type EnvelopeStore interface {
	SaveEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, envelope Envelope) error
	LoadEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (Envelope, error)
	CompareAndSwapEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, expectedRevision uint64, envelope Envelope) (bool, error)
}

type FencedEnvelopeStore interface {
	CompareAndSwapEncryptedSessionFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken, expectedRevision uint64, envelope Envelope) (bool, error)
}

type Vault struct {
	manager *Manager
	store   EnvelopeStore
}

func NewVault(manager *Manager, store EnvelopeStore) (*Vault, error) {
	if manager == nil || store == nil {
		return nil, ErrInvalidManager
	}
	return &Vault{manager: manager, store: store}, nil
}

func (vault *Vault) Save(ctx context.Context, scope Scope, plaintext []byte) error {
	envelope, err := vault.manager.Seal(ctx, scope, plaintext)
	if err != nil {
		return err
	}
	if err := vault.store.SaveEncryptedSession(ctx, domain.TenantID(scope.TenantID), domain.ConnectionID(scope.ConnectionID), envelope); err != nil {
		return ErrStore
	}
	return nil
}

func (vault *Vault) Load(ctx context.Context, scope Scope) ([]byte, error) {
	plaintext, _, err := vault.LoadVersioned(ctx, scope)
	return plaintext, err
}

func (vault *Vault) LoadVersioned(ctx context.Context, scope Scope) ([]byte, uint64, error) {
	envelope, err := vault.store.LoadEncryptedSession(ctx, domain.TenantID(scope.TenantID), domain.ConnectionID(scope.ConnectionID))
	if err != nil {
		return nil, 0, ErrStore
	}
	if envelope.Revision == 0 {
		return nil, 0, ErrStore
	}
	plaintext, err := vault.manager.Open(ctx, scope, envelope)
	return plaintext, envelope.Revision, err
}

func (vault *Vault) CompareAndSwapFenced(ctx context.Context, scope Scope, ownerID string, fencingToken, expectedRevision uint64, plaintext []byte) (bool, error) {
	store, ok := vault.store.(FencedEnvelopeStore)
	if !ok || ownerID == "" || fencingToken == 0 || expectedRevision == 0 {
		return false, ErrStore
	}
	envelope, err := vault.manager.Seal(ctx, scope, plaintext)
	if err != nil {
		return false, err
	}
	swapped, err := store.CompareAndSwapEncryptedSessionFenced(ctx, domain.TenantID(scope.TenantID), domain.ConnectionID(scope.ConnectionID), ownerID, fencingToken, expectedRevision, envelope)
	if err != nil {
		return false, ErrStore
	}
	return swapped, nil
}

func (vault *Vault) Rotate(ctx context.Context, scope Scope) (bool, error) {
	const maxConflicts = 5
	for attempt := 0; attempt < maxConflicts; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		envelope, err := vault.store.LoadEncryptedSession(ctx, domain.TenantID(scope.TenantID), domain.ConnectionID(scope.ConnectionID))
		if err != nil {
			return false, ErrStore
		}
		rotated, changed, err := vault.manager.Rewrap(ctx, scope, envelope)
		if err != nil || !changed {
			return changed, err
		}
		swapped, err := vault.store.CompareAndSwapEncryptedSession(ctx, domain.TenantID(scope.TenantID), domain.ConnectionID(scope.ConnectionID), envelope.Revision, rotated)
		if err != nil {
			return false, ErrStore
		}
		if swapped {
			return true, nil
		}
	}
	return false, ErrStore
}
