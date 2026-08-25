package gmessages

import (
	"context"
	"errors"

	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type gatewayContactProvider interface {
	gatewayContactClient() ContactClient
}

type ActorContactProvider struct {
	executor connectionactor.ProviderExecutor
}

func NewActorContactProvider(executor connectionactor.ProviderExecutor) (*ActorContactProvider, error) {
	if executor == nil {
		return nil, ErrInvalidClient
	}
	return &ActorContactProvider{executor: executor}, nil
}

func (provider *ActorContactProvider) ListContacts(ctx context.Context, connection domain.Connection) ([]contactsync.ProviderContact, error) {
	if connection.ID == "" || connection.TenantID == "" {
		return nil, contactsync.ErrConnectionAccessDenied
	}
	key := connectionactor.Key{TenantID: connection.TenantID, ConnectionID: connection.ID}
	var contacts []contactsync.ProviderContact
	err := provider.executor.Execute(ctx, key, func(operationCtx context.Context, active connectionactor.Provider) error {
		contactProvider, ok := active.(gatewayContactProvider)
		if !ok || contactProvider.gatewayContactClient() == nil {
			return errors.New("active provider does not support contact sync")
		}
		adapter, err := New(connection, contactProvider.gatewayContactClient())
		if err != nil {
			return err
		}
		contacts, err = adapter.ListContacts(operationCtx, connection)
		return err
	})
	return contacts, err
}

var _ contactsync.Provider = (*ActorContactProvider)(nil)
