package gmessages

import (
	"context"
	"errors"
	"net/http"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
)

type AuthorizationFailureMarker interface {
	MarkAuthorizationFailure(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (pairing.AuthorizationTransition, error)
}

// HandleProviderError converts a concrete Google authorization failure into
// the durable, idempotent gateway transition. Non-authorization errors are
// left for the Task 6 connection actor to classify without changing state.
func HandleProviderError(ctx context.Context, marker AuthorizationFailureMarker, tenantID domain.TenantID, connectionID domain.ConnectionID, providerError error) (pairing.AuthorizationTransition, bool, error) {
	if !isAuthorizationFailure(providerError) {
		return pairing.AuthorizationTransition{}, false, nil
	}
	transition, err := marker.MarkAuthorizationFailure(ctx, tenantID, connectionID)
	return transition, true, err
}

func isAuthorizationFailure(providerError error) bool {
	if errors.Is(providerError, events.ErrInvalidCredentials) {
		return true
	}
	var httpError events.HTTPError
	return errors.As(providerError, &httpError) && httpError.StatusCode == http.StatusUnauthorized
}
