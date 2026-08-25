package gmessages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
)

func TestHandleProviderAuthorizationFailureMarksReauthorizationOnce(t *testing.T) {
	marker := &fakeAuthorizationMarker{}
	providerError := events.HTTPError{StatusCode: http.StatusUnauthorized}
	first, handled, err := HandleProviderError(context.Background(), marker, "tenant-a", "connection-1", providerError)
	if err != nil || !handled || !first.Transitioned || first.EventID != "event-stable" {
		t.Fatalf("first result = %#v, handled=%v, error=%v", first, handled, err)
	}
	second, handled, err := HandleProviderError(context.Background(), marker, "tenant-a", "connection-1", providerError)
	if err != nil || !handled || second.Transitioned || second.EventID != first.EventID || marker.notifications != 1 {
		t.Fatalf("repeat result = %#v, handled=%v, error=%v notifications=%d", second, handled, err, marker.notifications)
	}

	result, handled, err := HandleProviderError(context.Background(), marker, "tenant-a", "connection-1", errors.New("phone offline"))
	if err != nil || handled || result.EventID != "" || marker.calls != 2 {
		t.Fatalf("non-auth result = %#v, handled=%v, error=%v calls=%d", result, handled, err, marker.calls)
	}
}

type fakeAuthorizationMarker struct {
	calls, notifications int
}

func (marker *fakeAuthorizationMarker) MarkAuthorizationFailure(context.Context, domain.TenantID, domain.ConnectionID) (pairing.AuthorizationTransition, error) {
	marker.calls++
	if marker.calls == 1 {
		marker.notifications++
		return pairing.AuthorizationTransition{Transitioned: true, EventID: "event-stable"}, nil
	}
	return pairing.AuthorizationTransition{EventID: "event-stable"}, nil
}
