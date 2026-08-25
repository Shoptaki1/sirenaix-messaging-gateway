package domain_test

import (
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestMessageStateDerivationNeverRegressesReceipts(t *testing.T) {
	history := []domain.MessageState{
		domain.MessageStateQueued,
		domain.MessageStateDispatching,
		domain.MessageStateProviderAccepted,
		domain.MessageStateAwaitingPhone,
		domain.MessageStateSent,
		domain.MessageStateDelivered,
		domain.MessageStateSent,
		domain.MessageStateRead,
		domain.MessageStateDelivered,
	}
	if got, want := domain.DeriveMessageState(history), domain.MessageStateRead; got != want {
		t.Fatalf("DeriveMessageState() = %q, want %q", got, want)
	}
}

func TestMessageStateDerivationAllowsUncertainToReconcile(t *testing.T) {
	history := []domain.MessageState{
		domain.MessageStateQueued,
		domain.MessageStateDispatching,
		domain.MessageStateUncertain,
		domain.MessageStateSent,
	}
	if got, want := domain.DeriveMessageState(history), domain.MessageStateSent; got != want {
		t.Fatalf("DeriveMessageState() = %q, want %q", got, want)
	}
}

func TestMessageStateDerivationDoesNotCallRelayAcceptanceSent(t *testing.T) {
	for _, history := range [][]domain.MessageState{
		{domain.MessageStateQueued, domain.MessageStateDispatching, domain.MessageStateProviderAccepted},
		{domain.MessageStateQueued, domain.MessageStateDispatching, domain.MessageStateProviderAccepted, domain.MessageStateAwaitingPhone},
	} {
		got := domain.DeriveMessageState(history)
		if got == domain.MessageStateSent || got == domain.MessageStateDelivered || got == domain.MessageStateRead {
			t.Fatalf("relay/phone acceptance was overstated as %q", got)
		}
	}
}

func TestMessageStatesValidateCompletePublicContract(t *testing.T) {
	states := []domain.MessageState{
		domain.MessageStateQueued,
		domain.MessageStateDispatching,
		domain.MessageStateProviderAccepted,
		domain.MessageStateAwaitingPhone,
		domain.MessageStateSent,
		domain.MessageStateDelivered,
		domain.MessageStateRead,
		domain.MessageStateUncertain,
		domain.MessageStateFailed,
	}
	for _, state := range states {
		if err := state.Validate(); err != nil {
			t.Errorf("Validate(%q) = %v", state, err)
		}
	}
	if err := domain.MessageState("carrier_delivered").Validate(); err == nil {
		t.Fatal("unknown state passed validation")
	}
}
