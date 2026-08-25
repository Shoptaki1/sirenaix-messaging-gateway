package domain

import "fmt"

// MessageState is the externally visible, audit-derived message state. Relay
// acceptance and phone acceptance deliberately remain below carrier-sent.
type MessageState string

const (
	MessageStateQueued           MessageState = "queued"
	MessageStateDispatching      MessageState = "dispatching"
	MessageStateProviderAccepted MessageState = "provider_accepted"
	MessageStateAwaitingPhone    MessageState = "awaiting_phone"
	MessageStateSent             MessageState = "sent"
	MessageStateDelivered        MessageState = "delivered"
	MessageStateRead             MessageState = "read"
	MessageStateUncertain        MessageState = "uncertain"
	MessageStateFailed           MessageState = "failed"
)

func (state MessageState) Validate() error {
	switch state {
	case MessageStateQueued, MessageStateDispatching, MessageStateProviderAccepted,
		MessageStateAwaitingPhone, MessageStateSent, MessageStateDelivered,
		MessageStateRead, MessageStateUncertain, MessageStateFailed:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidMessageState, state)
	}
}

// DeriveMessageState folds immutable history without allowing an old provider
// receipt to lower the current state. Uncertain is intentionally reconcilable
// by later remote echo/status evidence.
func DeriveMessageState(history []MessageState) MessageState {
	current := MessageState("")
	currentRank := -1
	for _, observed := range history {
		rank, valid := messageStateRank(observed)
		if !valid {
			continue
		}
		switch observed {
		case MessageStateUncertain:
			if currentRank < messageStateRankMust(MessageStateSent) {
				current = observed
				currentRank = messageStateRankMust(MessageStateUncertain)
			}
		case MessageStateFailed:
			if currentRank < messageStateRankMust(MessageStateSent) {
				current = observed
				currentRank = messageStateRankMust(MessageStateFailed)
			}
		default:
			if current == MessageStateUncertain || (current != MessageStateFailed && rank >= currentRank) {
				current, currentRank = observed, rank
			}
		}
	}
	return current
}

func messageStateRank(state MessageState) (int, bool) {
	switch state {
	case MessageStateQueued:
		return 0, true
	case MessageStateDispatching:
		return 1, true
	case MessageStateProviderAccepted:
		return 2, true
	case MessageStateAwaitingPhone:
		return 3, true
	case MessageStateSent:
		return 4, true
	case MessageStateDelivered:
		return 5, true
	case MessageStateRead:
		return 6, true
	case MessageStateUncertain:
		return 2, true
	case MessageStateFailed:
		return 7, true
	default:
		return 0, false
	}
}

func messageStateRankMust(state MessageState) int {
	rank, _ := messageStateRank(state)
	return rank
}
