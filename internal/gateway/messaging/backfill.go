package messaging

import "errors"

// MaxBackfillConversationsPerPage is shared by provider page requests,
// checkpoint validation, and the PostgreSQL array constraint. Keeping one
// production limit prevents a page accepted by the actor from being rejected
// after provider I/O has already occurred.
const MaxBackfillConversationsPerPage = 100

// BackfillItemState is durable progress for one conversation in a provider
// conversation-list page. Poisoned children remain blocking evidence while
// healthy siblings may continue; the parent cursor never advances over them.
type BackfillItemState string

const (
	BackfillItemPending  BackfillItemState = "pending"
	BackfillItemComplete BackfillItemState = "complete"
	BackfillItemPoisoned BackfillItemState = "poisoned"
)

var (
	ErrBackfillCheckpointConflict  = errors.New("provider backfill checkpoint changed")
	ErrBackfillPoisoned            = errors.New("provider backfill page contains poison")
	ErrBackfillProviderUnavailable = errors.New("provider backfill operation unavailable")
	ErrCanonicalLaneBusy           = errors.New("provider conversation already has queued or in-flight work on another lane")
)

type BackfillItem struct {
	Ordinal        int
	ConversationID string
	State          BackfillItemState
	SafeError      string
}

type BackfillCheckpoint struct {
	ID           string
	NextCursor   []byte
	Terminal     bool
	ScanComplete bool
	Items        []BackfillItem
}

type BackfillPage struct {
	BaseCursor []byte
	NextCursor []byte
	Terminal   bool
	Items      []BackfillItem
}
