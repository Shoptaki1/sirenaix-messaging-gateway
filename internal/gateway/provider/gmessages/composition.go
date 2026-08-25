package gmessages

import (
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

// MessagingRuntimeStore is the Task 7 PostgreSQL boundary needed by the
// actor-owned provider services. A postgres.Repository satisfies it directly.
type MessagingRuntimeStore interface {
	messaging.DispatchStore
	LineResolver
	CreatedConversationStore
	BackfillCursorStore
	BackfillCheckpointStore
}

// MessagingServicesConfig is the composition surface used by the Task 8
// entrypoint. Executor must be the same connection actor whose provider
// factory owns the libgm client generation.
type MessagingServicesConfig struct {
	Executor      connectionactor.ProviderExecutor
	Store         MessagingRuntimeStore
	Media         MediaSource
	Keys          MediaKeyOpener
	OwnerID       string
	MaxMediaBytes int64
}

type MessagingServices struct {
	Sender       *ActorSender
	Dispatcher   *messaging.Dispatcher
	MediaFetcher *ActorMediaFetcher
	Backfill     *ActorBackfillWorker
}

func NewMessagingServices(config MessagingServicesConfig) (*MessagingServices, error) {
	sender, err := NewActorSender(ActorSenderConfig{
		Executor: config.Executor, Lines: config.Store, Media: config.Media,
		Routes: config.Store, MaxMediaBytes: config.MaxMediaBytes,
	})
	if err != nil {
		return nil, err
	}
	dispatcher, err := messaging.NewDispatcher(messaging.DispatchConfig{
		Store: config.Store, Sender: sender, OwnerID: config.OwnerID,
	})
	if err != nil {
		return nil, err
	}
	fetcher, err := NewActorMediaFetcher(ActorMediaFetcherConfig{
		Executor: config.Executor, Keys: config.Keys, MaxBytes: config.MaxMediaBytes,
	})
	if err != nil {
		return nil, err
	}
	backfill, err := NewActorBackfillWorker(ActorBackfillWorkerConfig{Executor: config.Executor, Cursors: config.Store, Checkpoints: config.Store})
	if err != nil {
		return nil, err
	}
	return &MessagingServices{Sender: sender, Dispatcher: dispatcher, MediaFetcher: fetcher, Backfill: backfill}, nil
}

var _ MessagingRuntimeStore = interface {
	messaging.DispatchStore
	LineResolver
	CreatedConversationStore
	BackfillCursorStore
	BackfillCheckpointStore
}(nil)

var _ media.ActorFetcher = (*ActorMediaFetcher)(nil)
