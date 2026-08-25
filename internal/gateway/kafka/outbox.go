package kafka

import (
	"context"
	"encoding/binary"
	"errors"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var ErrInvalidOutboxWorker = errors.New("invalid Kafka outbox worker configuration")

type OutboxEvent struct {
	EventID        string
	TenantID       domain.TenantID
	ConnectionID   domain.ConnectionID
	ConversationID string
	CanonicalBody  []byte
}

type EventRecord struct {
	Topic        string
	Key          []byte
	PartitionKey []byte
	Value        []byte
}

type EventStore interface {
	ClaimEvents(context.Context, domain.TenantID, string, int) ([]OutboxEvent, error)
	MarkPublished(context.Context, domain.TenantID, string) error
}

type EventPublisher interface {
	// Publish returns only after the broker acknowledges the record.
	Publish(context.Context, EventRecord) error
}

type OutboxWorkerConfig struct {
	Store       EventStore
	Publisher   EventPublisher
	OwnerID     string
	TenantID    domain.TenantID
	EventsTopic string
	ClaimLimit  int
}

type OutboxWorker struct {
	store      EventStore
	publisher  EventPublisher
	ownerID    string
	tenantID   domain.TenantID
	topic      string
	claimLimit int
}

func NewOutboxWorker(config OutboxWorkerConfig) (*OutboxWorker, error) {
	if config.Store == nil || config.Publisher == nil || config.OwnerID == "" || config.TenantID == "" {
		return nil, ErrInvalidOutboxWorker
	}
	if config.EventsTopic == "" {
		config.EventsTopic = DefaultEventsTopic
	}
	if config.ClaimLimit == 0 {
		config.ClaimLimit = 64
	}
	if config.ClaimLimit < 1 || config.ClaimLimit > 256 {
		return nil, ErrInvalidOutboxWorker
	}
	return &OutboxWorker{
		store: config.Store, publisher: config.Publisher, ownerID: config.OwnerID, tenantID: config.TenantID,
		topic: config.EventsTopic, claimLimit: config.ClaimLimit,
	}, nil
}

func (worker *OutboxWorker) RunBatch(ctx context.Context) error {
	events, err := worker.store.ClaimEvents(ctx, worker.tenantID, worker.ownerID, worker.claimLimit)
	if err != nil {
		return err
	}
	for _, event := range events {
		if event.TenantID != worker.tenantID {
			return errors.New("Kafka store returned a cross-tenant event")
		}
		if event.EventID == "" || event.TenantID == "" || event.ConnectionID == "" || len(event.CanonicalBody) == 0 || len(event.CanonicalBody) > 1<<20 {
			return errors.New("invalid Kafka outbox event")
		}
		if err := worker.publisher.Publish(ctx, EventRecord{
			Topic: worker.topic, Key: []byte(event.EventID),
			PartitionKey: PartitionKey(string(event.TenantID), string(event.ConnectionID), event.ConversationID),
			Value:        append([]byte(nil), event.CanonicalBody...),
		}); err != nil {
			return err
		}
		// A crash here intentionally causes a replay with the same event ID.
		if err := worker.store.MarkPublished(ctx, event.TenantID, event.EventID); err != nil {
			return err
		}
	}
	return nil
}

func PartitionKey(tenantID, connectionID, conversationID string) []byte {
	values := []string{tenantID, connectionID, conversationID}
	result := make([]byte, 0, len(tenantID)+len(connectionID)+len(conversationID)+12)
	var size [4]byte
	for _, value := range values {
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		result = append(result, size[:]...)
		result = append(result, value...)
	}
	return result
}
