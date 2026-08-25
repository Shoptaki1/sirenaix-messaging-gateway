package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
)

type eventStore struct {
	events  []kafka.OutboxEvent
	marked  []string
	markErr error
}

func (store *eventStore) ClaimEvents(context.Context, domain.TenantID, string, int) ([]kafka.OutboxEvent, error) {
	return append([]kafka.OutboxEvent(nil), store.events...), nil
}

func (store *eventStore) MarkPublished(_ context.Context, _ domain.TenantID, eventID string) error {
	store.marked = append(store.marked, eventID)
	return store.markErr
}

type eventPublisher struct{ records []kafka.EventRecord }

func (publisher *eventPublisher) Publish(_ context.Context, record kafka.EventRecord) error {
	publisher.records = append(publisher.records, record)
	return nil
}

func TestOutboxPublisherUsesEventDedupeKeyAndDeterministicConversationPartition(t *testing.T) {
	event := kafka.OutboxEvent{
		EventID: "event-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
		CanonicalBody: []byte(`{"event_id":"event-a"}`),
	}
	store := &eventStore{events: []kafka.OutboxEvent{event}}
	publisher := &eventPublisher{}
	worker, err := kafka.NewOutboxWorker(kafka.OutboxWorkerConfig{Store: store, Publisher: publisher, OwnerID: "worker-a", TenantID: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.records) != 1 || string(publisher.records[0].Key) != "event-a" || publisher.records[0].Topic != kafka.DefaultEventsTopic ||
		!bytes.Equal(publisher.records[0].PartitionKey, kafka.PartitionKey("tenant-a", "connection-a", "conversation-a")) ||
		!bytes.Equal(publisher.records[0].Value, event.CanonicalBody) || len(store.marked) != 1 {
		t.Fatalf("published=%+v marked=%v", publisher.records, store.marked)
	}
}

func TestOutboxPublishAcknowledgmentCrashReplaysSameEventID(t *testing.T) {
	crash := errors.New("database crashed after broker acknowledgment")
	store := &eventStore{events: []kafka.OutboxEvent{{EventID: "event-a", TenantID: "tenant-a", ConnectionID: "connection-a", CanonicalBody: []byte("{}")}}, markErr: crash}
	publisher := &eventPublisher{}
	worker, _ := kafka.NewOutboxWorker(kafka.OutboxWorkerConfig{Store: store, Publisher: publisher, OwnerID: "worker-a", TenantID: "tenant-a"})
	if err := worker.RunBatch(context.Background()); !errors.Is(err, crash) {
		t.Fatalf("first RunBatch error = %v", err)
	}
	store.markErr = nil
	if err := worker.RunBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.records) != 2 || string(publisher.records[0].Key) != string(publisher.records[1].Key) {
		t.Fatalf("replayed records = %+v", publisher.records)
	}
}
