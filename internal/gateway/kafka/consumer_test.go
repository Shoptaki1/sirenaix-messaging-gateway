package kafka_test

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

type authorizer struct {
	tenant domain.TenantID
	err    error
}

func (authorizer authorizer) Authorize(context.Context, string) (domain.TenantID, error) {
	return authorizer.tenant, authorizer.err
}

type commandService struct {
	calls []struct {
		tenant domain.TenantID
		key    string
		input  messaging.SendInput
		audit  messaging.CommandAudit
	}
	err      error
	sequence *[]string
}

func (service *commandService) SubmitAuthenticatedCommand(_ context.Context, tenant domain.TenantID, key string, input messaging.SendInput, audit messaging.CommandAudit) (messaging.OutboundMessage, error) {
	service.calls = append(service.calls, struct {
		tenant domain.TenantID
		key    string
		input  messaging.SendInput
		audit  messaging.CommandAudit
	}{tenant: tenant, key: key, input: input, audit: audit})
	if service.sequence != nil {
		*service.sequence = append(*service.sequence, "application-commit")
	}
	return messaging.OutboundMessage{ID: "message-a", TenantID: tenant}, service.err
}

type offsetCommitter struct {
	calls    int
	err      error
	sequence *[]string
}

func (committer *offsetCommitter) Commit(context.Context, kafka.CommandRecord) error {
	committer.calls++
	if committer.sequence != nil {
		*committer.sequence = append(*committer.sequence, "offset-commit")
	}
	return committer.err
}

type commandDLQ struct {
	records  []kafka.CommandDLQRecord
	sequence *[]string
}

type commandMutationStore struct{ calls int }

func (store *commandMutationStore) CreateOutbound(_ context.Context, command messaging.CreateOutbound) (messaging.CreateResult, error) {
	store.calls++
	return messaging.CreateResult{Outcome: messaging.CreateInserted, Message: command.Message}, nil
}

func (*commandMutationStore) GetMessage(context.Context, domain.TenantID, domain.MessageID) (messaging.OutboundMessage, error) {
	return messaging.OutboundMessage{}, messaging.ErrNotFound
}

func (store *commandDLQ) Store(_ context.Context, record kafka.CommandDLQRecord) error {
	store.records = append(store.records, record)
	if store.sequence != nil {
		*store.sequence = append(*store.sequence, "kafka-dlq-commit")
	}
	return nil
}

func validCommandRecord() kafka.CommandRecord {
	return kafka.CommandRecord{
		Topic: kafka.DefaultCommandsTopic, Partition: 2, Offset: 8, Principal: "producer-a", CorrelationID: "correlation-a",
		Value: []byte(`{"version":"v1","tenant_id":"tenant-a","idempotency_key":"idem-a","connection_id":"connection-a","conversation_id":"conversation-a","text":"hello"}`),
	}
}

func TestConsumerUsesAuthorizedTenantAndCommitsOnlyAfterSharedApplicationTransaction(t *testing.T) {
	sequence := []string{}
	service := &commandService{sequence: &sequence}
	committer := &offsetCommitter{sequence: &sequence}
	consumer, err := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer{tenant: "tenant-a"}, Commands: service, Offsets: committer, DLQ: &commandDLQ{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.Handle(context.Background(), validCommandRecord()); err != nil {
		t.Fatal(err)
	}
	if len(service.calls) != 1 || service.calls[0].tenant != "tenant-a" || service.calls[0].key != "idem-a" || committer.calls != 1 {
		t.Fatalf("service=%+v offset commits=%d", service.calls, committer.calls)
	}
	audit := service.calls[0].audit
	if audit.Topic != kafka.DefaultCommandsTopic || audit.Partition != 2 || audit.Offset != 8 || audit.ProducerIdentity != "producer-a" || audit.CorrelationID != "correlation-a" || audit.PayloadDigest == ([32]byte{}) {
		t.Fatalf("broker audit = %+v", audit)
	}
	if len(sequence) != 2 || sequence[0] != "application-commit" || sequence[1] != "offset-commit" {
		t.Fatalf("commit order = %v", sequence)
	}
}

func TestConsumerReservedProviderConversationUsesTenantDLQBeforeApplicationMutation(t *testing.T) {
	store := &commandMutationStore{}
	commands, err := messaging.NewService(messaging.Config{
		Store: store, NewID: func() string { return "018f4ca7-52c4-7c5d-ae9b-5b7a358f9741" },
	})
	if err != nil {
		t.Fatal(err)
	}
	dlq, committer := &commandDLQ{}, &offsetCommitter{}
	consumer, err := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer{tenant: "tenant-a"}, Commands: commands, Offsets: committer, DLQ: dlq,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := validCommandRecord()
	record.Value = []byte(`{"version":"v1","tenant_id":"tenant-a","idempotency_key":"reserved-route","connection_id":"connection-a","conversation_id":"_provider_page","text":"hello"}`)
	if err = consumer.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 || len(dlq.records) != 1 || committer.calls != 1 {
		t.Fatalf("reserved Kafka route mutations=%d dlq=%+v commits=%d", store.calls, dlq.records, committer.calls)
	}
}

func TestConsumerTenantMismatchUsesKafkaDLQNotPayloadTenant(t *testing.T) {
	sequence := []string{}
	service := &commandService{}
	dlq := &commandDLQ{sequence: &sequence}
	committer := &offsetCommitter{sequence: &sequence}
	consumer, _ := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer{tenant: "tenant-b"}, Commands: service, Offsets: committer, DLQ: dlq,
	})
	if err := consumer.Handle(context.Background(), validCommandRecord()); err != nil {
		t.Fatal(err)
	}
	if len(service.calls) != 0 || len(dlq.records) != 1 || dlq.records[0].AuthorizedTenant != "tenant-b" || committer.calls != 1 {
		t.Fatalf("service=%d dlq=%+v commits=%d", len(service.calls), dlq.records, committer.calls)
	}
	if len(sequence) != 2 || sequence[0] != "kafka-dlq-commit" || sequence[1] != "offset-commit" {
		t.Fatalf("DLQ/offset order = %v", sequence)
	}
}

func TestConsumerWithoutAuthenticatedTenantFailsClosedWithoutDLQOrOffsetCommit(t *testing.T) {
	service := &commandService{}
	dlq := &commandDLQ{}
	committer := &offsetCommitter{}
	consumer, _ := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer{err: errors.New("broker identity unavailable")}, Commands: service, Offsets: committer, DLQ: dlq,
	})
	if err := consumer.Handle(context.Background(), validCommandRecord()); !errors.Is(err, kafka.ErrInvalidConsumer) {
		t.Fatalf("Handle error = %v, want ErrInvalidConsumer", err)
	}
	if len(service.calls) != 0 || len(dlq.records) != 0 || committer.calls != 0 {
		t.Fatalf("unauthenticated record mutated state: service=%d dlq=%d commits=%d", len(service.calls), len(dlq.records), committer.calls)
	}
}

func TestConsumerCommitCrashRedeliveryConvergesThroughIdempotency(t *testing.T) {
	crash := errors.New("offset commit crashed")
	service := &commandService{}
	committer := &offsetCommitter{err: crash}
	consumer, _ := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer{tenant: "tenant-a"}, Commands: service, Offsets: committer, DLQ: &commandDLQ{},
	})
	record := validCommandRecord()
	if err := consumer.Handle(context.Background(), record); !errors.Is(err, crash) {
		t.Fatalf("first Handle error = %v", err)
	}
	committer.err = nil
	if err := consumer.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(service.calls) != 2 || service.calls[0].key != service.calls[1].key || committer.calls != 2 {
		t.Fatalf("redelivery calls=%+v commits=%d", service.calls, committer.calls)
	}
}

func TestTopicTenantAuthorizerKeepsTwoProducerTopicsTenantBound(t *testing.T) {
	authorizer, err := kafka.NewTopicTenantAuthorizer(map[string]kafka.TopicBinding{
		"sirenaix.tenant-a.commands.v1": {TenantID: "tenant-a", Principal: "producer-a"},
		"sirenaix.tenant-b.commands.v1": {TenantID: "tenant-b", Principal: "producer-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for principal, want := range map[string]domain.TenantID{"producer-a": "tenant-a", "producer-b": "tenant-b"} {
		if got, authErr := authorizer.Authorize(context.Background(), principal); authErr != nil || got != want {
			t.Fatalf("Authorize(%q) = (%q, %v), want %q", principal, got, authErr, want)
		}
	}
	if _, authErr := authorizer.Authorize(context.Background(), "producer-unknown"); authErr == nil {
		t.Fatal("unmapped producer was authorized")
	}
}

func TestTwoTrustedTenantTopicsRejectCrossTenantPayload(t *testing.T) {
	bindings := map[string]kafka.TopicBinding{
		"sirenaix.tenant-a.commands.v1": {TenantID: "tenant-a", Principal: "producer-a"},
		"sirenaix.tenant-b.commands.v1": {TenantID: "tenant-b", Principal: "producer-b"},
	}
	authorizer, _ := kafka.NewTopicTenantAuthorizer(bindings)
	service, dlq, committer := &commandService{}, &commandDLQ{}, &offsetCommitter{}
	consumer, err := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer, Commands: service, Offsets: committer, DLQ: dlq,
		AllowedTopics: []string{"sirenaix.tenant-a.commands.v1", "sirenaix.tenant-b.commands.v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	record := validCommandRecord()
	record.Topic, record.Principal = "sirenaix.tenant-a.commands.v1", "producer-a"
	record.Value = []byte(`{"version":"v1","tenant_id":"tenant-b","idempotency_key":"idem-cross","connection_id":"connection-b","text":"denied"}`)
	if err = consumer.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(service.calls) != 0 || len(dlq.records) != 1 || dlq.records[0].AuthorizedTenant != "tenant-a" {
		t.Fatalf("cross-tenant command service=%d dlq=%+v", len(service.calls), dlq.records)
	}
	record.Topic, record.Principal = "sirenaix.tenant-b.commands.v1", "producer-b"
	record.Value = []byte(`{"version":"v1","tenant_id":"tenant-b","idempotency_key":"idem-b","connection_id":"connection-b","text":"allowed"}`)
	if err = consumer.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(service.calls) != 1 || service.calls[0].tenant != "tenant-b" {
		t.Fatalf("tenant-b service calls = %+v", service.calls)
	}
}

func TestMappedTenantPoisonCommitsDLQAndNextTenantContinues(t *testing.T) {
	bindings := map[string]kafka.TopicBinding{
		"commands.tenant-a": {TenantID: "tenant-a", Principal: "producer-a"},
		"commands.tenant-b": {TenantID: "tenant-b", Principal: "producer-b"},
	}
	authorizer, _ := kafka.NewTopicTenantAuthorizer(bindings)
	service, dlq, committer := &commandService{}, &commandDLQ{}, &offsetCommitter{}
	consumer, _ := kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
		Authorizer: authorizer, Commands: service, Offsets: committer, DLQ: dlq,
		AllowedTopics: []string{"commands.tenant-a", "commands.tenant-b"},
	})
	poison := validCommandRecord()
	poison.Topic, poison.Principal, poison.Value = "commands.tenant-a", "producer-a", make([]byte, (1<<20)+1)
	if err := consumer.Handle(context.Background(), poison); err != nil {
		t.Fatal(err)
	}
	valid := validCommandRecord()
	valid.Topic, valid.Principal = "commands.tenant-b", "producer-b"
	valid.Value = []byte(`{"version":"v1","tenant_id":"tenant-b","idempotency_key":"idem-b-after-poison","connection_id":"connection-b","text":"healthy"}`)
	if err := consumer.Handle(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
	if len(dlq.records) != 1 || dlq.records[0].AuthorizedTenant != "tenant-a" || len(dlq.records[0].OriginalPayload) != 1<<20 ||
		len(service.calls) != 1 || service.calls[0].tenant != "tenant-b" || committer.calls != 2 {
		t.Fatalf("poison continuation: dlq=%+v service=%+v commits=%d", dlq.records, service.calls, committer.calls)
	}
}
