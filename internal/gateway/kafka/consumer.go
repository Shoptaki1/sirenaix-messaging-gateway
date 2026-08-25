// Package kafka provides broker-neutral command and event boundaries. The
// production franz-go adapter is optional; no broker configuration disables
// that adapter instead of changing authorization behavior.
package kafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

const (
	DefaultCommandsTopic    = "sirenaix.messaging.commands.v1"
	DefaultEventsTopic      = "sirenaix.messaging.events.v1"
	DefaultCommandsDLQTopic = "sirenaix.messaging.commands.dlq.v1"
	maxCommandBytes         = 1 << 20
)

var ErrInvalidConsumer = errors.New("invalid Kafka command consumer configuration")

type CommandRecord struct {
	Topic         string
	Partition     int32
	Offset        int64
	Principal     string
	CorrelationID string
	Value         []byte
}

type CommandDLQRecord struct {
	Topic            string
	Partition        int32
	Offset           int64
	AuthorizedTenant domain.TenantID
	CorrelationID    string
	OriginalPayload  []byte
	SafeError        string
}

type TenantAuthorizer interface {
	// Authorize derives tenant identity from authenticated broker producer/ACL
	// metadata. Implementations must never trust the message value itself.
	Authorize(context.Context, string) (domain.TenantID, error)
}

type TopicBinding struct {
	TenantID  domain.TenantID
	Principal string
}

type TopicTenantAuthorizer struct{ tenants map[string]domain.TenantID }

func NewTopicTenantAuthorizer(bindings map[string]TopicBinding) (*TopicTenantAuthorizer, error) {
	if len(bindings) == 0 {
		return nil, ErrInvalidConsumer
	}
	authorizer := &TopicTenantAuthorizer{tenants: make(map[string]domain.TenantID, len(bindings))}
	for topic, binding := range bindings {
		if topic == "" || len(topic) > 249 || binding.TenantID == "" || binding.Principal == "" || len(binding.Principal) > 256 {
			return nil, ErrInvalidConsumer
		}
		if existing, duplicate := authorizer.tenants[binding.Principal]; duplicate && existing != binding.TenantID {
			return nil, ErrInvalidConsumer
		}
		authorizer.tenants[binding.Principal] = binding.TenantID
	}
	return authorizer, nil
}

func (authorizer *TopicTenantAuthorizer) Authorize(_ context.Context, principal string) (domain.TenantID, error) {
	if authorizer == nil {
		return "", ErrInvalidConsumer
	}
	tenantID := authorizer.tenants[principal]
	if tenantID == "" {
		return "", ErrInvalidConsumer
	}
	return tenantID, nil
}

type CommandService interface {
	SubmitAuthenticatedCommand(context.Context, domain.TenantID, string, messaging.SendInput, messaging.CommandAudit) (messaging.OutboundMessage, error)
}

type OffsetCommitter interface {
	Commit(context.Context, CommandRecord) error
}

type CommandDLQStore interface {
	Store(context.Context, CommandDLQRecord) error
}

type CommandConsumerConfig struct {
	Authorizer    TenantAuthorizer
	Commands      CommandService
	Offsets       OffsetCommitter
	DLQ           CommandDLQStore
	CommandsTopic string
	AllowedTopics []string
}

type CommandConsumer struct {
	authorizer TenantAuthorizer
	commands   CommandService
	offsets    OffsetCommitter
	dlq        CommandDLQStore
	topics     map[string]struct{}
}

func NewCommandConsumer(config CommandConsumerConfig) (*CommandConsumer, error) {
	if config.Authorizer == nil || config.Commands == nil || config.Offsets == nil || config.DLQ == nil {
		return nil, ErrInvalidConsumer
	}
	if len(config.AllowedTopics) == 0 {
		if config.CommandsTopic == "" {
			config.CommandsTopic = DefaultCommandsTopic
		}
		config.AllowedTopics = []string{config.CommandsTopic}
	}
	topics := make(map[string]struct{}, len(config.AllowedTopics))
	for _, topic := range config.AllowedTopics {
		if topic == "" || len(topic) > 249 {
			return nil, ErrInvalidConsumer
		}
		topics[topic] = struct{}{}
	}
	return &CommandConsumer{
		authorizer: config.Authorizer, commands: config.Commands, offsets: config.Offsets,
		dlq: config.DLQ, topics: topics,
	}, nil
}

type commandEnvelope struct {
	Version        string              `json:"version"`
	TenantID       domain.TenantID     `json:"tenant_id"`
	IdempotencyKey string              `json:"idempotency_key"`
	ConnectionID   domain.ConnectionID `json:"connection_id"`
	ConversationID string              `json:"conversation_id"`
	Recipient      string              `json:"recipient"`
	LineID         domain.LineID       `json:"line_id"`
	RouteMode      string              `json:"route_mode"`
	Text           string              `json:"text"`
	MediaIDs       []domain.MediaID    `json:"media_ids"`
}

func (consumer *CommandConsumer) Handle(ctx context.Context, record CommandRecord) error {
	if record.Principal == "" {
		return ErrInvalidConsumer
	}
	authorizedTenant, err := consumer.authorizer.Authorize(ctx, record.Principal)
	if err != nil || authorizedTenant == "" {
		// Without an authenticated tenant, assigning the payload to any
		// tenant-scoped DLQ would itself cross the authorization boundary.
		// Leave the offset uncommitted for operator/broker policy handling.
		return ErrInvalidConsumer
	}
	if _, allowed := consumer.topics[record.Topic]; !allowed || len(record.Value) == 0 || len(record.Value) > maxCommandBytes {
		return consumer.deadLetterAndCommit(ctx, record, "invalid command record", authorizedTenant, nil)
	}
	command, err := decodeCommand(record.Value)
	if err != nil {
		return consumer.deadLetterAndCommit(ctx, record, "invalid command envelope", authorizedTenant, nil)
	}
	if command.TenantID != authorizedTenant {
		return consumer.deadLetterAndCommit(ctx, record, "command tenant does not match authenticated producer", authorizedTenant, nil)
	}
	_, err = consumer.commands.SubmitAuthenticatedCommand(ctx, authorizedTenant, command.IdempotencyKey, messaging.SendInput{
		ConnectionID: command.ConnectionID, ConversationID: command.ConversationID,
		Recipient: command.Recipient, LineID: command.LineID, RouteMode: command.RouteMode,
		Text: command.Text, MediaIDs: append([]domain.MediaID(nil), command.MediaIDs...),
	}, messaging.CommandAudit{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		ProducerIdentity: record.Principal, CorrelationID: record.CorrelationID,
		PayloadDigest: sha256.Sum256(record.Value),
	})
	if err != nil {
		if errors.Is(err, messaging.ErrInvalidCommand) || errors.Is(err, messaging.ErrInvalidRoute) ||
			errors.Is(err, messaging.ErrIdempotencyConflict) {
			return consumer.deadLetterAndCommit(ctx, record, "command rejected", authorizedTenant, err)
		}
		return err
	}
	// Submit returns only after the shared application transaction committed.
	return consumer.offsets.Commit(ctx, record)
}

func decodeCommand(value []byte) (commandEnvelope, error) {
	if !utf8.Valid(value) {
		return commandEnvelope{}, errors.New("invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var command commandEnvelope
	if err := decoder.Decode(&command); err != nil {
		return commandEnvelope{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return commandEnvelope{}, errors.New("multiple JSON values")
	}
	if command.Version != "v1" || command.TenantID == "" || command.IdempotencyKey == "" || command.ConnectionID == "" {
		return commandEnvelope{}, errors.New("missing command fields")
	}
	if command.ConversationID != "" && !domain.ValidProviderConversationID(strings.TrimSpace(command.ConversationID)) {
		return commandEnvelope{}, errors.New("invalid provider conversation ID")
	}
	return command, nil
}

func (consumer *CommandConsumer) deadLetterAndCommit(ctx context.Context, record CommandRecord, safeError string, tenantID domain.TenantID, cause error) error {
	if cause != nil {
		safeError = fmt.Sprintf("%s: %v", safeError, cause)
	}
	payload := append([]byte(nil), record.Value...)
	if len(payload) > maxCommandBytes {
		payload = payload[:maxCommandBytes]
		safeError = "command payload exceeded limit"
	}
	if err := consumer.dlq.Store(ctx, CommandDLQRecord{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		AuthorizedTenant: tenantID, CorrelationID: record.CorrelationID,
		OriginalPayload: payload, SafeError: safeError,
	}); err != nil {
		return err
	}
	return consumer.offsets.Commit(ctx, record)
}
