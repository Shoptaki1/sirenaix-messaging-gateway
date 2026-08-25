package kafka

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const (
	partitionKeyHeader = "sirenaix-partition-key"
	correlationHeader  = "sirenaix-correlation-id"
)

var ErrInvalidFranzConfig = errors.New("invalid secure kafka configuration")
var ErrUnmappedCommandRecord = errors.New("kafka command record has no trusted tenant binding")
var ErrKafkaMetadataUnavailable = errors.New("kafka readiness metadata unavailable")
var ErrKafkaTopicMissing = errors.New("required kafka topic unavailable")
var ErrKafkaTopicUnauthorized = errors.New("required kafka topic authorization unavailable")
var ErrKafkaClusterUnauthorized = errors.New("required kafka cluster authorization unavailable")
var ErrKafkaGroupUnauthorized = errors.New("required kafka consumer group authorization unavailable")
var ErrKafkaAuthorizationUnverifiable = errors.New("kafka readiness authorization unverifiable")

type FranzConfig struct {
	Brokers              []string
	ClientID             string
	GroupID              string
	CommandsTopic        string
	CommandTopics        map[string]TopicBinding
	SharedCommandTopic   string
	SharedAuthenticator  RecordAuthenticator
	TLSConfig            *tls.Config
	SASL                 sasl.Mechanism
	OnSecurityQuarantine func(topic string, partition int32, offset int64)
}

type FranzAdapter struct {
	client              *kgo.Client
	metadata            franzMetadataRequester
	readinessTopics     map[string]kafkaTopicAccess
	readinessGroupID    string
	commandTopics       map[string]TopicBinding
	sharedCommandTopic  string
	sharedAuthenticator RecordAuthenticator
	securityQuarantine  func(string, int32, int64)
}

type franzMetadataRequester interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}

type kafkaTopicAccess uint8

const (
	kafkaTopicDescribe kafkaTopicAccess = 1 << iota
	kafkaTopicRead
	kafkaTopicWrite
	kafkaReadinessTimeout = 5 * time.Second
)

// NewFranzAdapter returns enabled=false when no brokers are configured. It
// never substitutes an in-memory or unauthenticated broker path.
func NewFranzAdapter(config FranzConfig) (*FranzAdapter, bool, error) {
	if len(config.Brokers) == 0 {
		return nil, false, nil
	}
	if config.ClientID == "" || config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || config.CommandsTopic != "" {
		return nil, false, ErrInvalidFranzConfig
	}
	for _, broker := range config.Brokers {
		host, port, err := net.SplitHostPort(broker)
		if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
			return nil, false, ErrInvalidFranzConfig
		}
	}
	if _, collides := config.CommandTopics[DefaultEventsTopic]; collides || config.SharedCommandTopic == DefaultEventsTopic {
		return nil, false, ErrInvalidFranzConfig
	}
	commandMode := len(config.CommandTopics) > 0 || config.SharedCommandTopic != ""
	if commandMode && (config.GroupID == "" || config.OnSecurityQuarantine == nil || (config.SASL == nil && !hasClientCertificate(config.TLSConfig))) {
		return nil, false, ErrInvalidFranzConfig
	}
	if config.SharedCommandTopic != "" && config.SharedAuthenticator == nil {
		return nil, false, ErrInvalidFranzConfig
	}
	commandTopics := make(map[string]TopicBinding, len(config.CommandTopics))
	readinessTopics := map[string]kafkaTopicAccess{
		DefaultEventsTopic: kafkaTopicDescribe | kafkaTopicWrite,
	}
	topics := make([]string, 0, len(config.CommandTopics)+1)
	for topic, binding := range config.CommandTopics {
		if topic == "" || len(topic) > 249 || binding.TenantID == "" || binding.Principal == "" || len(binding.Principal) > 256 || topic == config.SharedCommandTopic {
			return nil, false, ErrInvalidFranzConfig
		}
		commandTopics[topic] = binding
		readinessTopics[topic] |= kafkaTopicDescribe | kafkaTopicRead
		topics = append(topics, topic)
	}
	if config.SharedCommandTopic != "" {
		if strings.TrimSpace(config.SharedCommandTopic) != config.SharedCommandTopic || len(config.SharedCommandTopic) > 249 {
			return nil, false, ErrInvalidFranzConfig
		}
		topics = append(topics, config.SharedCommandTopic)
		readinessTopics[config.SharedCommandTopic] |= kafkaTopicDescribe | kafkaTopicRead
	}
	tlsConfig := config.TLSConfig.Clone()
	if tlsConfig.MinVersion == 0 || tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
		kgo.DialTLSConfig(tlsConfig),
		kgo.DialTimeout(5 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.FetchMaxBytes(2 << 20),
		kgo.BrokerMaxReadBytes(4 << 20),
		kgo.BrokerMaxWriteBytes(4 << 20),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(10),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.ProducerBatchMaxBytes(2 << 20),
		kgo.RecordPartitioner(tuplePartitioner()),
	}
	if commandMode {
		options = append(options, kgo.ConsumerGroup(config.GroupID), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit())
	}
	if config.SASL != nil {
		options = append(options, kgo.SASL(config.SASL))
	}
	readinessGroupID := ""
	if commandMode {
		readinessGroupID = config.GroupID
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, false, fmt.Errorf("create Kafka client: %w", err)
	}
	return &FranzAdapter{
		client: client, metadata: client, readinessTopics: readinessTopics,
		readinessGroupID: readinessGroupID,
		commandTopics:    commandTopics, sharedCommandTopic: config.SharedCommandTopic,
		sharedAuthenticator: config.SharedAuthenticator, securityQuarantine: config.OnSecurityQuarantine,
	}, true, nil
}

func hasClientCertificate(config *tls.Config) bool {
	return len(config.Certificates) > 0 || config.GetClientCertificate != nil
}

func (adapter *FranzAdapter) Close() {
	if adapter != nil && adapter.client != nil {
		adapter.client.Close()
	}
}

// Check verifies the exact topic set without publishing a probe record. Kafka
// metadata reports existence, describe authorization, and (on supported
// brokers) the read/write operations authorized for each topic. Failure values
// deliberately contain neither broker nor topic identifiers.
func (adapter *FranzAdapter) Check(ctx context.Context) error {
	if adapter == nil || adapter.metadata == nil || len(adapter.readinessTopics) == 0 || ctx == nil {
		return ErrInvalidFranzConfig
	}
	checkCtx, cancel := context.WithTimeout(ctx, kafkaReadinessTimeout)
	defer cancel()

	names := make([]string, 0, len(adapter.readinessTopics))
	for name := range adapter.readinessTopics {
		names = append(names, name)
	}
	sort.Strings(names)
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	request.IncludeTopicAuthorizedOperations = true
	for _, name := range names {
		topicName := name
		request.Topics = append(request.Topics, kmsg.MetadataRequestTopic{Topic: &topicName})
	}
	responseMessage, err := adapter.metadata.Request(checkCtx, request)
	if err != nil {
		if errors.Is(err, kerr.UnsupportedVersion) {
			return ErrKafkaAuthorizationUnverifiable
		}
		return ErrKafkaMetadataUnavailable
	}
	response, ok := responseMessage.(*kmsg.MetadataResponse)
	if !ok || response == nil || response.ErrorCode != 0 {
		return ErrKafkaMetadataUnavailable
	}
	seen := make(map[string]struct{}, len(response.Topics))
	for _, topic := range response.Topics {
		if topic.Topic == nil {
			return ErrKafkaMetadataUnavailable
		}
		name := *topic.Topic
		required, expected := adapter.readinessTopics[name]
		if !expected {
			return ErrKafkaMetadataUnavailable
		}
		if _, duplicate := seen[name]; duplicate {
			return ErrKafkaMetadataUnavailable
		}
		seen[name] = struct{}{}
		if readinessErr := classifyKafkaMetadataError(topic.ErrorCode); readinessErr != nil {
			return readinessErr
		}
		if len(topic.Partitions) == 0 {
			return ErrKafkaTopicMissing
		}
		for _, partition := range topic.Partitions {
			if readinessErr := classifyKafkaMetadataError(partition.ErrorCode); readinessErr != nil {
				return readinessErr
			}
		}
		if topic.AuthorizedOperations == math.MinInt32 {
			return ErrKafkaAuthorizationUnverifiable
		}
		if required&kafkaTopicDescribe != 0 && !kafkaOperationAllowed(topic.AuthorizedOperations, kmsg.ACLOperationDescribe) ||
			required&kafkaTopicRead != 0 && !kafkaOperationAllowed(topic.AuthorizedOperations, kmsg.ACLOperationRead) ||
			required&kafkaTopicWrite != 0 && !kafkaOperationAllowed(topic.AuthorizedOperations, kmsg.ACLOperationWrite) {
			return ErrKafkaTopicUnauthorized
		}
	}
	if len(seen) != len(adapter.readinessTopics) {
		return ErrKafkaTopicMissing
	}
	if err = adapter.checkClusterAuthorization(checkCtx); err != nil {
		return err
	}
	if adapter.readinessGroupID != "" {
		return adapter.checkConsumerGroupAuthorization(checkCtx)
	}
	return nil
}

func (adapter *FranzAdapter) checkClusterAuthorization(ctx context.Context) error {
	request := kmsg.NewPtrDescribeClusterRequest()
	request.IncludeClusterAuthorizedOperations = true
	responseMessage, err := adapter.metadata.Request(ctx, request)
	if err != nil {
		if errors.Is(err, kerr.UnsupportedVersion) {
			return ErrKafkaAuthorizationUnverifiable
		}
		return ErrKafkaMetadataUnavailable
	}
	response, ok := responseMessage.(*kmsg.DescribeClusterResponse)
	if !ok || response == nil {
		return ErrKafkaMetadataUnavailable
	}
	switch kerr.ErrorForCode(response.ErrorCode) {
	case nil:
	case kerr.UnsupportedVersion:
		return ErrKafkaAuthorizationUnverifiable
	case kerr.ClusterAuthorizationFailed:
		return ErrKafkaClusterUnauthorized
	default:
		return ErrKafkaMetadataUnavailable
	}
	if response.ClusterAuthorizedOperations == math.MinInt32 {
		return ErrKafkaAuthorizationUnverifiable
	}
	if !kafkaOperationAllowed(response.ClusterAuthorizedOperations, kmsg.ACLOperationDescribe) ||
		!kafkaOperationAllowed(response.ClusterAuthorizedOperations, kmsg.ACLOperationIdempotentWrite) {
		return ErrKafkaClusterUnauthorized
	}
	return nil
}

func (adapter *FranzAdapter) checkConsumerGroupAuthorization(ctx context.Context) error {
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{adapter.readinessGroupID}
	request.IncludeAuthorizedOperations = true
	responseMessage, err := adapter.metadata.Request(ctx, request)
	if err != nil {
		if errors.Is(err, kerr.UnsupportedVersion) {
			return ErrKafkaAuthorizationUnverifiable
		}
		return ErrKafkaMetadataUnavailable
	}
	response, ok := responseMessage.(*kmsg.DescribeGroupsResponse)
	if !ok || response == nil || len(response.Groups) != 1 || response.Groups[0].Group != adapter.readinessGroupID {
		return ErrKafkaMetadataUnavailable
	}
	group := response.Groups[0]
	switch kerr.ErrorForCode(group.ErrorCode) {
	case nil, kerr.GroupIDNotFound:
	case kerr.UnsupportedVersion:
		return ErrKafkaAuthorizationUnverifiable
	case kerr.GroupAuthorizationFailed:
		return ErrKafkaGroupUnauthorized
	default:
		return ErrKafkaMetadataUnavailable
	}
	if group.AuthorizedOperations == math.MinInt32 {
		return ErrKafkaAuthorizationUnverifiable
	}
	if !kafkaOperationAllowed(group.AuthorizedOperations, kmsg.ACLOperationRead) {
		return ErrKafkaGroupUnauthorized
	}
	return nil
}

func classifyKafkaMetadataError(code int16) error {
	switch kerr.ErrorForCode(code) {
	case nil:
		return nil
	case kerr.UnknownTopicOrPartition:
		return ErrKafkaTopicMissing
	case kerr.TopicAuthorizationFailed:
		return ErrKafkaTopicUnauthorized
	default:
		return ErrKafkaMetadataUnavailable
	}
}

func kafkaOperationAllowed(bits int32, operation kmsg.ACLOperation) bool {
	return bits&(1<<uint32(kmsg.ACLOperationAll)) != 0 || bits&(1<<uint32(operation)) != 0
}

func (adapter *FranzAdapter) Publish(ctx context.Context, record EventRecord) error {
	publisher, _ := NewFranzEventPublisher(adapter.client)
	return publisher.Publish(ctx, record)
}

func (adapter *FranzAdapter) Commit(ctx context.Context, record CommandRecord) error {
	if adapter == nil || adapter.client == nil {
		return ErrInvalidFranzConfig
	}
	return adapter.client.CommitRecords(ctx, &kgo.Record{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
	})
}

type RecordAuthenticator interface {
	Authenticate(context.Context, *kgo.Record) (string, error)
}

func (adapter *FranzAdapter) RunCommands(ctx context.Context, consumer *CommandConsumer) error {
	if adapter == nil || adapter.client == nil || consumer == nil || (len(adapter.commandTopics) == 0 && adapter.sharedCommandTopic == "") {
		return ErrInvalidFranzConfig
	}
	for ctx.Err() == nil {
		fetches := adapter.client.PollFetches(ctx)
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			return fetchErrors[0].Err
		}
		if err := adapter.processCommandFetch(ctx, fetches.Records(), func(record CommandRecord) error {
			return consumer.Handle(ctx, record)
		}, func(topic string, partition int32) {
			adapter.client.PauseFetchPartitions(map[string][]int32{topic: {partition}})
		}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// processCommandFetch treats an unauthenticated record as a partition barrier.
// franz-go can return later offsets from the same partition in the same fetch;
// handling one of those after pausing would allow its commit to skip the poison
// offset. Other partitions remain available so one hostile producer cannot
// stop every configured tenant.
func (adapter *FranzAdapter) processCommandFetch(
	ctx context.Context,
	records []*kgo.Record,
	handle func(CommandRecord) error,
	pause func(string, int32),
) error {
	type topicPartition struct {
		topic     string
		partition int32
	}
	blocked := make(map[topicPartition]struct{})
	for _, record := range records {
		if record == nil {
			return ErrInvalidFranzConfig
		}
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		if _, quarantined := blocked[key]; quarantined {
			continue
		}
		trusted, err := adapter.trustedCommandRecord(ctx, record)
		if err != nil {
			if errors.Is(err, ErrUnmappedCommandRecord) {
				blocked[key] = struct{}{}
				pause(record.Topic, record.Partition)
				adapter.securityQuarantine(record.Topic, record.Partition, record.Offset)
				continue
			}
			return err
		}
		if err = handle(trusted); err != nil {
			return err
		}
	}
	return nil
}

func (adapter *FranzAdapter) trustedCommandRecord(ctx context.Context, record *kgo.Record) (CommandRecord, error) {
	if adapter == nil || record == nil {
		return CommandRecord{}, ErrInvalidFranzConfig
	}
	principal := ""
	if binding, mapped := adapter.commandTopics[record.Topic]; mapped {
		principal = binding.Principal
	} else if record.Topic == adapter.sharedCommandTopic && adapter.sharedAuthenticator != nil {
		if len(record.Value) == 0 || len(record.Value) > maxCommandBytes {
			return CommandRecord{}, ErrUnmappedCommandRecord
		}
		var err error
		principal, err = adapter.sharedAuthenticator.Authenticate(ctx, record)
		if err != nil {
			return CommandRecord{}, ErrUnmappedCommandRecord
		}
	}
	if principal == "" {
		return CommandRecord{}, ErrUnmappedCommandRecord
	}
	return CommandRecord{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Principal: principal, CorrelationID: recordHeader(record, correlationHeader, 256), Value: append([]byte(nil), record.Value...)}, nil
}

const (
	signedKeyIDHeader     = "sirenaix-key-id"
	signedTimestampHeader = "sirenaix-timestamp"
	signedTenantHeader    = "sirenaix-tenant-id"
	signedPrincipalHeader = "sirenaix-principal"
	signedSignatureHeader = "sirenaix-signature-v1"
)

type SignedCommandIdentity struct {
	TenantID  domain.TenantID
	Principal string
	Secret    []byte
}

type HMACRecordAuthenticator struct {
	keys    map[string]SignedCommandIdentity
	now     func() time.Time
	maxSkew time.Duration
}

func NewHMACRecordAuthenticator(keys map[string]SignedCommandIdentity) (*HMACRecordAuthenticator, error) {
	if len(keys) == 0 {
		return nil, ErrInvalidFranzConfig
	}
	authenticator := &HMACRecordAuthenticator{keys: make(map[string]SignedCommandIdentity, len(keys)), now: time.Now, maxSkew: 5 * time.Minute}
	for keyID, identity := range keys {
		if keyID == "" || len(keyID) > 128 || identity.TenantID == "" || identity.Principal == "" || len(identity.Secret) < 32 {
			return nil, ErrInvalidFranzConfig
		}
		identity.Secret = append([]byte(nil), identity.Secret...)
		authenticator.keys[keyID] = identity
	}
	return authenticator, nil
}

func (authenticator *HMACRecordAuthenticator) Authenticate(_ context.Context, record *kgo.Record) (string, error) {
	if authenticator == nil || record == nil {
		return "", ErrInvalidFranzConfig
	}
	keyID, ok := uniqueRecordHeader(record, signedKeyIDHeader, 128)
	if !ok {
		return "", ErrInvalidFranzConfig
	}
	identity, ok := authenticator.keys[keyID]
	if !ok {
		return "", ErrInvalidFranzConfig
	}
	timestampText, timestampOK := uniqueRecordHeader(record, signedTimestampHeader, 32)
	tenant, tenantOK := uniqueRecordHeader(record, signedTenantHeader, 256)
	principal, principalOK := uniqueRecordHeader(record, signedPrincipalHeader, 256)
	signatureText, signatureOK := uniqueRecordHeader(record, signedSignatureHeader, 128)
	timestamp, err := strconv.ParseInt(timestampText, 10, 64)
	if !timestampOK || !tenantOK || !principalOK || !signatureOK || err != nil || tenant != string(identity.TenantID) || principal != identity.Principal {
		return "", ErrInvalidFranzConfig
	}
	observedAt := time.Unix(timestamp, 0)
	if delta := authenticator.now().Sub(observedAt); delta > authenticator.maxSkew || delta < -authenticator.maxSkew {
		return "", ErrInvalidFranzConfig
	}
	signature, err := base64.RawURLEncoding.DecodeString(signatureText)
	if err != nil || len(signature) != sha256.Size {
		return "", ErrInvalidFranzConfig
	}
	mac := hmac.New(sha256.New, identity.Secret)
	fmt.Fprintf(mac, "v1\n%s\n%s\n%s\n%s\n", record.Topic, timestampText, tenant, principal)
	mac.Write(record.Value)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return "", ErrInvalidFranzConfig
	}
	return identity.Principal, nil
}

func uniqueRecordHeader(record *kgo.Record, key string, maxBytes int) (string, bool) {
	value := ""
	found := false
	for _, header := range record.Headers {
		if header.Key != key {
			continue
		}
		if found || len(header.Value) == 0 || len(header.Value) > maxBytes {
			return "", false
		}
		value, found = string(header.Value), true
	}
	return value, found
}

type franzProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type FranzEventPublisher struct{ producer franzProducer }

func NewFranzEventPublisher(producer franzProducer) (*FranzEventPublisher, error) {
	if producer == nil {
		return nil, ErrInvalidFranzConfig
	}
	return &FranzEventPublisher{producer: producer}, nil
}

func (publisher *FranzEventPublisher) Publish(ctx context.Context, record EventRecord) error {
	if record.Topic == "" || len(record.Key) == 0 || len(record.PartitionKey) == 0 || len(record.Value) == 0 || len(record.Value) > 1<<20 {
		return ErrInvalidFranzConfig
	}
	result := publisher.producer.ProduceSync(ctx, &kgo.Record{
		Topic: record.Topic,
		Key:   append([]byte(nil), record.Key...),
		Value: append([]byte(nil), record.Value...),
		Headers: []kgo.RecordHeader{{
			Key: partitionKeyHeader, Value: append([]byte(nil), record.PartitionKey...),
		}},
	})
	return result.FirstErr()
}

func tuplePartitioner() kgo.Partitioner {
	return kgo.BasicConsistentPartitioner(func(string) func(*kgo.Record, int) int {
		return func(record *kgo.Record, partitions int) int {
			if partitions <= 1 {
				return 0
			}
			partitionKey := []byte(nil)
			for _, header := range record.Headers {
				if header.Key == partitionKeyHeader {
					partitionKey = header.Value
					break
				}
			}
			if len(partitionKey) == 0 {
				partitionKey = record.Key
			}
			digest := sha256.Sum256(partitionKey)
			return int(binary.BigEndian.Uint64(digest[:8]) % uint64(partitions))
		}
	})
}

func recordHeader(record *kgo.Record, key string, maxBytes int) string {
	for _, header := range record.Headers {
		if header.Key == key && len(header.Value) <= maxBytes {
			return string(header.Value)
		}
	}
	return ""
}

var _ EventPublisher = (*FranzAdapter)(nil)
var _ OffsetCommitter = (*FranzAdapter)(nil)
