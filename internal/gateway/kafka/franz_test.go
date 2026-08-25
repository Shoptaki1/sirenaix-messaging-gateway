package kafka

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fakeFranzProducer struct{ records []*kgo.Record }

type recordAuthenticatorFunc func(context.Context, *kgo.Record) (string, error)

type fakeFranzMetadataRequester struct {
	request         *kmsg.MetadataRequest
	clusterRequest  *kmsg.DescribeClusterRequest
	groupRequest    *kmsg.DescribeGroupsRequest
	response        kmsg.Response
	clusterResponse kmsg.Response
	groupResponse   kmsg.Response
	err             error
	clusterErr      error
	groupErr        error
	deadline        time.Time
	deadlines       []time.Time
}

func (requester *fakeFranzMetadataRequester) Request(ctx context.Context, request kmsg.Request) (kmsg.Response, error) {
	deadline, _ := ctx.Deadline()
	requester.deadline = deadline
	requester.deadlines = append(requester.deadlines, deadline)
	switch typed := request.(type) {
	case *kmsg.MetadataRequest:
		requester.request = typed
		return requester.response, requester.err
	case *kmsg.DescribeClusterRequest:
		requester.clusterRequest = typed
		if requester.clusterResponse != nil || requester.clusterErr != nil {
			return requester.clusterResponse, requester.clusterErr
		}
		response := kmsg.NewPtrDescribeClusterResponse()
		response.ClusterAuthorizedOperations = kafkaAuthorizationBits(
			kmsg.ACLOperationDescribe,
			kmsg.ACLOperationIdempotentWrite,
		)
		return response, nil
	case *kmsg.DescribeGroupsRequest:
		requester.groupRequest = typed
		if requester.groupResponse != nil || requester.groupErr != nil {
			return requester.groupResponse, requester.groupErr
		}
		response := kmsg.NewPtrDescribeGroupsResponse()
		for _, groupID := range typed.Groups {
			group := kmsg.NewDescribeGroupsResponseGroup()
			group.Group = groupID
			group.AuthorizedOperations = kafkaAuthorizationBits(kmsg.ACLOperationRead)
			response.Groups = append(response.Groups, group)
		}
		return response, nil
	default:
		return nil, errors.New("unexpected Kafka readiness request")
	}
}

func (authenticate recordAuthenticatorFunc) Authenticate(ctx context.Context, record *kgo.Record) (string, error) {
	return authenticate(ctx, record)
}

func TestFranzReadinessRejectsUnconfiguredAdapter(t *testing.T) {
	if err := (*FranzAdapter)(nil).Check(context.Background()); !errors.Is(err, ErrInvalidFranzConfig) {
		t.Fatalf("nil adapter readiness error = %v, want ErrInvalidFranzConfig", err)
	}
	if err := (&FranzAdapter{}).Check(context.Background()); !errors.Is(err, ErrInvalidFranzConfig) {
		t.Fatalf("empty adapter readiness error = %v, want ErrInvalidFranzConfig", err)
	}
}

func TestFranzReadinessRequestsEveryTopicAndRequiresRoleSpecificAuthorization(t *testing.T) {
	events := DefaultEventsTopic
	commands := "sirenaix.tenant-a.commands.v1"
	requester := &fakeFranzMetadataRequester{response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
		metadataTopic(events, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationWrite), 1, 0),
		metadataTopic(commands, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationRead), 1, 0),
	}}}
	adapter := &FranzAdapter{metadata: requester, readinessTopics: map[string]kafkaTopicAccess{
		events:   kafkaTopicDescribe | kafkaTopicWrite,
		commands: kafkaTopicDescribe | kafkaTopicRead,
	}}
	if err := adapter.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	remaining := time.Until(requester.deadline)
	if requester.request == nil || !requester.request.IncludeTopicAuthorizedOperations || requester.request.AllowAutoTopicCreation ||
		requester.deadline.IsZero() || remaining <= 0 || remaining > kafkaReadinessTimeout+time.Second {
		t.Fatalf("metadata request = %+v, deadline=%v", requester.request, requester.deadline)
	}
	gotTopics := make(map[string]bool)
	for _, topic := range requester.request.Topics {
		if topic.Topic != nil {
			gotTopics[*topic.Topic] = true
		}
	}
	if len(gotTopics) != 2 || !gotTopics[events] || !gotTopics[commands] {
		t.Fatalf("requested topics = %v", gotTopics)
	}
}

func TestFranzReadinessFailsClosedForMissingUnauthorizedOrUnverifiableTopics(t *testing.T) {
	events := DefaultEventsTopic
	secret := "broker-secret.internal:9093"
	for _, test := range []struct {
		name     string
		response kmsg.Response
		err      error
		want     error
	}{
		{name: "transport", err: errors.New(secret), want: ErrKafkaMetadataUnavailable},
		{name: "metadata authorization API unsupported", err: fmt.Errorf("%w: %s", kerr.UnsupportedVersion, secret), want: ErrKafkaAuthorizationUnverifiable},
		{name: "response error", response: &kmsg.MetadataResponse{ErrorCode: kerr.UnknownServerError.Code}, want: ErrKafkaMetadataUnavailable},
		{name: "missing response topic", response: &kmsg.MetadataResponse{}, want: ErrKafkaTopicMissing},
		{name: "unknown topic", response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{metadataTopic(events, 0, 0, kerr.UnknownTopicOrPartition.Code)}}, want: ErrKafkaTopicMissing},
		{name: "topic authorization error", response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{metadataTopic(events, 0, 0, kerr.TopicAuthorizationFailed.Code)}}, want: ErrKafkaTopicUnauthorized},
		{name: "no partitions", response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{metadataTopic(events, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationWrite), 0, 0)}}, want: ErrKafkaTopicMissing},
		{name: "write denied", response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{metadataTopic(events, kafkaAuthorizationBits(kmsg.ACLOperationDescribe), 1, 0)}}, want: ErrKafkaTopicUnauthorized},
		{name: "operations unavailable", response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{metadataTopic(events, math.MinInt32, 1, 0)}}, want: ErrKafkaAuthorizationUnverifiable},
	} {
		t.Run(test.name, func(t *testing.T) {
			requester := &fakeFranzMetadataRequester{response: test.response, err: test.err}
			adapter := &FranzAdapter{metadata: requester, readinessTopics: map[string]kafkaTopicAccess{
				events: kafkaTopicDescribe | kafkaTopicWrite,
			}}
			err := adapter.Check(context.Background())
			if !errors.Is(err, test.want) || strings.Contains(fmt.Sprint(err), secret) {
				t.Fatalf("Check() error = %v, want safe %v", err, test.want)
			}
		})
	}
}

func TestFranzReadinessRequiresReadPermissionForCommandTopics(t *testing.T) {
	commands := "sirenaix.tenant-a.commands.v1"
	requester := &fakeFranzMetadataRequester{response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
		metadataTopic(commands, kafkaAuthorizationBits(kmsg.ACLOperationDescribe), 1, 0),
	}}}
	adapter := &FranzAdapter{metadata: requester, readinessTopics: map[string]kafkaTopicAccess{
		commands: kafkaTopicDescribe | kafkaTopicRead,
	}}
	if err := adapter.Check(context.Background()); !errors.Is(err, ErrKafkaTopicUnauthorized) {
		t.Fatalf("command readiness error = %v, want ErrKafkaTopicUnauthorized", err)
	}
}

func TestFranzReadinessVerifiesProducerClusterAndConsumerGroupAuthorization(t *testing.T) {
	const commands = "sirenaix.tenant-a.commands.v1"
	requester := &fakeFranzMetadataRequester{response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
		metadataTopic(DefaultEventsTopic, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationWrite), 1, 0),
		metadataTopic(commands, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationRead), 1, 0),
	}}}
	adapter := newCommandReadinessAdapter(t, requester, commands)
	if err := adapter.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if requester.clusterRequest == nil || !requester.clusterRequest.IncludeClusterAuthorizedOperations {
		t.Fatalf("cluster authorization request = %+v", requester.clusterRequest)
	}
	if requester.groupRequest == nil || !requester.groupRequest.IncludeAuthorizedOperations ||
		len(requester.groupRequest.Groups) != 1 || requester.groupRequest.Groups[0] != "sirenaix-commands" {
		t.Fatalf("consumer group authorization request = %+v", requester.groupRequest)
	}
	if len(requester.deadlines) != 3 {
		t.Fatalf("readiness request count = %d, want metadata, cluster, and group checks", len(requester.deadlines))
	}
	for _, deadline := range requester.deadlines {
		remaining := time.Until(deadline)
		if deadline.IsZero() || remaining <= 0 || remaining > kafkaReadinessTimeout+time.Second {
			t.Fatalf("unbounded readiness request deadline = %v", deadline)
		}
	}
}

func TestFranzReadinessFailsClosedWhenClusterOrGroupAuthorizationCannotBeProven(t *testing.T) {
	const commands = "sirenaix.tenant-a.commands.v1"
	secret := "group-or-cluster-secret"
	for _, test := range []struct {
		name            string
		clusterResponse kmsg.Response
		clusterErr      error
		groupResponse   kmsg.Response
		groupErr        error
		want            error
	}{
		{
			name:            "cluster describe denied",
			clusterResponse: clusterAuthorizationResponse(kafkaAuthorizationBits(kmsg.ACLOperationIdempotentWrite)),
			want:            ErrKafkaClusterUnauthorized,
		},
		{
			name:            "producer idempotent write denied",
			clusterResponse: clusterAuthorizationResponse(kafkaAuthorizationBits(kmsg.ACLOperationDescribe)),
			want:            ErrKafkaClusterUnauthorized,
		},
		{
			name:            "cluster operations not advertised",
			clusterResponse: clusterAuthorizationResponse(math.MinInt32),
			want:            ErrKafkaAuthorizationUnverifiable,
		},
		{
			name:       "cluster authorization API unsupported",
			clusterErr: fmt.Errorf("%w: %s", kerr.UnsupportedVersion, secret),
			want:       ErrKafkaAuthorizationUnverifiable,
		},
		{
			name:          "consumer group read denied",
			groupResponse: groupAuthorizationResponse("sirenaix-commands", kafkaAuthorizationBits(kmsg.ACLOperationDescribe), 0),
			want:          ErrKafkaGroupUnauthorized,
		},
		{
			name:          "consumer group operations not advertised",
			groupResponse: groupAuthorizationResponse("sirenaix-commands", math.MinInt32, 0),
			want:          ErrKafkaAuthorizationUnverifiable,
		},
		{
			name:          "consumer group authorization rejected",
			groupResponse: groupAuthorizationResponse("sirenaix-commands", 0, kerr.GroupAuthorizationFailed.Code),
			want:          ErrKafkaGroupUnauthorized,
		},
		{
			name:     "consumer group authorization API unsupported",
			groupErr: fmt.Errorf("%w: %s", kerr.UnsupportedVersion, secret),
			want:     ErrKafkaAuthorizationUnverifiable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			requester := &fakeFranzMetadataRequester{
				response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
					metadataTopic(DefaultEventsTopic, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationWrite), 1, 0),
					metadataTopic(commands, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationRead), 1, 0),
				}},
				clusterResponse: test.clusterResponse,
				clusterErr:      test.clusterErr,
				groupResponse:   test.groupResponse,
				groupErr:        test.groupErr,
			}
			adapter := newCommandReadinessAdapter(t, requester, commands)
			err := adapter.Check(context.Background())
			if !errors.Is(err, test.want) || err.Error() != test.want.Error() || strings.Contains(err.Error(), secret) {
				t.Fatalf("Check() error = %v, want fixed %v", err, test.want)
			}
		})
	}
}

func newCommandReadinessAdapter(t *testing.T, requester franzMetadataRequester, commandTopic string) *FranzAdapter {
	t.Helper()
	adapter, enabled, err := NewFranzAdapter(FranzConfig{
		Brokers:  []string{"broker.example:9093"},
		ClientID: "sirenaix",
		GroupID:  "sirenaix-commands",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{
			Certificate: [][]byte{{1}},
		}}},
		CommandTopics: map[string]TopicBinding{
			commandTopic: {TenantID: "tenant-a", Principal: "producer-a"},
		},
		OnSecurityQuarantine: func(string, int32, int64) {},
	})
	if err != nil || !enabled || adapter == nil {
		t.Fatalf("NewFranzAdapter() = (%v, %v, %v)", adapter, enabled, err)
	}
	client := adapter.client
	t.Cleanup(client.Close)
	adapter.client = nil // Readiness must use metadata APIs only; it cannot emit a record.
	adapter.metadata = requester
	return adapter
}

func clusterAuthorizationResponse(operations int32) *kmsg.DescribeClusterResponse {
	response := kmsg.NewPtrDescribeClusterResponse()
	response.ClusterAuthorizedOperations = operations
	return response
}

func groupAuthorizationResponse(groupID string, operations int32, errorCode int16) *kmsg.DescribeGroupsResponse {
	response := kmsg.NewPtrDescribeGroupsResponse()
	group := kmsg.NewDescribeGroupsResponseGroup()
	group.Group = groupID
	group.AuthorizedOperations = operations
	group.ErrorCode = errorCode
	response.Groups = append(response.Groups, group)
	return response
}

func metadataTopic(name string, operations int32, partitions int, errorCode int16) kmsg.MetadataResponseTopic {
	topic := kmsg.NewMetadataResponseTopic()
	topic.Topic = &name
	topic.AuthorizedOperations = operations
	topic.ErrorCode = errorCode
	topic.Partitions = make([]kmsg.MetadataResponseTopicPartition, partitions)
	return topic
}

func kafkaAuthorizationBits(operations ...kmsg.ACLOperation) int32 {
	var bits int32
	for _, operation := range operations {
		bits |= 1 << uint32(operation)
	}
	return bits
}

func (producer *fakeFranzProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	producer.records = append(producer.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for index, record := range records {
		results[index].Record = record
	}
	return results
}

func TestSharedCommandTopicRequiresValidTenantBoundHMAC(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	authenticator, err := NewHMACRecordAuthenticator(map[string]SignedCommandIdentity{
		"key-a": {TenantID: "tenant-a", Principal: "producer-a", Secret: secret},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0).UTC()
	authenticator.now = func() time.Time { return now }
	record := &kgo.Record{Topic: DefaultCommandsTopic, Value: []byte(`{"tenant_id":"tenant-a"}`)}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "v1\n%s\n%s\n%s\n%s\n", record.Topic, timestamp, "tenant-a", "producer-a")
	mac.Write(record.Value)
	record.Headers = []kgo.RecordHeader{
		{Key: signedKeyIDHeader, Value: []byte("key-a")},
		{Key: signedTimestampHeader, Value: []byte(timestamp)},
		{Key: signedTenantHeader, Value: []byte("tenant-a")},
		{Key: signedPrincipalHeader, Value: []byte("producer-a")},
		{Key: signedSignatureHeader, Value: []byte(base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))},
	}
	if principal, authErr := authenticator.Authenticate(context.Background(), record); authErr != nil || principal != "producer-a" {
		t.Fatalf("Authenticate = (%q, %v)", principal, authErr)
	}
	record.Value = []byte(`{"tenant_id":"tenant-b"}`)
	if _, authErr := authenticator.Authenticate(context.Background(), record); authErr == nil {
		t.Fatal("tampered shared-topic payload was authenticated")
	}
	record.Headers = append(record.Headers, kgo.RecordHeader{Key: signedTenantHeader, Value: []byte("tenant-a")})
	if _, authErr := authenticator.Authenticate(context.Background(), record); authErr == nil {
		t.Fatal("duplicate signed tenant header was authenticated")
	}
}

func TestFranzAdapterIsExplicitlyDisabledWithoutBrokersAndRequiresTLSWhenEnabled(t *testing.T) {
	adapter, enabled, err := NewFranzAdapter(FranzConfig{})
	if err != nil || enabled || adapter != nil {
		t.Fatalf("disabled adapter = (%v, %v, %v)", adapter, enabled, err)
	}
	if _, enabled, err = NewFranzAdapter(FranzConfig{Brokers: []string{"broker.example:9093"}, ClientID: "sirenaix", GroupID: "sirenaix"}); err == nil || enabled {
		t.Fatalf("TLS-less enabled adapter = (%v, %v)", enabled, err)
	}
	if _, enabled, err = NewFranzAdapter(FranzConfig{
		Brokers: []string{"broker.example:9093"}, ClientID: "sirenaix", GroupID: "sirenaix",
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // rejection fixture
	}); err == nil || enabled {
		t.Fatalf("insecure TLS adapter = (%v, %v)", enabled, err)
	}
	// A TLS configuration is cloned and hardened by the constructor. A live
	// broker is not contacted while constructing the client.
	adapter, enabled, err = NewFranzAdapter(FranzConfig{
		Brokers: []string{"broker.example:9093"}, ClientID: "sirenaix", GroupID: "sirenaix",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	})
	if err != nil || !enabled || adapter == nil {
		t.Fatalf("enabled adapter = (%v, %v, %v)", adapter, enabled, err)
	}
	adapter.Close()
}

func TestFranzCommandModeRequiresAuthenticatedPerTenantTopicBindings(t *testing.T) {
	bindings := map[string]TopicBinding{
		"sirenaix.tenant-a.commands.v1": {TenantID: "tenant-a", Principal: "producer-a"},
		"sirenaix.tenant-b.commands.v1": {TenantID: "tenant-b", Principal: "producer-b"},
	}
	base := FranzConfig{
		Brokers: []string{"broker.example:9093"}, ClientID: "sirenaix", GroupID: "sirenaix-commands",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}, CommandTopics: bindings,
		OnSecurityQuarantine: func(string, int32, int64) {},
	}
	if _, enabled, err := NewFranzAdapter(base); err == nil || enabled {
		t.Fatalf("command mode without SASL or mTLS = (%v, %v), want fail closed", enabled, err)
	}
	base.TLSConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{{1}}}}
	adapter, enabled, err := NewFranzAdapter(base)
	if err != nil || !enabled {
		t.Fatalf("mTLS command adapter = (%v, %v)", enabled, err)
	}
	defer adapter.Close()
	for topic, want := range map[string]string{
		"sirenaix.tenant-a.commands.v1": "producer-a",
		"sirenaix.tenant-b.commands.v1": "producer-b",
	} {
		record, recordErr := adapter.trustedCommandRecord(context.Background(), &kgo.Record{Topic: topic, Value: []byte("{}")})
		if recordErr != nil || record.Principal != want {
			t.Fatalf("trusted record %q = (%+v, %v)", topic, record, recordErr)
		}
	}
	requester := &fakeFranzMetadataRequester{response: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
		metadataTopic(DefaultEventsTopic, kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationWrite), 1, 0),
		metadataTopic("sirenaix.tenant-a.commands.v1", kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationRead), 1, 0),
		metadataTopic("sirenaix.tenant-b.commands.v1", kafkaAuthorizationBits(kmsg.ACLOperationDescribe, kmsg.ACLOperationRead), 1, 0),
	}}}
	adapter.metadata = requester
	if err = adapter.Check(context.Background()); err != nil || requester.request == nil || len(requester.request.Topics) != 3 {
		t.Fatalf("constructor readiness contract = (%+v, %v)", requester.request, err)
	}
	if _, err = adapter.trustedCommandRecord(context.Background(), &kgo.Record{Topic: DefaultCommandsTopic, Value: []byte("{}")}); err == nil {
		t.Fatal("unmapped shared command topic was accepted")
	}
}

func TestFranzAdapterRejectsEventsTopicCollisionBeforeMultiTenantCommandRouting(t *testing.T) {
	quarantines := 0
	base := FranzConfig{
		Brokers:  []string{"broker.example:9093"},
		ClientID: "sirenaix",
		GroupID:  "sirenaix-commands",
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{
			Certificate: [][]byte{{1}},
		}}},
		CommandTopics: map[string]TopicBinding{
			"sirenaix.tenant-a.commands.v1": {TenantID: "tenant-a", Principal: "producer-a"},
			DefaultEventsTopic:              {TenantID: "tenant-b", Principal: "producer-b"},
		},
		OnSecurityQuarantine: func(string, int32, int64) { quarantines++ },
	}
	if adapter, enabled, err := NewFranzAdapter(base); !errors.Is(err, ErrInvalidFranzConfig) || enabled || adapter != nil {
		if adapter != nil {
			adapter.Close()
		}
		t.Fatalf("mapped event/command collision = (%v, %v, %v), want rejected before client creation", adapter, enabled, err)
	}
	if quarantines != 0 {
		t.Fatalf("rejected collision reached command quarantine/DLQ path %d times", quarantines)
	}

	base.CommandTopics = map[string]TopicBinding{
		"sirenaix.tenant-a.commands.v1": {TenantID: "tenant-a", Principal: "producer-a"},
	}
	base.SharedCommandTopic = DefaultEventsTopic
	base.SharedAuthenticator = recordAuthenticatorFunc(func(context.Context, *kgo.Record) (string, error) {
		return "producer-shared", nil
	})
	if adapter, enabled, err := NewFranzAdapter(base); !errors.Is(err, ErrInvalidFranzConfig) || enabled || adapter != nil {
		if adapter != nil {
			adapter.Close()
		}
		t.Fatalf("shared event/command collision = (%v, %v, %v), want rejected before client creation", adapter, enabled, err)
	}
}

func TestFranzDerivesMappedTenantIdentityBeforePayloadBoundsAndQuarantinesUnmapped(t *testing.T) {
	adapter := &FranzAdapter{
		commandTopics: map[string]TopicBinding{
			"commands.tenant-a": {TenantID: "tenant-a", Principal: "producer-a"},
		},
	}
	oversized := &kgo.Record{Topic: "commands.tenant-a", Partition: 2, Offset: 8, Value: make([]byte, maxCommandBytes+1)}
	trusted, err := adapter.trustedCommandRecord(context.Background(), oversized)
	if err != nil || trusted.Principal != "producer-a" || len(trusted.Value) != maxCommandBytes+1 {
		t.Fatalf("mapped oversized record = (%+v, %v)", trusted, err)
	}
	if _, err = adapter.trustedCommandRecord(context.Background(), &kgo.Record{Topic: "commands.unmapped", Value: []byte("poison")}); !errors.Is(err, ErrUnmappedCommandRecord) {
		t.Fatalf("unmapped record = %v, want security quarantine", err)
	}
}

func TestFranzUnmappedPoisonStopsLaterRecordsInSamePartitionFetch(t *testing.T) {
	var quarantined, paused, handled []int64
	adapter := &FranzAdapter{
		sharedCommandTopic: "commands.shared",
		sharedAuthenticator: recordAuthenticatorFunc(func(_ context.Context, record *kgo.Record) (string, error) {
			if string(record.Value) == "bad" {
				return "", ErrInvalidFranzConfig
			}
			return "producer-a", nil
		}),
		securityQuarantine: func(_ string, _ int32, offset int64) { quarantined = append(quarantined, offset) },
	}
	records := []*kgo.Record{
		{Topic: "commands.shared", Partition: 1, Offset: 10, Value: []byte("bad")},
		{Topic: "commands.shared", Partition: 1, Offset: 11, Value: []byte("good-but-must-not-commit")},
		{Topic: "commands.shared", Partition: 2, Offset: 20, Value: []byte("good-other-partition")},
	}
	err := adapter.processCommandFetch(context.Background(), records,
		func(record CommandRecord) error {
			handled = append(handled, record.Offset)
			return nil
		},
		func(topic string, partition int32) {
			if topic == "commands.shared" && partition == 1 {
				paused = append(paused, 10)
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(quarantined) != "[10]" || fmt.Sprint(paused) != "[10]" || fmt.Sprint(handled) != "[20]" {
		t.Fatalf("quarantined=%v paused=%v handled=%v", quarantined, paused, handled)
	}
}

func TestFranzPublisherKeepsEventIDAsDedupeKeyAndPartitionTupleSeparate(t *testing.T) {
	producer := &fakeFranzProducer{}
	publisher, err := NewFranzEventPublisher(producer)
	if err != nil {
		t.Fatal(err)
	}
	partitionKey := PartitionKey("tenant-a", "connection-a", "conversation-a")
	if err = publisher.Publish(context.Background(), EventRecord{
		Topic: DefaultEventsTopic, Key: []byte("event-a"), PartitionKey: partitionKey, Value: []byte("{}"),
	}); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 1 || string(producer.records[0].Key) != "event-a" || string(producer.records[0].Value) != "{}" {
		t.Fatalf("records = %+v", producer.records)
	}
	var gotPartitionKey []byte
	for _, header := range producer.records[0].Headers {
		if header.Key == partitionKeyHeader {
			gotPartitionKey = header.Value
		}
	}
	if string(gotPartitionKey) != string(partitionKey) {
		t.Fatalf("partition header = %x, want %x", gotPartitionKey, partitionKey)
	}
	partitioner := tuplePartitioner().ForTopic(DefaultEventsTopic)
	first := partitioner.Partition(producer.records[0], 17)
	second := partitioner.Partition(producer.records[0], 17)
	if first != second || first < 0 || first >= 17 {
		t.Fatalf("partitions = %d, %d", first, second)
	}
}
