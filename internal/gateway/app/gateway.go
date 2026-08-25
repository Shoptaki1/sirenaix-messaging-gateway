package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/internal/gateway/auth"
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/httpapi"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media/s3store"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/internal/gateway/provider/gmessages"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/awskms"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

const (
	EnvironmentProduction  = "production"
	EnvironmentDevelopment = "development"
	ObjectBackendS3        = "s3"
	ObjectBackendLocal     = "local"
)

var ErrInvalidRuntime = errors.New("invalid fail-closed gateway runtime configuration")

type RuntimeConfig struct {
	Environment                  string
	HTTPAddress                  string
	TLSCertFile                  string
	TLSKeyFile                   string
	AllowPlainHTTPBehindTLSProxy bool
	OwnerID                      string
	Tenants                      []domain.TenantID
	Repository                   *postgres.Repository
	Verifier                     auth.Verifier
	KeyWrapper                   session.KeyWrapper
	Objects                      media.ObjectStore
	ObjectBackend                string
	ProviderMediaHosts           []string
	Kafka                        *kafka.FranzAdapter
	KafkaCommandTopics           map[string]kafka.TopicBinding
	Logger                       zerolog.Logger
	PollInterval                 time.Duration
	MaxMediaBytes                int64
	MaxMediaPixels               int64
	MaxWebhookEndpoints          int
	ACKTimeout                   time.Duration
	ACKConcurrency               int
	MediaTempDirectory           string
	OnWorkerError                func(error)
	ActorMetrics                 connectionactor.MetricsSink
	HTTPMiddleware               func(http.Handler) http.Handler
}

type Gateway struct {
	server                *http.Server
	supervisor            *Supervisor
	kafkaAdapter          *kafka.FranzAdapter
	commands              *kafka.CommandConsumer
	tlsCertFile           string
	tlsKeyFile            string
	requireLocalPlaintext bool
	started               atomic.Bool
}

func ValidateRuntimeConfig(config RuntimeConfig) error {
	config.Environment = strings.TrimSpace(config.Environment)
	if config.Environment != EnvironmentProduction && config.Environment != EnvironmentDevelopment {
		return ErrInvalidRuntime
	}
	if strings.TrimSpace(config.HTTPAddress) == "" || strings.TrimSpace(config.OwnerID) == "" || len(config.OwnerID) > 220 ||
		len(config.Tenants) == 0 || config.Repository == nil || config.Verifier == nil || config.KeyWrapper == nil || config.Objects == nil ||
		(config.ObjectBackend != ObjectBackendS3 && config.ObjectBackend != ObjectBackendLocal) || len(config.ProviderMediaHosts) == 0 ||
		(config.TLSCertFile == "") != (config.TLSKeyFile == "") || config.MaxWebhookEndpoints < 0 || config.MaxWebhookEndpoints > webhook.MaxEndpointsPerTenant ||
		config.ACKConcurrency < 0 || config.ACKConcurrency > gmessages.MaxACKConcurrency || config.ACKTimeout < 0 ||
		config.ACKTimeout > gmessages.DefaultACKCoordinationTimeout ||
		(config.ACKTimeout > 0 && config.ACKTimeout < gmessages.MinimumACKCoordinationTimeout) {
		return ErrInvalidRuntime
	}
	if config.Environment == EnvironmentProduction {
		if config.OnWorkerError == nil {
			return ErrInvalidRuntime
		}
		if _, ok := config.Verifier.(*auth.OIDCVerifier); !ok {
			return ErrInvalidRuntime
		}
		if _, ok := config.KeyWrapper.(*awskms.Wrapper); !ok {
			return ErrInvalidRuntime
		}
		if _, ok := config.Objects.(*s3store.Store); !ok || config.ObjectBackend != ObjectBackendS3 {
			return ErrInvalidRuntime
		}
		if config.TLSCertFile == "" && !config.AllowPlainHTTPBehindTLSProxy {
			return ErrInvalidRuntime
		}
		if config.TLSCertFile == "" && config.AllowPlainHTTPBehindTLSProxy && !isLoopbackTCPAddress(config.HTTPAddress) {
			return ErrInvalidRuntime
		}
	}
	trusted := make(map[domain.TenantID]struct{}, len(config.Tenants))
	for _, tenantID := range config.Tenants {
		if tenantID == "" {
			return ErrInvalidRuntime
		}
		if _, duplicate := trusted[tenantID]; duplicate {
			return ErrInvalidRuntime
		}
		trusted[tenantID] = struct{}{}
	}
	for _, binding := range config.KafkaCommandTopics {
		if _, ok := trusted[binding.TenantID]; !ok {
			return ErrInvalidRuntime
		}
	}
	if len(config.KafkaCommandTopics) > 0 {
		if config.Kafka == nil {
			return ErrInvalidRuntime
		}
		if _, err := kafka.NewTopicTenantAuthorizer(config.KafkaCommandTopics); err != nil {
			return ErrInvalidRuntime
		}
	}
	return nil
}

func isLoopbackTCPAddress(address string) bool {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || port == "" || host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func NewGateway(ctx context.Context, config RuntimeConfig) (*Gateway, error) {
	if err := ValidateRuntimeConfig(config); err != nil {
		return nil, err
	}
	keyManager, err := session.NewManager(config.KeyWrapper)
	if err != nil {
		return nil, err
	}
	vault, err := session.NewVault(keyManager, config.Repository)
	if err != nil {
		return nil, err
	}
	actorSessions, err := connectionactor.NewVaultSessionStore(vault, "gmessages")
	if err != nil {
		return nil, err
	}
	inbox, err := ingress.NewService(config.Repository)
	if err != nil {
		return nil, err
	}
	durableSink, err := gmessages.NewDurableSink(gmessages.DurableSinkConfig{
		Inbox: inbox, ACKs: config.Repository, Sealer: keyManager,
		ACKTimeout: config.ACKTimeout, ACKConcurrency: config.ACKConcurrency,
	})
	if err != nil {
		return nil, err
	}
	mediaPolicy, err := media.NewURLPolicy(media.URLPolicyConfig{AllowedHosts: config.ProviderMediaHosts})
	if err != nil {
		return nil, err
	}
	pinnedPolicy, err := gmessages.NewPinnedMediaRequestPolicy(mediaPolicy)
	if err != nil {
		return nil, err
	}
	providerFactory := gmessages.NewRuntimeFactory(config.Logger).
		WithDurableMessaging(durableSink).
		WithMediaRequestPolicy(pinnedPolicy)
	actorPool, err := connectionactor.NewPool(connectionactor.PoolConfig{NewActor: func(connectionactor.Key) (connectionactor.RunnerExecutor, error) {
		return connectionactor.NewActor(connectionactor.ActorConfig{
			OwnerID: config.OwnerID, Store: config.Repository, Sessions: actorSessions, Providers: providerFactory,
			Metrics: config.ActorMetrics,
		})
	}})
	if err != nil {
		return nil, err
	}
	uploader, err := media.NewUploader(media.UploadConfig{
		Objects: config.Objects, Metadata: config.Repository, NewID: uuid.NewString,
		MaxBytes: config.MaxMediaBytes, MaxPixels: config.MaxMediaPixels, TempDirectory: config.MediaTempDirectory,
	})
	if err != nil {
		return nil, err
	}
	messagingServices, err := composeMessagingServices(config, actorPool, config.Repository, uploader, keyManager)
	if err != nil {
		return nil, err
	}
	messageService, err := messaging.NewService(messaging.Config{Store: config.Repository, NewID: uuid.NewString})
	if err != nil {
		return nil, err
	}
	contactProvider, err := gmessages.NewActorContactProvider(actorPool)
	if err != nil {
		return nil, err
	}
	contactService, err := contactsync.NewService(config.Repository, contactProvider, config.Repository)
	if err != nil {
		return nil, err
	}
	pairingService, err := pairing.NewService(pairing.Dependencies{
		Provider: gmessages.NewPairingProvider(), Repository: config.Repository, Sessions: keyManager, NewID: uuid.NewString,
	})
	if err != nil {
		return nil, err
	}
	destinationGuard := webhook.NewPublicDestinationGuard(media.URLPolicyConfig{})
	webhookService, err := webhook.NewEndpointService(webhook.EndpointConfig{
		Store: config.Repository, Secrets: keyManager, Destinations: destinationGuard, NewID: uuid.NewString,
		MaxEndpoints: config.MaxWebhookEndpoints,
	})
	if err != nil {
		return nil, err
	}
	webhookDeliverer, err := webhook.NewHTTPDeliverer(destinationGuard)
	if err != nil {
		return nil, err
	}

	mediaWorkers := make(map[domain.TenantID]OneWorker, len(config.Tenants))
	backfillWorkers := make(map[domain.TenantID]ConnectionWorker, len(config.Tenants))
	webhookWorkers := make(map[domain.TenantID]BatchWorker, len(config.Tenants))
	kafkaWorkers := make(map[domain.TenantID]BatchWorker, len(config.Tenants))
	for _, tenantID := range config.Tenants {
		backfillWorkers[tenantID] = messagingServices.Backfill
		mediaWorker, workerErr := media.NewFetchWorker(media.FetchWorkerConfig{
			TenantID: tenantID, OwnerID: config.OwnerID + "/media", Store: config.Repository,
			Importer: uploader, Fetcher: messagingServices.MediaFetcher, NewID: uuid.NewString,
		})
		if workerErr != nil {
			return nil, workerErr
		}
		mediaWorkers[tenantID] = mediaWorker
		webhookWorker, workerErr := webhook.NewWorker(webhook.WorkerConfig{
			Store: config.Repository, Secrets: keyManager, Deliverer: webhookDeliverer,
			OwnerID: config.OwnerID + "/webhook", TenantID: tenantID,
		})
		if workerErr != nil {
			return nil, workerErr
		}
		webhookWorkers[tenantID] = webhookWorker
		if config.Kafka != nil {
			kafkaWorker, kafkaErr := kafka.NewOutboxWorker(kafka.OutboxWorkerConfig{
				Store: config.Repository, Publisher: config.Kafka, OwnerID: config.OwnerID + "/kafka", TenantID: tenantID,
			})
			if kafkaErr != nil {
				return nil, kafkaErr
			}
			kafkaWorkers[tenantID] = kafkaWorker
		}
	}
	supervisor, err := NewSupervisor(SupervisorConfig{
		Tenants: config.Tenants, Connections: config.Repository, Actors: actorPool,
		Lanes: config.Repository, Dispatcher: messagingServices.Dispatcher,
		Media: mediaWorkers, Backfill: backfillWorkers, Webhooks: webhookWorkers, Kafka: kafkaWorkers,
		BackfillQuarantine: config.Repository, PollInterval: config.PollInterval, OnError: config.OnWorkerError,
	})
	if err != nil {
		return nil, err
	}
	handler, err := httpapi.NewHandler(httpapi.Dependencies{
		Store: config.Repository, Syncer: contactService, Pairing: pairingService, Health: config.Repository,
		Verifier: config.Verifier, NewID: uuid.NewString, Messages: messageService, Media: uploader, Webhooks: webhookService,
	})
	if err != nil {
		return nil, err
	}
	var publicHandler http.Handler = handler
	if config.HTTPMiddleware != nil {
		publicHandler = config.HTTPMiddleware(publicHandler)
		if publicHandler == nil {
			return nil, ErrInvalidRuntime
		}
	}
	server, err := httpapi.NewServer(config.HTTPAddress, publicHandler)
	if err != nil {
		return nil, err
	}
	var commandConsumer *kafka.CommandConsumer
	if config.Kafka != nil && len(config.KafkaCommandTopics) > 0 {
		authorizer, authErr := kafka.NewTopicTenantAuthorizer(config.KafkaCommandTopics)
		if authErr != nil {
			return nil, authErr
		}
		topics := make([]string, 0, len(config.KafkaCommandTopics))
		for topic := range config.KafkaCommandTopics {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		commandConsumer, err = kafka.NewCommandConsumer(kafka.CommandConsumerConfig{
			Authorizer: authorizer, Commands: messageService, Offsets: config.Kafka, DLQ: config.Repository, AllowedTopics: topics,
		})
		if err != nil {
			return nil, err
		}
	}
	return &Gateway{
		server: server, supervisor: supervisor, kafkaAdapter: config.Kafka, commands: commandConsumer,
		tlsCertFile: config.TLSCertFile, tlsKeyFile: config.TLSKeyFile,
		requireLocalPlaintext: config.Environment == EnvironmentProduction && config.TLSCertFile == "" && config.AllowPlainHTTPBehindTLSProxy,
	}, nil
}

func composeMessagingServices(
	config RuntimeConfig,
	executor connectionactor.ProviderExecutor,
	store gmessages.MessagingRuntimeStore,
	mediaSource gmessages.MediaSource,
	keys gmessages.MediaKeyOpener,
) (*gmessages.MessagingServices, error) {
	return gmessages.NewMessagingServices(gmessages.MessagingServicesConfig{
		Executor: executor, Store: store, Media: mediaSource, Keys: keys,
		OwnerID: config.OwnerID, MaxMediaBytes: config.MaxMediaBytes,
	})
}

func (gateway *Gateway) Run(ctx context.Context, listener net.Listener) error {
	if gateway == nil || gateway.server == nil || gateway.supervisor == nil || listener == nil || ctx == nil {
		return ErrInvalidRuntime
	}
	if gateway.requireLocalPlaintext {
		if err := validateLocalPlaintextListener(listener); err != nil {
			return err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	gateway.started.Store(true)
	defer gateway.started.Store(false)
	results := make(chan error, 3)
	var running sync.WaitGroup
	launch := func(run func() error) {
		running.Add(1)
		go func() { defer running.Done(); results <- run() }()
	}
	launch(func() error { return gateway.supervisor.Run(runCtx) })
	launch(func() error {
		if gateway.tlsCertFile != "" {
			return gateway.server.ServeTLS(listener, gateway.tlsCertFile, gateway.tlsKeyFile)
		}
		return gateway.server.Serve(listener)
	})
	if gateway.kafkaAdapter != nil && gateway.commands != nil {
		launch(func() error { return gateway.kafkaAdapter.RunCommands(runCtx, gateway.commands) })
	}

	var result error
	select {
	case <-ctx.Done():
	case err := <-results:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	_ = gateway.server.Shutdown(shutdownCtx)
	cancelShutdown()
	running.Wait()
	if gateway.kafkaAdapter != nil {
		gateway.kafkaAdapter.Close()
	}
	return result
}

// Started reports process-level gateway supervision readiness. It deliberately
// does not inspect individual phone connectivity: one disconnected phone must
// not make the whole multi-tenant service unready.
func (gateway *Gateway) Started() bool {
	return gateway != nil && gateway.started.Load()
}

func validateLocalPlaintextListener(listener net.Listener) error {
	switch local := listener.(type) {
	case *net.TCPListener:
		address, ok := local.Addr().(*net.TCPAddr)
		if !ok || address.IP == nil || !address.IP.IsLoopback() {
			return ErrInvalidRuntime
		}
		return nil
	case *net.UnixListener:
		return nil
	default:
		// Unknown listener implementations can misrepresent their address. A
		// production embedding must provide a concrete local OS listener.
		return ErrInvalidRuntime
	}
}
