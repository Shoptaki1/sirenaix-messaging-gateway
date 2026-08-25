package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/internal/gateway/app"
	"go.mau.fi/mautrix-gmessages/internal/gateway/auth"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media/s3store"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ops"
	"go.mau.fi/mautrix-gmessages/internal/gateway/provider/gmessages"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/awskms"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/localkey"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

const maxConfigFileBytes = 1 << 20

var (
	version                       = "dev"
	revision                      = "unknown"
	buildDate                     = "unknown"
	errCommandDatabaseUnavailable = errors.New("command database unavailable")
)

type commandFailureClass string

const (
	commandFailureMigrationPending     commandFailureClass = "migration_pending"
	commandFailureAdoptionRequired     commandFailureClass = "adoption_required"
	commandFailureMigrationLock        commandFailureClass = "migration_lock"
	commandFailureAdoptionUnsafe       commandFailureClass = "adoption_unsafe"
	commandFailureMigrationLedger      commandFailureClass = "migration_ledger"
	commandFailureTenantPairing        commandFailureClass = "tenant_pairing_active"
	commandFailureTenantNotFound       commandFailureClass = "tenant_not_found"
	commandFailureTenantQuota          commandFailureClass = "tenant_quota"
	commandFailureTenantSuspended      commandFailureClass = "tenant_suspended"
	commandFailureTenantInput          commandFailureClass = "tenant_input"
	commandFailureInvalidInput         commandFailureClass = "invalid_input"
	commandFailureDatabaseUnavailable  commandFailureClass = "database_unavailable"
	commandFailureMigrationUnavailable commandFailureClass = "migration_unavailable"
	commandFailureTenantUnavailable    commandFailureClass = "tenant_unavailable"
	commandFailureServiceUnavailable   commandFailureClass = "service_unavailable"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCommand(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		// Configuration, provider frames, secrets, URLs, and database errors are
		// intentionally not rendered by the executable's last-resort logger.
		_, _ = os.Stderr.WriteString(safeCommandFailure(os.Args[1:], err))
		os.Exit(1)
	}
}

func safeCommandFailure(args []string, err error) string {
	switch classifyCommandFailure(args, err) {
	case commandFailureMigrationPending:
		return "sirenaix-gateway: database migrations are pending\n"
	case commandFailureAdoptionRequired:
		return "sirenaix-gateway: existing database schema requires explicit adoption review\n"
	case commandFailureMigrationLock:
		return "sirenaix-gateway: migration lock was not acquired\n"
	case commandFailureAdoptionUnsafe:
		return "sirenaix-gateway: existing schema failed adoption verification\n"
	case commandFailureMigrationLedger:
		return "sirenaix-gateway: migration ledger validation failed\n"
	case commandFailureTenantPairing:
		return "sirenaix-gateway: tenant has an active pairing attempt\n"
	case commandFailureTenantNotFound:
		return "sirenaix-gateway: tenant was not found\n"
	case commandFailureTenantQuota:
		return "sirenaix-gateway: tenant connection quota was exceeded\n"
	case commandFailureTenantSuspended:
		return "sirenaix-gateway: tenant is suspended\n"
	case commandFailureTenantInput:
		return "sirenaix-gateway: tenant administration input is invalid\n"
	case commandFailureInvalidInput:
		return "sirenaix-gateway: command input or configuration is invalid\n"
	case commandFailureDatabaseUnavailable:
		return "sirenaix-gateway: command database is unavailable\n"
	case commandFailureMigrationUnavailable:
		return "sirenaix-gateway: migration command failed; inspect operator logs\n"
	case commandFailureTenantUnavailable:
		return "sirenaix-gateway: tenant command failed; inspect operator logs\n"
	default:
		return "sirenaix-gateway stopped safely; inspect bounded service metrics\n"
	}
}

func classifyCommandFailure(args []string, err error) commandFailureClass {
	switch {
	case errors.Is(err, postgres.ErrMigrationPending):
		return commandFailureMigrationPending
	case errors.Is(err, postgres.ErrUntrackedSchema):
		return commandFailureAdoptionRequired
	case errors.Is(err, postgres.ErrMigrationLock):
		return commandFailureMigrationLock
	case errors.Is(err, postgres.ErrUnsafeAdoption):
		return commandFailureAdoptionUnsafe
	case errors.Is(err, postgres.ErrMigrationGap), errors.Is(err, postgres.ErrMigrationDuplicate),
		errors.Is(err, postgres.ErrMigrationDrift), errors.Is(err, postgres.ErrDatabaseAhead),
		errors.Is(err, postgres.ErrMigrationCatalog):
		return commandFailureMigrationLedger
	case errors.Is(err, postgres.ErrTenantPairingActive):
		return commandFailureTenantPairing
	case errors.Is(err, postgres.ErrTenantNotFound):
		return commandFailureTenantNotFound
	case errors.Is(err, postgres.ErrConnectionQuotaExceeded):
		return commandFailureTenantQuota
	case errors.Is(err, postgres.ErrTenantSuspended):
		return commandFailureTenantSuspended
	case errors.Is(err, postgres.ErrInvalidTenantAdminInput):
		return commandFailureTenantInput
	case errors.Is(err, app.ErrInvalidRuntime):
		return commandFailureInvalidInput
	case errors.Is(err, errCommandDatabaseUnavailable):
		return commandFailureDatabaseUnavailable
	case len(args) > 0 && args[0] == "migrate":
		return commandFailureMigrationUnavailable
	case len(args) > 0 && args[0] == "tenant":
		return commandFailureTenantUnavailable
	default:
		return commandFailureServiceUnavailable
	}
}

func runCommand(ctx context.Context, args []string, getenv func(string) string, output io.Writer) error {
	if ctx == nil || getenv == nil || output == nil {
		return app.ErrInvalidRuntime
	}
	if len(args) == 0 || (len(args) == 1 && args[0] == "serve") {
		return run(ctx, getenv)
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			return app.ErrInvalidRuntime
		}
		_, err := fmt.Fprintf(output, "sirenaix-gateway %s (revision %s, built %s)\n", version, revision, buildDate)
		return err
	case "migrate":
		return runMigrationCommand(ctx, args[1:], getenv, output)
	case "tenant":
		return runTenantCommand(ctx, args[1:], getenv, output)
	default:
		return app.ErrInvalidRuntime
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	environment := strings.TrimSpace(getenv("SIRENAIX_ENVIRONMENT"))
	ownerID := strings.TrimSpace(getenv("SIRENAIX_OWNER_ID"))
	httpAddress := strings.TrimSpace(getenv("SIRENAIX_HTTP_ADDRESS"))
	databaseURL := strings.TrimSpace(getenv("SIRENAIX_DATABASE_URL"))
	tenants, err := parseTenants(getenv("SIRENAIX_TENANTS"))
	if err != nil || environment == "" || ownerID == "" || httpAddress == "" || databaseURL == "" {
		return app.ErrInvalidRuntime
	}
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("service", "sirenaix-gateway").Logger()

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	defer db.Close()
	poolConfig, err := parseDatabasePoolConfig(getenv)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	applyDatabasePoolConfig(db, poolConfig)
	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()
	if err = db.PingContext(startupCtx); err != nil {
		return app.ErrInvalidRuntime
	}
	migrationRunner, err := postgres.NewMigrationRunner(db, migrationConfig(getenv))
	if err != nil || migrationRunner.CheckCurrent(startupCtx) != nil {
		// Serving never mutates the schema. Operators must run the explicit
		// migration command with its separate migration credential first.
		return app.ErrInvalidRuntime
	}
	repository, err := postgres.New(db)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	queueMetrics := ops.QueueSourceFunc(func(checkCtx context.Context) (ops.QueueDepths, error) {
		var total ops.QueueDepths
		for _, tenantID := range tenants {
			depths, depthErr := repository.OperationalQueueDepths(checkCtx, tenantID)
			if depthErr != nil {
				return ops.QueueDepths{}, depthErr
			}
			total.Messages += depths.Messages
			total.Media += depths.Media
			total.Webhooks += depths.Webhooks
			total.Kafka += depths.Kafka
		}
		return total, nil
	})
	metricsRegistry := ops.NewRegistry(db, queueMetrics)

	oidcClient := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxIdleConns: 20, MaxIdleConnsPerHost: 4,
	}}
	verifier, err := auth.NewOIDCVerifier(startupCtx, auth.OIDCConfig{
		Issuer: getenv("SIRENAIX_OIDC_ISSUER"), Audience: getenv("SIRENAIX_OIDC_AUDIENCE"),
		TenantClaim: getenv("SIRENAIX_OIDC_TENANT_CLAIM"), HTTPClient: oidcClient,
		AllowInsecureLoopbackIssuer: environment == app.EnvironmentDevelopment && parseBool(getenv("SIRENAIX_DEV_ALLOW_HTTP_OIDC")),
	})
	if err != nil {
		return app.ErrInvalidRuntime
	}
	var keyWrapper session.KeyWrapper
	switch strings.TrimSpace(getenv("SIRENAIX_KEY_BACKEND")) {
	case "aws-kms":
		kmsKeys, currentKMSVersion, parseErr := parseKMSKeys(getenv("SIRENAIX_KMS_KEYS"), getenv("SIRENAIX_KMS_CURRENT_VERSION"))
		if parseErr != nil {
			return app.ErrInvalidRuntime
		}
		keyWrapper, err = awskms.New(startupCtx, awskms.Config{
			KeyIDs: kmsKeys, CurrentVersion: currentKMSVersion, Region: getenv("SIRENAIX_AWS_REGION"), OperationTimeout: 10 * time.Second,
		})
	case "local":
		if environment != app.EnvironmentDevelopment {
			return app.ErrInvalidRuntime
		}
		masterKey, parseErr := parseDevelopmentMasterKey(getenv("SIRENAIX_DEV_MASTER_KEY_B64"))
		if parseErr != nil {
			return app.ErrInvalidRuntime
		}
		keyWrapper, err = localkey.New(localkey.Config{MasterKey: masterKey})
		zeroBytes(masterKey)
	default:
		return app.ErrInvalidRuntime
	}
	if err != nil {
		return app.ErrInvalidRuntime
	}

	objectBackend := strings.TrimSpace(getenv("SIRENAIX_OBJECT_BACKEND"))
	var objects media.ObjectStore
	switch objectBackend {
	case app.ObjectBackendS3:
		objects, err = s3store.New(startupCtx, s3store.Config{
			Bucket: getenv("SIRENAIX_S3_BUCKET"), Prefix: getenv("SIRENAIX_S3_PREFIX"), Region: getenv("SIRENAIX_AWS_REGION"),
			Endpoint: getenv("SIRENAIX_S3_ENDPOINT"), UsePathStyle: parseBool(getenv("SIRENAIX_S3_PATH_STYLE")),
			ExpectedBucketOwner:       getenv("SIRENAIX_S3_EXPECTED_BUCKET_OWNER"),
			AllowMissingExpectedOwner: parseBool(getenv("SIRENAIX_S3_ALLOW_MISSING_EXPECTED_OWNER")),
			OperationTimeout:          30 * time.Second, KMSKeyID: getenv("SIRENAIX_S3_KMS_KEY_ID"),
		})
	case app.ObjectBackendLocal:
		if environment != app.EnvironmentDevelopment {
			return app.ErrInvalidRuntime
		}
		objects, err = media.NewLocalStore(getenv("SIRENAIX_DEV_OBJECT_ROOT"))
	default:
		return app.ErrInvalidRuntime
	}
	if err != nil {
		return app.ErrInvalidRuntime
	}
	if closer, ok := objects.(io.Closer); ok {
		defer closer.Close()
	}

	topicBindings, err := parseTopicBindings(getenv("SIRENAIX_KAFKA_COMMAND_TOPICS"), tenants)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	kafkaAdapter, err := configureKafka(getenv, topicBindings, logger)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	kafkaOwnedByGateway := false
	defer func() {
		if kafkaAdapter != nil && !kafkaOwnedByGateway {
			kafkaAdapter.Close()
		}
	}()
	maxMediaBytes, err := parseOptionalPositiveInt64(getenv("SIRENAIX_MAX_MEDIA_BYTES"), media.DefaultMaxBytes)
	if err != nil || maxMediaBytes > media.HardMaxBytes {
		return app.ErrInvalidRuntime
	}
	maxMediaPixels, err := parseOptionalPositiveInt64(getenv("SIRENAIX_MAX_MEDIA_PIXELS"), media.DefaultMaxPixels)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	maxWebhookEndpoints, err := parseOptionalPositiveInt64(getenv("SIRENAIX_MAX_WEBHOOK_ENDPOINTS"), webhook.DefaultMaxEndpointsPerTenant)
	if err != nil || maxWebhookEndpoints > webhook.MaxEndpointsPerTenant {
		return app.ErrInvalidRuntime
	}
	ackTimeout, err := parseOptionalDuration(getenv("SIRENAIX_ACK_TIMEOUT"), gmessages.DefaultACKCoordinationTimeout)
	if err != nil || ackTimeout < gmessages.MinimumACKCoordinationTimeout || ackTimeout > gmessages.DefaultACKCoordinationTimeout {
		return app.ErrInvalidRuntime
	}
	ackConcurrency, err := parseOptionalPositiveInt64(getenv("SIRENAIX_ACK_CONCURRENCY"), gmessages.DefaultACKConcurrency)
	if err != nil || ackConcurrency > gmessages.MaxACKConcurrency {
		return app.ErrInvalidRuntime
	}
	tlsCertFile, tlsKeyFile := strings.TrimSpace(getenv("SIRENAIX_TLS_CERT_FILE")), strings.TrimSpace(getenv("SIRENAIX_TLS_KEY_FILE"))
	if (tlsCertFile == "") != (tlsKeyFile == "") {
		return app.ErrInvalidRuntime
	}
	if tlsCertFile != "" {
		certificatePEM, certErr := readBoundedFile(tlsCertFile)
		keyPEM, keyErr := readBoundedFile(tlsKeyFile)
		if certErr != nil || keyErr != nil {
			return app.ErrInvalidRuntime
		}
		if _, err = tls.X509KeyPair(certificatePEM, keyPEM); err != nil {
			return app.ErrInvalidRuntime
		}
	}
	workerErrorReporter := func(error) {
		// Worker errors may wrap provider/database details. Emit only a bounded
		// classification; Supervisor escalates repeated failures to process health.
		logger.Error().Str("class", "worker_failure").Msg("gateway worker failed")
	}

	gateway, err := app.NewGateway(startupCtx, app.RuntimeConfig{
		Environment: environment, HTTPAddress: httpAddress,
		TLSCertFile: tlsCertFile, TLSKeyFile: tlsKeyFile,
		AllowPlainHTTPBehindTLSProxy: parseBool(getenv("SIRENAIX_ALLOW_PLAIN_HTTP_BEHIND_TLS_PROXY")),
		OwnerID:                      ownerID, Tenants: tenants, Repository: repository, Verifier: verifier, KeyWrapper: keyWrapper,
		Objects: objects, ObjectBackend: objectBackend, ProviderMediaHosts: splitNonEmpty(getenv("SIRENAIX_PROVIDER_MEDIA_HOSTS")),
		Kafka: kafkaAdapter, KafkaCommandTopics: topicBindings, Logger: logger, PollInterval: time.Second,
		MaxMediaBytes:       maxMediaBytes,
		MaxMediaPixels:      maxMediaPixels,
		MaxWebhookEndpoints: int(maxWebhookEndpoints),
		ACKTimeout:          ackTimeout,
		ACKConcurrency:      int(ackConcurrency),
		MediaTempDirectory:  getenv("SIRENAIX_MEDIA_TEMP_DIRECTORY"),
		OnWorkerError:       workerErrorReporter,
		ActorMetrics:        metricsRegistry,
		HTTPMiddleware:      metricsRegistry.WrapHTTP,
	})
	if err != nil {
		return app.ErrInvalidRuntime
	}
	kafkaOwnedByGateway = true
	listener, err := net.Listen("tcp", httpAddress)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	defer listener.Close()
	opsAddress := strings.TrimSpace(getenv("SIRENAIX_OPS_ADDRESS"))
	if opsAddress == "" {
		opsAddress = "127.0.0.1:9090"
	}
	if opsAddress == httpAddress {
		return app.ErrInvalidRuntime
	}
	readinessChecks := []ops.NamedCheck{
		{Name: ops.DependencyDatabase, Check: ops.CheckFunc(func(checkCtx context.Context) error { return db.PingContext(checkCtx) }),
			ClassifyFailure: func(error) ops.FailureClass { return ops.FailureUnavailable }},
		{Name: ops.DependencySchema, Check: ops.CheckFunc(migrationRunner.CheckCurrent),
			ClassifyFailure: func(error) ops.FailureClass { return ops.FailureSchema }},
		{Name: ops.DependencyGateway, Check: ops.CheckFunc(func(context.Context) error {
			if !gateway.Started() {
				return errors.New("gateway startup incomplete")
			}
			return nil
		}), ClassifyFailure: func(error) ops.FailureClass { return ops.FailureStartup }},
	}
	if kafkaAdapter != nil {
		readinessChecks = append(readinessChecks, ops.NamedCheck{Name: ops.DependencyKafka, Check: kafkaAdapter,
			ClassifyFailure: classifyKafkaReadinessFailure})
	}
	if objectCheck, ok := objects.(ops.Check); ok {
		readinessChecks = append(readinessChecks, ops.NamedCheck{Name: ops.DependencyObjectStore, Check: objectCheck,
			ClassifyFailure: func(error) ops.FailureClass { return ops.FailureUnavailable }})
	} else if environment == app.EnvironmentProduction {
		return app.ErrInvalidRuntime
	}
	opsHandler, err := ops.NewHandler(ops.Config{
		Registry: metricsRegistry, CheckTimeout: 2 * time.Second,
		Checks: readinessChecks,
		OnReadinessFailure: func(dependency ops.Dependency, class ops.FailureClass) {
			logger.Error().Str("dependency", string(dependency)).Str("class", string(class)).Msg("readiness dependency failed")
		},
	})
	if err != nil {
		return app.ErrInvalidRuntime
	}
	opsListener, err := net.Listen("tcp", opsAddress)
	if err != nil {
		return app.ErrInvalidRuntime
	}
	defer opsListener.Close()
	opsServer := &http.Server{
		Addr: opsAddress, Handler: opsHandler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	logger.Info().Str("event", "listeners_starting").Msg("gateway lifecycle")
	result := runGatewayAndOperations(ctx, gateway, listener, opsServer, opsListener)
	logger.Info().Str("event", "stopped").Msg("gateway lifecycle")
	return result
}

func classifyKafkaReadinessFailure(err error) ops.FailureClass {
	switch {
	case errors.Is(err, kafka.ErrKafkaTopicMissing):
		return ops.FailureMissing
	case errors.Is(err, kafka.ErrKafkaAuthorizationUnverifiable):
		return ops.FailureAuthorizationUnverifiable
	case errors.Is(err, kafka.ErrKafkaTopicUnauthorized),
		errors.Is(err, kafka.ErrKafkaGroupUnauthorized),
		errors.Is(err, kafka.ErrKafkaClusterUnauthorized):
		return ops.FailureAuthorization
	default:
		return ops.FailureUnavailable
	}
}

func configureKafka(getenv func(string) string, topics map[string]kafka.TopicBinding, logger zerolog.Logger) (*kafka.FranzAdapter, error) {
	brokers := splitNonEmpty(getenv("SIRENAIX_KAFKA_BROKERS"))
	if len(brokers) == 0 {
		if len(topics) != 0 {
			return nil, kafka.ErrInvalidFranzConfig
		}
		return nil, nil
	}
	tlsConfig, err := loadKafkaTLS(getenv)
	if err != nil {
		return nil, err
	}
	adapter, enabled, err := kafka.NewFranzAdapter(kafka.FranzConfig{
		Brokers: brokers, ClientID: strings.TrimSpace(getenv("SIRENAIX_KAFKA_CLIENT_ID")),
		GroupID: strings.TrimSpace(getenv("SIRENAIX_KAFKA_GROUP_ID")), CommandTopics: topics, TLSConfig: tlsConfig,
		OnSecurityQuarantine: func(string, int32, int64) {
			logger.Error().Str("class", "kafka_unmapped_command").Msg("Kafka command partition paused for operator review")
		},
	})
	if err != nil || !enabled {
		return nil, err
	}
	return adapter, nil
}

func loadKafkaTLS(getenv func(string) string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if path := strings.TrimSpace(getenv("SIRENAIX_KAFKA_CA_FILE")); path != "" {
		contents, err := readBoundedFile(path)
		if err != nil {
			return nil, err
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil || !roots.AppendCertsFromPEM(contents) {
			return nil, errors.New("invalid Kafka CA")
		}
		config.RootCAs = roots
	}
	certFile, keyFile := strings.TrimSpace(getenv("SIRENAIX_KAFKA_CLIENT_CERT_FILE")), strings.TrimSpace(getenv("SIRENAIX_KAFKA_CLIENT_KEY_FILE"))
	if (certFile == "") != (keyFile == "") {
		return nil, kafka.ErrInvalidFranzConfig
	}
	if certFile != "" {
		certificatePEM, err := readBoundedFile(certFile)
		if err != nil {
			return nil, err
		}
		keyPEM, err := readBoundedFile(keyFile)
		if err != nil {
			return nil, err
		}
		certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
		if err != nil {
			return nil, kafka.ErrInvalidFranzConfig
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("invalid configuration file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxConfigFileBytes {
		return nil, errors.New("invalid configuration file")
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxConfigFileBytes+1))
	if err != nil || len(contents) < 1 || len(contents) > maxConfigFileBytes {
		return nil, errors.New("invalid configuration file")
	}
	return contents, nil
}

func parseTenants(value string) ([]domain.TenantID, error) {
	parts, err := strictCSV(value)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 || len(parts) > 1024 {
		return nil, app.ErrInvalidRuntime
	}
	seen := make(map[domain.TenantID]struct{}, len(parts))
	tenants := make([]domain.TenantID, 0, len(parts))
	for _, part := range parts {
		tenantID := domain.TenantID(part)
		if len(part) > 128 || strings.ContainsAny(part, "\x00\r\n") {
			return nil, app.ErrInvalidRuntime
		}
		if _, duplicate := seen[tenantID]; duplicate {
			return nil, app.ErrInvalidRuntime
		}
		seen[tenantID] = struct{}{}
		tenants = append(tenants, tenantID)
	}
	return tenants, nil
}

func parseKMSKeys(value, current string) (map[int]string, int, error) {
	currentVersion, err := strconv.Atoi(strings.TrimSpace(current))
	if err != nil || currentVersion < 1 {
		return nil, 0, app.ErrInvalidRuntime
	}
	keys := make(map[int]string)
	items, splitErr := strictCSV(value)
	if splitErr != nil {
		return nil, 0, app.ErrInvalidRuntime
	}
	for _, item := range items {
		versionText, keyID, found := strings.Cut(item, "=")
		version, parseErr := strconv.Atoi(versionText)
		if !found || parseErr != nil || version < 1 || keyID == "" || keys[version] != "" {
			return nil, 0, app.ErrInvalidRuntime
		}
		keys[version] = keyID
	}
	if keys[currentVersion] == "" {
		return nil, 0, app.ErrInvalidRuntime
	}
	return keys, currentVersion, nil
}

func parseDevelopmentMasterKey(value string) ([]byte, error) {
	if strings.TrimSpace(value) != value || value == "" {
		return nil, app.ErrInvalidRuntime
	}
	key, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(key) != 32 {
		zeroBytes(key)
		return nil, app.ErrInvalidRuntime
	}
	return key, nil
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func parseTopicBindings(value string, tenants []domain.TenantID) (map[string]kafka.TopicBinding, error) {
	trusted := make(map[domain.TenantID]struct{}, len(tenants))
	for _, tenantID := range tenants {
		trusted[tenantID] = struct{}{}
	}
	bindings := make(map[string]kafka.TopicBinding)
	if strings.TrimSpace(value) == "" {
		return bindings, nil
	}
	items, err := strictCSV(value)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		parts := strings.Split(item, "=")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" || len(parts[0]) > 249 {
			return nil, app.ErrInvalidRuntime
		}
		binding := kafka.TopicBinding{TenantID: domain.TenantID(parts[1]), Principal: parts[2]}
		if _, ok := trusted[binding.TenantID]; !ok {
			return nil, app.ErrInvalidRuntime
		}
		if _, duplicate := bindings[parts[0]]; duplicate {
			return nil, app.ErrInvalidRuntime
		}
		bindings[parts[0]] = binding
	}
	return bindings, nil
}

func splitNonEmpty(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func strictCSV(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, app.ErrInvalidRuntime
	}
	parts := strings.Split(value, ",")
	result := make([]string, len(parts))
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, app.ErrInvalidRuntime
		}
		result[index] = part
	}
	return result, nil
}

func parseBool(value string) bool {
	parsed, _ := strconv.ParseBool(strings.TrimSpace(value))
	return parsed
}

func parseOptionalPositiveInt64(value string, fallback int64) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 1 {
		return 0, app.ErrInvalidRuntime
	}
	return parsed, nil
}

func parseOptionalDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, app.ErrInvalidRuntime
	}
	return parsed, nil
}

type databasePoolConfig struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

func parseDatabasePoolConfig(getenv func(string) string) (databasePoolConfig, error) {
	if getenv == nil {
		return databasePoolConfig{}, app.ErrInvalidRuntime
	}
	parseInt := func(key string, fallback, minimum, maximum int) (int, error) {
		value := strings.TrimSpace(getenv(key))
		if value == "" {
			return fallback, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < minimum || parsed > maximum {
			return 0, app.ErrInvalidRuntime
		}
		return parsed, nil
	}
	maxOpen, err := parseInt("SIRENAIX_DB_MAX_OPEN_CONNS", 32, 1, 256)
	if err != nil {
		return databasePoolConfig{}, err
	}
	maxIdle, err := parseInt("SIRENAIX_DB_MAX_IDLE_CONNS", 8, 0, 256)
	if err != nil || maxIdle > maxOpen {
		return databasePoolConfig{}, app.ErrInvalidRuntime
	}
	maxLifetime, err := parseOptionalDuration(getenv("SIRENAIX_DB_CONN_MAX_LIFETIME"), 30*time.Minute)
	if err != nil || maxLifetime < time.Minute || maxLifetime > 2*time.Hour {
		return databasePoolConfig{}, app.ErrInvalidRuntime
	}
	maxIdleTime, err := parseOptionalDuration(getenv("SIRENAIX_DB_CONN_MAX_IDLE_TIME"), 5*time.Minute)
	if err != nil || maxIdleTime < time.Minute || maxIdleTime > 30*time.Minute {
		return databasePoolConfig{}, app.ErrInvalidRuntime
	}
	return databasePoolConfig{MaxOpen: maxOpen, MaxIdle: maxIdle, MaxLifetime: maxLifetime, MaxIdleTime: maxIdleTime}, nil
}

func applyDatabasePoolConfig(db *sql.DB, config databasePoolConfig) {
	db.SetMaxOpenConns(config.MaxOpen)
	db.SetMaxIdleConns(config.MaxIdle)
	db.SetConnMaxLifetime(config.MaxLifetime)
	db.SetConnMaxIdleTime(config.MaxIdleTime)
}

func migrationConfig(getenv func(string) string) postgres.MigrationRunnerConfig {
	lockTimeout, lockErr := parseOptionalDuration(getenv("SIRENAIX_MIGRATION_LOCK_TIMEOUT"), 30*time.Second)
	statementTimeout, statementErr := parseOptionalDuration(getenv("SIRENAIX_MIGRATION_STATEMENT_TIMEOUT"), 2*time.Minute)
	if lockErr != nil || statementErr != nil {
		return postgres.MigrationRunnerConfig{LockTimeout: -1, StatementTimeout: -1}
	}
	return postgres.MigrationRunnerConfig{LockTimeout: lockTimeout, StatementTimeout: statementTimeout}
}

func openCommandDatabase(ctx context.Context, databaseURL string, getenv func(string) string) (*sql.DB, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, app.ErrInvalidRuntime
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, app.ErrInvalidRuntime
	}
	pool, err := parseDatabasePoolConfig(getenv)
	if err != nil {
		db.Close()
		return nil, err
	}
	applyDatabasePoolConfig(db, pool)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, errCommandDatabaseUnavailable
	}
	return db, nil
}

func runMigrationCommand(ctx context.Context, args []string, getenv func(string) string, output io.Writer) error {
	if len(args) == 0 || (args[0] != "up" && args[0] != "status") {
		return app.ErrInvalidRuntime
	}
	db, err := openCommandDatabase(ctx, getenv("SIRENAIX_MIGRATION_DATABASE_URL"), getenv)
	if err != nil {
		return err
	}
	defer db.Close()
	runner, err := postgres.NewMigrationRunner(db, migrationConfig(getenv))
	if err != nil {
		return app.ErrInvalidRuntime
	}
	switch args[0] {
	case "up":
		flags := flag.NewFlagSet("migrate up", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		adopt := flags.Bool("adopt-existing", false, "adopt a fully verified legacy schema")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return app.ErrInvalidRuntime
		}
		result, upErr := runner.Up(ctx, postgres.MigrationUpOptions{AdoptExisting: *adopt})
		if upErr != nil {
			return upErr
		}
		return json.NewEncoder(output).Encode(map[string]any{
			"previous": result.Previous, "current": result.Current, "applied": result.Applied, "adopted": result.Adopted,
		})
	case "status":
		flags := flag.NewFlagSet("migrate status", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		check := flags.Bool("check", false, "fail unless current")
		if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
			return app.ErrInvalidRuntime
		}
		status, statusErr := runner.Status(ctx)
		if statusErr != nil {
			return statusErr
		}
		if encodeErr := json.NewEncoder(output).Encode(map[string]any{
			"tracked": status.Tracked, "current": status.Current, "latest": status.Latest, "pending": len(status.Pending),
		}); encodeErr != nil {
			return encodeErr
		}
		if *check && !status.IsCurrent() {
			return postgres.ErrMigrationPending
		}
		return nil
	default:
		return app.ErrInvalidRuntime
	}
}

func runTenantCommand(ctx context.Context, args []string, getenv func(string) string, output io.Writer) error {
	if len(args) == 0 {
		return app.ErrInvalidRuntime
	}
	action := postgres.TenantAdminAction(args[0])
	if action.Validate() != nil {
		return app.ErrInvalidRuntime
	}
	db, err := openCommandDatabase(ctx, getenv("SIRENAIX_ADMIN_DATABASE_URL"), getenv)
	if err != nil {
		return err
	}
	defer db.Close()
	runner, err := postgres.NewMigrationRunner(db, migrationConfig(getenv))
	if err != nil {
		return app.ErrInvalidRuntime
	}
	if err = runner.CheckCurrent(ctx); err != nil {
		return err
	}
	administrator, err := postgres.NewTenantAdministrator(db, strings.TrimSpace(getenv("SIRENAIX_ADMIN_ACTOR")))
	if err != nil {
		return app.ErrInvalidRuntime
	}
	flags := flag.NewFlagSet("tenant "+string(action), flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tenantID := flags.String("id", "", "tenant identifier")
	name := flags.String("name", "", "tenant display name")
	maxConnections := flags.Int("max-connections", postgres.DefaultMaxConnectionsPerTenant, "connection quota")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return app.ErrInvalidRuntime
	}
	var status postgres.TenantOperationalStatus
	switch action {
	case postgres.TenantActionProvision:
		status, err = administrator.Provision(ctx, postgres.TenantAdminInput{
			TenantID: *tenantID, Name: *name, MaxConnections: *maxConnections,
		})
	case postgres.TenantActionStatus:
		if *name != "" || *maxConnections != postgres.DefaultMaxConnectionsPerTenant {
			return app.ErrInvalidRuntime
		}
		status, err = administrator.Status(ctx, *tenantID)
	case postgres.TenantActionSuspend:
		if *name != "" || *maxConnections != postgres.DefaultMaxConnectionsPerTenant {
			return app.ErrInvalidRuntime
		}
		status, err = administrator.Suspend(ctx, *tenantID)
	case postgres.TenantActionResume:
		if *name != "" || *maxConnections != postgres.DefaultMaxConnectionsPerTenant {
			return app.ErrInvalidRuntime
		}
		status, err = administrator.Resume(ctx, *tenantID)
	default:
		return app.ErrInvalidRuntime
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(status)
}

func runGatewayAndOperations(ctx context.Context, gateway *app.Gateway, publicListener net.Listener, opsServer *http.Server, opsListener net.Listener) error {
	if ctx == nil || gateway == nil || publicListener == nil || opsServer == nil || opsListener == nil {
		return app.ErrInvalidRuntime
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- gateway.Run(runCtx, publicListener) }()
	go func() { results <- opsServer.Serve(opsListener) }()

	remaining := 2
	var result error
	select {
	case <-ctx.Done():
	case first := <-results:
		remaining--
		if first != nil && !errors.Is(first, context.Canceled) && !errors.Is(first, http.ErrServerClosed) {
			result = first
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_ = opsServer.Shutdown(shutdownCtx)
	shutdownCancel()
	for remaining > 0 {
		select {
		case next := <-results:
			remaining--
			if result == nil && next != nil && !errors.Is(next, context.Canceled) && !errors.Is(next, http.ErrServerClosed) {
				result = next
			}
		case <-time.After(20 * time.Second):
			return errors.New("gateway shutdown timeout")
		}
	}
	return result
}
