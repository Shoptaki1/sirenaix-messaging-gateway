package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/auth"
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media/s3store"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session/awskms"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

type runtimeVerifier struct{}

func (runtimeVerifier) Verify(context.Context, string) (auth.Principal, error) {
	return auth.Principal{}, nil
}

type runtimeKeyWrapper struct{}

func (runtimeKeyWrapper) WrapKey(context.Context, []byte) (session.WrappedKey, error) {
	return session.WrappedKey{}, nil
}
func (runtimeKeyWrapper) UnwrapKey(context.Context, session.WrappedKey) ([]byte, error) {
	return nil, nil
}

type runtimeObjects struct{}

func (runtimeObjects) Put(context.Context, string, io.Reader, int64, string) (media.ObjectInfo, error) {
	return media.ObjectInfo{}, nil
}
func (runtimeObjects) Open(context.Context, string) (io.ReadCloser, media.ObjectInfo, error) {
	return nil, media.ObjectInfo{}, nil
}
func (runtimeObjects) Delete(context.Context, string) error { return nil }

func TestProductionRuntimeRequiresConcreteOIDCKMSS3AndTLSBoundary(t *testing.T) {
	valid := RuntimeConfig{
		Environment: EnvironmentProduction, HTTPAddress: "127.0.0.1:8443", OwnerID: "owner-a", Tenants: []domain.TenantID{"tenant-a"},
		Repository: &postgres.Repository{}, Verifier: &auth.OIDCVerifier{}, KeyWrapper: &awskms.Wrapper{},
		Objects: &s3store.Store{}, ObjectBackend: ObjectBackendS3, ProviderMediaHosts: []string{"media.example"},
		AllowPlainHTTPBehindTLSProxy: true, OnWorkerError: func(error) {},
	}
	if err := ValidateRuntimeConfig(valid); err != nil {
		t.Fatalf("valid production config = %v", err)
	}
	for name, mutate := range map[string]func(*RuntimeConfig){
		"non OIDC verifier":        func(config *RuntimeConfig) { config.Verifier = runtimeVerifier{} },
		"non KMS wrapper":          func(config *RuntimeConfig) { config.KeyWrapper = runtimeKeyWrapper{} },
		"non S3 objects":           func(config *RuntimeConfig) { config.Objects = runtimeObjects{} },
		"local backend":            func(config *RuntimeConfig) { config.ObjectBackend = ObjectBackendLocal },
		"unprotected listener":     func(config *RuntimeConfig) { config.AllowPlainHTTPBehindTLSProxy = false },
		"missing error reporter":   func(config *RuntimeConfig) { config.OnWorkerError = nil },
		"webhook quota too high":   func(config *RuntimeConfig) { config.MaxWebhookEndpoints = webhook.MaxEndpointsPerTenant + 1 },
		"ACK concurrency too high": func(config *RuntimeConfig) { config.ACKConcurrency = 17 },
		"ACK transport too long":   func(config *RuntimeConfig) { config.ACKTimeout = 4*time.Second + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := ValidateRuntimeConfig(candidate); err == nil {
				t.Fatal("unsafe production config unexpectedly passed")
			}
		})
	}
}

func TestProductionPlainHTTPProxyBoundaryRequiresLoopbackListener(t *testing.T) {
	base := RuntimeConfig{
		Environment: EnvironmentProduction, OwnerID: "owner-a", Tenants: []domain.TenantID{"tenant-a"},
		Repository: &postgres.Repository{}, Verifier: &auth.OIDCVerifier{}, KeyWrapper: &awskms.Wrapper{},
		Objects: &s3store.Store{}, ObjectBackend: ObjectBackendS3, ProviderMediaHosts: []string{"media.example"},
		AllowPlainHTTPBehindTLSProxy: true, OnWorkerError: func(error) {},
	}
	for _, address := range []string{":8080", "0.0.0.0:8080", "192.0.2.4:8080", "[::]:8080"} {
		candidate := base
		candidate.HTTPAddress = address
		if err := ValidateRuntimeConfig(candidate); err == nil {
			t.Fatalf("public plaintext listener %q unexpectedly accepted", address)
		}
	}
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080"} {
		candidate := base
		candidate.HTTPAddress = address
		if err := ValidateRuntimeConfig(candidate); err != nil {
			t.Fatalf("loopback plaintext listener %q = %v", address, err)
		}
	}
}

func TestGatewayRunRejectsConfiguredLoopbackWithActualPublicPlaintextListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Skipf("public test listener unavailable: %v", err)
	}
	defer listener.Close()
	gateway := &Gateway{server: &http.Server{}, supervisor: &Supervisor{}, requireLocalPlaintext: true}
	if err = gateway.Run(context.Background(), listener); !errors.Is(err, ErrInvalidRuntime) {
		t.Fatalf("Run(public plaintext listener) = %v, want ErrInvalidRuntime", err)
	}
}

func TestLocalPlaintextListenerValidationCoversIPv4IPv6AndUnix(t *testing.T) {
	for _, networkAddress := range [][2]string{{"tcp4", "127.0.0.1:0"}, {"tcp6", "[::1]:0"}} {
		listener, err := net.Listen(networkAddress[0], networkAddress[1])
		if err != nil {
			if networkAddress[0] == "tcp6" {
				continue
			}
			t.Fatal(err)
		}
		if err = validateLocalPlaintextListener(listener); err != nil {
			listener.Close()
			t.Fatalf("%s loopback listener = %v", networkAddress[0], err)
		}
		listener.Close()
	}
	if runtime.GOOS != "windows" {
		listener, err := net.Listen("unix", t.TempDir()+"/gateway.sock")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		if err = validateLocalPlaintextListener(listener); err != nil {
			t.Fatalf("Unix listener = %v", err)
		}
	}
}

func TestDevelopmentRuntimeAllowsOnlyExplicitLocalMode(t *testing.T) {
	config := RuntimeConfig{
		Environment: EnvironmentDevelopment, HTTPAddress: "127.0.0.1:8080", OwnerID: "owner-a", Tenants: []domain.TenantID{"tenant-a"},
		Repository: &postgres.Repository{}, Verifier: runtimeVerifier{}, KeyWrapper: runtimeKeyWrapper{},
		Objects: runtimeObjects{}, ObjectBackend: ObjectBackendLocal, ProviderMediaHosts: []string{"media.example"},
	}
	if err := ValidateRuntimeConfig(config); err != nil {
		t.Fatalf("explicit development config = %v", err)
	}
	config.Environment = ""
	if err := ValidateRuntimeConfig(config); err == nil {
		t.Fatal("implicit environment unexpectedly passed")
	}
}

type ownerCapturingMessagingStore struct{ claimOwner string }

func (store *ownerCapturingMessagingStore) ClaimNext(_ context.Context, _ messaging.LaneKey, ownerID string) (messaging.DispatchClaim, bool, error) {
	store.claimOwner = ownerID
	return messaging.DispatchClaim{}, false, nil
}
func (*ownerCapturingMessagingStore) BeginProviderIO(context.Context, messaging.DispatchClaim, string) (bool, error) {
	panic("unexpected provider I/O")
}
func (*ownerCapturingMessagingStore) RenewProviderIO(context.Context, messaging.DispatchClaim, string) (bool, error) {
	panic("unexpected provider I/O")
}
func (*ownerCapturingMessagingStore) CompleteDispatch(context.Context, messaging.DispatchClaim, []domain.MessageState, string) error {
	panic("unexpected provider I/O")
}
func (*ownerCapturingMessagingStore) ReleaseBeforeDispatch(context.Context, messaging.DispatchClaim, string) error {
	panic("unexpected provider I/O")
}
func (*ownerCapturingMessagingStore) GetLine(context.Context, domain.TenantID, domain.ConnectionID, domain.LineID) (domain.Line, error) {
	panic("unexpected line lookup")
}
func (*ownerCapturingMessagingStore) RecordCreatedConversationFenced(context.Context, domain.TenantID, domain.ConnectionID, domain.MessageID, string, string, bool, string, uint64) error {
	panic("unexpected route write")
}
func (*ownerCapturingMessagingStore) LoadCommittedCursor(context.Context, domain.TenantID, domain.ConnectionID, string) ([]byte, error) {
	panic("unexpected cursor read")
}
func (*ownerCapturingMessagingStore) LoadBackfillCheckpoint(context.Context, domain.TenantID, domain.ConnectionID) (*messaging.BackfillCheckpoint, error) {
	panic("unexpected checkpoint read")
}
func (*ownerCapturingMessagingStore) StageBackfillPageFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, messaging.BackfillPage) error {
	panic("unexpected checkpoint write")
}
func (*ownerCapturingMessagingStore) MarkBackfillItemFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, string, int, messaging.BackfillItemState, string) error {
	panic("unexpected checkpoint write")
}
func (*ownerCapturingMessagingStore) CompleteBackfillPageFenced(context.Context, domain.TenantID, domain.ConnectionID, string, uint64, string) error {
	panic("unexpected checkpoint write")
}

type noProviderExecutor struct{}

func (noProviderExecutor) Execute(context.Context, connectionactor.Key, connectionactor.ProviderOperation) error {
	panic("unexpected provider I/O")
}

type noMediaSource struct{}

func (noMediaSource) Open(context.Context, domain.TenantID, domain.MediaID) (io.ReadCloser, media.Record, error) {
	panic("unexpected media open")
}

type noKeyOpener struct{}

func (noKeyOpener) Open(context.Context, session.Scope, session.Envelope) ([]byte, error) {
	panic("unexpected key open")
}

func TestMessagingCompositionUsesExactConnectionActorLeaseOwner(t *testing.T) {
	store := &ownerCapturingMessagingStore{}
	services, err := composeMessagingServices(RuntimeConfig{OwnerID: "actor-owner-a"}, noProviderExecutor{}, store, noMediaSource{}, noKeyOpener{})
	if err != nil {
		t.Fatal(err)
	}
	worked, err := services.Dispatcher.DispatchLane(context.Background(), messaging.LaneKey{
		TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
	})
	if err != nil || worked || store.claimOwner != "actor-owner-a" {
		t.Fatalf("DispatchLane() = (%v, %v), claim owner %q", worked, err, store.claimOwner)
	}
}
