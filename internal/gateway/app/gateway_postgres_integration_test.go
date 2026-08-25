//go:build postgres_integration

package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

type integrationProviderExecutor struct{ calls int }

func (executor *integrationProviderExecutor) Execute(context.Context, connectionactor.Key, connectionactor.ProviderOperation) error {
	executor.calls++
	return context.DeadlineExceeded
}

func TestPostgresIntegrationQueuedMessageReachesExactOwnerActorExecutor(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()
	schema := fmt.Sprintf("sirenaix_app_it_%d", time.Now().UnixNano())
	if _, err = adminDB.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
	})
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries, err := postgres.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		contents, readErr := postgres.Migrations.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.ExecContext(ctx, string(contents)); err != nil {
			t.Fatalf("apply %s: %v", entry.Name(), err)
		}
	}
	repository, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	const tenantID, connectionID, ownerID = domain.TenantID("tenant-a"), domain.ConnectionID("connection-a"), "actor-owner-a"
	if err = repository.SaveTenant(ctx, domain.Tenant{ID: tenantID, Name: "Tenant A"}); err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveConnection(ctx, tenantID, postgres.ConnectionRecord{
		Connection:                domain.Connection{ID: connectionID, TenantID: tenantID, Name: "Connection A", State: domain.ConnectionStateConnected},
		ProviderDeviceFingerprint: bytes.Repeat([]byte{'a'}, 32),
	}); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := repository.AcquireConnectionLease(ctx, tenantID, connectionID, ownerID, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("AcquireConnectionLease() = (%+v, %v, %v)", lease, acquired, err)
	}
	inbox, _ := ingress.NewService(repository)
	if result, processErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantID, ConnectionID: connectionID, OwnerID: ownerID, FencingToken: lease.FencingToken,
		ProviderResponseID: "conversation-seed", Raw: []byte("conversation-seed"),
		Projection: ingress.Projection{Conversations: []ingress.ProjectedConversation{{ConversationID: "conversation-a", DefaultOutgoingID: "outgoing-a"}}},
	}); processErr != nil || !result.ACKEligible {
		t.Fatalf("seed conversation = (%+v, %v)", result, processErr)
	}
	commands, _ := messaging.NewService(messaging.Config{Store: repository, NewID: func() string { return "message-a" }})
	message, err := commands.Submit(ctx, tenantID, "idempotency-a", messaging.SendInput{
		ConnectionID: connectionID, ConversationID: "conversation-a", Text: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	executor := &integrationProviderExecutor{}
	services, err := composeMessagingServices(RuntimeConfig{OwnerID: ownerID}, executor, repository, noMediaSource{}, noKeyOpener{})
	if err != nil {
		t.Fatal(err)
	}
	worked, err := services.Dispatcher.DispatchLane(ctx, messaging.LaneKey{TenantID: tenantID, ConnectionID: connectionID, ConversationID: "conversation-a"})
	if err != nil || !worked || executor.calls != 1 {
		t.Fatalf("DispatchLane() = (%v, %v), executor calls=%d", worked, err, executor.calls)
	}
	stored, err := repository.GetMessage(ctx, tenantID, message.ID)
	if err != nil || stored.State != domain.MessageStateUncertain {
		t.Fatalf("provider ambiguity state = (%+v, %v)", stored, err)
	}
}
