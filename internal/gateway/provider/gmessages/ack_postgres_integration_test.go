//go:build postgres_integration

package gmessages

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
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

func TestPostgresIntegrationACKLimiterReservesPoolAndRenewsLeases(t *testing.T) {
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
	schema := fmt.Sprintf("sirenaix_ack_limit_it_%d", time.Now().UnixNano())
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
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
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
	const tenantID = domain.TenantID("tenant-ack-limiter")
	if err = repository.SaveTenant(ctx, domain.Tenant{ID: tenantID, Name: "ACK limiter tenant"}); err != nil {
		t.Fatal(err)
	}
	inbox, err := ingress.NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewDurableSink(DurableSinkConfig{
		Inbox: inbox, ACKs: repository, Sealer: postgresIntegrationSealer{},
		ACKTimeout: 2 * time.Second, ACKConcurrency: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	const total = 64
	ownerships := make([]connectionactor.ProviderOwnership, 0, total)
	responseIDs := make([]string, 0, total)
	for index := 0; index < total; index++ {
		connectionID := domain.ConnectionID(fmt.Sprintf("connection-ack-%02d", index))
		responseID := fmt.Sprintf("response-ack-%02d", index)
		if err = repository.SaveConnection(ctx, tenantID, postgres.ConnectionRecord{Connection: domain.Connection{
			ID: connectionID, TenantID: tenantID, State: domain.ConnectionStateConnected,
		}, ProviderDeviceFingerprint: bytes.Repeat([]byte{byte(index + 1)}, 32)}); err != nil {
			t.Fatalf("save connection %d: %v", index, err)
		}
		lease, acquired, acquireErr := repository.AcquireConnectionLease(ctx, tenantID, connectionID, "actor-owner", 30*time.Second)
		if acquireErr != nil || !acquired {
			t.Fatalf("acquire connection %d = (%+v, %v, %v)", index, lease, acquired, acquireErr)
		}
		processed, processErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantID, ConnectionID: connectionID, OwnerID: "actor-owner", FencingToken: lease.FencingToken,
			ProviderResponseID: responseID, Raw: []byte(responseID),
		})
		if processErr != nil || !processed.ACKEligible {
			t.Fatalf("seed ACK %d = (%+v, %v)", index, processed, processErr)
		}
		ownerships = append(ownerships, connectionactor.ProviderOwnership{
			Key:     connectionactor.Key{TenantID: tenantID, ConnectionID: connectionID},
			OwnerID: "actor-owner", FencingToken: lease.FencingToken, LeaseTTL: time.Minute,
		})
		responseIDs = append(responseIDs, responseID)
	}

	started := make(chan struct{}, total)
	release := make(chan struct{})
	results := make(chan error, total)
	for index := 0; index < total; index++ {
		index := index
		go func() {
			result, coordinateErr := sink.CoordinateACKs(ctx, ownerships[index], []string{responseIDs[index]}, func(sendCtx context.Context, _ []string) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-sendCtx.Done():
					return sendCtx.Err()
				}
			})
			if coordinateErr == nil && (len(result.AdmittedIDs) != 1 || result.ProviderError != nil) {
				coordinateErr = fmt.Errorf("unexpected ACK result: %+v", result)
			}
			results <- coordinateErr
		}()
	}
	for index := 0; index < 8; index++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d ACK transactions reached provider I/O", index)
		}
	}
	select {
	case <-started:
		t.Fatal("ninth ACK transaction consumed a database connection before admission was released")
	case <-time.After(150 * time.Millisecond):
	}
	if stats := db.Stats(); stats.InUse > 8 || stats.OpenConnections >= 32 {
		t.Fatalf("limited ACK DB usage = in-use %d open %d", stats.InUse, stats.OpenConnections)
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, 500*time.Millisecond)
	var one int
	if err = db.QueryRowContext(queryCtx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		cancelQuery()
		t.Fatalf("unrelated query starved by ACK I/O = (%d, %v)", one, err)
	}
	cancelQuery()
	close(release)
	for index := 0; index < total; index++ {
		select {
		case resultErr := <-results:
			if resultErr != nil {
				t.Fatalf("ACK coordination %d: %v", index, resultErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("ACK coordination %d did not join", index)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", string(tenantID)); err != nil {
		t.Fatal(err)
	}
	var renewed int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM connection_leases
WHERE tenant_id = $1 AND owner_id = 'actor-owner' AND expires_at > clock_timestamp() + interval '50 seconds'`, string(tenantID)).Scan(&renewed); err != nil {
		t.Fatal(err)
	}
	if renewed != total {
		t.Fatalf("renewed ACK leases = %d, want %d", renewed, total)
	}
}
