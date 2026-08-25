//go:build postgres_integration

package postgres

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	gatewaykafka "go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func integrationProviderCursor(t *testing.T, id string, timestamp int64) []byte {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&gmproto.Cursor{
		LastItemID: id, LastItemTimestamp: timestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestPostgresIntegrationTenantIsolationAndContactConvergence(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	if err := adminDB.PingContext(ctx); err != nil {
		adminDB.Close()
		t.Fatalf("ping admin database: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	role := "sirenaix_it_" + suffix
	schema := "sirenaix_it_" + suffix
	password := "integration-" + uuid.NewString()
	if _, err := adminDB.ExecContext(ctx, "CREATE ROLE "+pq.QuoteIdentifier(role)+" LOGIN PASSWORD "+pq.QuoteLiteral(password)+" NOSUPERUSER NOBYPASSRLS"); err != nil {
		adminDB.Close()
		t.Fatalf("create application role: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)+" AUTHORIZATION "+pq.QuoteIdentifier(role)); err != nil {
		_, _ = adminDB.ExecContext(ctx, "DROP ROLE "+pq.QuoteIdentifier(role))
		adminDB.Close()
		t.Fatalf("create isolated schema: %v", err)
	}
	var appDB *sql.DB
	t.Cleanup(func() {
		if appDB != nil {
			_ = appDB.Close()
		}
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
		_, _ = adminDB.ExecContext(context.Background(), "DROP ROLE "+pq.QuoteIdentifier(role))
		_ = adminDB.Close()
	})

	appDSN := applicationDSN(t, adminDSN, role, password, schema)
	appDB, err = sql.Open("postgres", appDSN)
	if err != nil {
		t.Fatalf("open application database: %v", err)
	}
	appDB.SetMaxOpenConns(16)
	if err := appDB.PingContext(ctx); err != nil {
		t.Fatalf("ping application database: %v", err)
	}

	migrations, err := Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	for _, migration := range migrations {
		contents, readErr := Migrations.ReadFile("migrations/" + migration.Name())
		if readErr != nil {
			t.Fatalf("read embedded migration %s: %v", migration.Name(), readErr)
		}
		if _, err := appDB.ExecContext(ctx, string(contents)); err != nil {
			t.Fatalf("apply real migration %s as application owner: %v", migration.Name(), err)
		}
	}
	assertApplicationRoleCannotBypassRLS(t, ctx, appDB, role)

	repository, err := New(appDB)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	seedTenantAndConnection(t, ctx, repository, "tenant-a", "connection-a", byte('a'))
	seedTenantAndConnection(t, ctx, repository, "tenant-b", "connection-b", byte('b'))
	exerciseTask8ContactsAndLines(t, ctx, repository, appDB)
	exerciseTask7Durability(t, ctx, repository, appDB, adminDB, appDSN)
	exercisePairingHardening(t, ctx, repository)
	exerciseConnectionLeaseFencing(t, ctx, repository)
	exerciseActorTableTenantIsolation(t, ctx, repository, appDB)
	exerciseReauthorizationHealthLockOrder(t, ctx, repository, appDB, adminDB, appDSN)

	connectionsA, err := repository.ListConnections(ctx, "tenant-a")
	if err != nil || len(connectionsA) != 1 || connectionsA[0].Connection.ID != "connection-a" {
		t.Fatalf("tenant A connections = %#v, error = %v", connectionsA, err)
	}
	connectionsB, err := repository.ListConnections(ctx, "tenant-b")
	if err != nil || len(connectionsB) != 1 || connectionsB[0].Connection.ID != "connection-b" {
		t.Fatalf("tenant B connections = %#v, error = %v", connectionsB, err)
	}
	if _, err := repository.GetConnection(ctx, "tenant-a", "connection-b"); !errors.Is(err, contactsync.ErrConnectionNotFound) {
		t.Fatalf("cross-tenant GetConnection() error = %v, want ErrConnectionNotFound", err)
	}
	assertTenantARLSCannotReadOrWriteTenantB(t, ctx, appDB)

	phone, _ := domain.ParseE164("+12025550199")
	update := contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderContactID: "provider-7",
		Phone: phone, ProviderDisplayName: "Alice Original",
	}
	first, err := repository.UpsertProviderContact(ctx, update)
	if err != nil {
		t.Fatalf("first provider upsert: %v", err)
	}
	if err := repository.SetContactAlias(ctx, "tenant-a", first.ID, "VIP customer"); err != nil {
		t.Fatalf("set server alias: %v", err)
	}
	label, _ := domain.NewLabel("label-vip", "tenant-a", "VIP")
	if err := repository.CreateLabel(ctx, "tenant-a", label); err != nil {
		t.Fatalf("create label: %v", err)
	}
	if err := repository.AttachLabel(ctx, "tenant-a", first.ID, label.ID); err != nil {
		t.Fatalf("attach label: %v", err)
	}
	update.ProviderDisplayName = "Alice Updated"
	second, err := repository.UpsertProviderContact(ctx, update)
	if err != nil {
		t.Fatalf("repeat provider upsert: %v", err)
	}
	if second.ID != first.ID || second.ProviderDisplayName != "Alice Updated" || second.Alias != "VIP customer" || len(second.LabelIDs) != 1 || second.LabelIDs[0] != "label-vip" {
		t.Fatalf("repeat upsert did not preserve canonical metadata: first=%#v second=%#v", first, second)
	}
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM contacts WHERE tenant_id = $1 AND normalized_phone = $2", []any{"tenant-a", phone.String()}, 1)
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM provider_contact_sources WHERE tenant_id = $1 AND connection_id = $2 AND provider_contact_id = $3", []any{"tenant-a", "connection-a", "provider-7"}, 1)
	if err := repository.DetachLabel(ctx, "tenant-a", first.ID, label.ID); err != nil {
		t.Fatalf("detach existing label: %v", err)
	}
	if err := repository.DetachLabel(ctx, "tenant-a", first.ID, label.ID); err != nil {
		t.Fatalf("idempotent detach absent link: %v", err)
	}
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM contact_labels WHERE tenant_id = $1 AND contact_id = $2 AND label_id = $3", []any{"tenant-a", string(first.ID), string(label.ID)}, 0)
	if err := repository.DetachLabel(ctx, "tenant-a", "contact-missing", label.ID); !errors.Is(err, ErrContactNotFound) {
		t.Fatalf("detach missing contact error = %v, want ErrContactNotFound", err)
	}
	if err := repository.DetachLabel(ctx, "tenant-a", first.ID, "label-missing"); !errors.Is(err, ErrLabelNotFound) {
		t.Fatalf("detach missing label error = %v, want ErrLabelNotFound", err)
	}

	racePhone, _ := domain.ParseE164("+13035550123")
	raceUpdate := contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderContactID: "provider-race",
		Phone: racePhone, ProviderDisplayName: "Concurrent Name",
	}
	const writers = 8
	results := make(chan domain.Contact, writers)
	errorsCh := make(chan error, writers)
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			contact, err := repository.UpsertProviderContact(ctx, raceUpdate)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- contact
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("concurrent provider upsert: %v", err)
	}
	var raceContactID domain.ContactID
	for contact := range results {
		if raceContactID == "" {
			raceContactID = contact.ID
		} else if contact.ID != raceContactID {
			t.Errorf("concurrent upserts returned contacts %q and %q", raceContactID, contact.ID)
		}
	}
	if raceContactID == "" {
		t.Fatal("concurrent upserts returned no contact")
	}
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM contacts WHERE tenant_id = $1 AND normalized_phone = $2", []any{"tenant-a", racePhone.String()}, 1)
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM provider_contact_sources WHERE tenant_id = $1 AND connection_id = $2 AND provider_contact_id = $3 AND contact_id = $4 AND normalized_phone = $5", []any{"tenant-a", "connection-a", "provider-race", string(raceContactID), racePhone.String()}, 1)

	conflictingPhone, _ := domain.ParseE164("+13035550124")
	raceUpdate.Phone = conflictingPhone
	if _, err := repository.UpsertProviderContact(ctx, raceUpdate); !errors.Is(err, contactsync.ErrProviderIdentityConflict) {
		t.Fatalf("immutable-source conflict error = %v, want ErrProviderIdentityConflict", err)
	}
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM contacts WHERE tenant_id = $1 AND normalized_phone = $2", []any{"tenant-a", conflictingPhone.String()}, 0)
	assertTenantCount(t, ctx, appDB, "tenant-a", "SELECT count(*) FROM provider_contact_sources WHERE tenant_id = $1 AND connection_id = $2 AND provider_contact_id = $3 AND contact_id = $4 AND normalized_phone = $5", []any{"tenant-a", "connection-a", "provider-race", string(raceContactID), racePhone.String()}, 1)
}

func exerciseTask8ContactsAndLines(t *testing.T, ctx context.Context, repository *Repository, db *sql.DB) {
	t.Helper()
	phone, _ := domain.ParseE164("+14155550123")
	alias := "Potential client"
	created, err := repository.UpsertServerContact(ctx, "tenant-a", "task8-server-contact-a", phone, &alias)
	if err != nil || created.ID != "task8-server-contact-a" || created.Alias != alias || created.ProviderDisplayName != "" {
		t.Fatalf("server-first contact = %#v, %v", created, err)
	}
	label, _ := domain.NewLabel("task8-lead", "tenant-a", "AI Lead")
	if err = repository.CreateLabel(ctx, "tenant-a", label); err != nil {
		t.Fatal(err)
	}
	if err = repository.AttachLabel(ctx, "tenant-a", created.ID, label.ID); err != nil {
		t.Fatal(err)
	}
	providerMerged, err := repository.UpsertProviderContact(ctx, contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderContactID: "task8-provider-contact",
		Phone: phone, ProviderDisplayName: "Provider Name",
	})
	if err != nil || providerMerged.ID != created.ID || providerMerged.Alias != alias || providerMerged.ProviderDisplayName != "Provider Name" ||
		len(providerMerged.LabelIDs) != 1 || providerMerged.LabelIDs[0] != label.ID {
		t.Fatalf("provider sync did not preserve server fields = %#v, %v", providerMerged, err)
	}
	preserved, err := repository.UpsertServerContact(ctx, "tenant-a", "must-not-replace-id", phone, nil)
	if err != nil || preserved.ID != created.ID || preserved.Alias != alias || preserved.ProviderDisplayName != "Provider Name" || len(preserved.LabelIDs) != 1 {
		t.Fatalf("idempotent contact convergence = %#v, %v", preserved, err)
	}
	tenantB, err := repository.UpsertServerContact(ctx, "tenant-b", "task8-server-contact-b", phone, &alias)
	if err != nil || tenantB.ID != "task8-server-contact-b" || tenantB.TenantID != "tenant-b" {
		t.Fatalf("tenant B same-phone contact = %#v, %v", tenantB, err)
	}
	if err = repository.SetContactAlias(ctx, "tenant-b", created.ID, "cross tenant"); !errors.Is(err, ErrContactNotFound) {
		t.Fatalf("cross-tenant server alias = %v", err)
	}
	assertTenantCount(t, ctx, db, "tenant-a", "SELECT count(*) FROM contacts WHERE tenant_id = $1 AND normalized_phone = $2", []any{"tenant-a", phone.String()}, 1)
	assertTenantCount(t, ctx, db, "tenant-b", "SELECT count(*) FROM contacts WHERE tenant_id = $1 AND normalized_phone = $2", []any{"tenant-b", phone.String()}, 1)

	linePhoneA, _ := domain.ParseE164("+12025550101")
	linePhoneB, _ := domain.ParseE164("+12025550102")
	lineA := LineRecord{
		Line:  domain.Line{ID: "task8-line-a", TenantID: "tenant-a", ConnectionID: "connection-a", ProviderParticipantID: "participant-a", ProviderOutgoingID: "outgoing-a", DisplayName: "SIM 1"},
		Phone: linePhoneA, CarrierName: "Carrier A", ColorHex: "#123456", RCSEnabled: true,
		ProviderSIMNumber: 1, ProviderSIMPayloadType: 7, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
	}
	lineB := LineRecord{
		Line:  domain.Line{ID: "task8-line-b", TenantID: "tenant-b", ConnectionID: "connection-b", ProviderParticipantID: "participant-b", ProviderOutgoingID: "outgoing-b", DisplayName: "SIM 2"},
		Phone: linePhoneB, CarrierName: "Carrier B", ColorHex: "#654321", RCSEnabled: false,
		ProviderSIMNumber: 2, ProviderSIMPayloadType: 8, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
	}
	leaseA, acquired, leaseErr := repository.AcquireConnectionLease(ctx, "tenant-a", "connection-a", "task8-lines-a", time.Minute)
	if leaseErr != nil || !acquired {
		t.Fatalf("acquire tenant A line lease = (%+v, %v, %v)", leaseA, acquired, leaseErr)
	}
	defer func() {
		_, _ = repository.ReleaseConnectionLease(ctx, "tenant-a", "connection-a", "task8-lines-a", leaseA.FencingToken)
	}()
	leaseB, acquired, leaseErr := repository.AcquireConnectionLease(ctx, "tenant-b", "connection-b", "task8-lines-b", time.Minute)
	if leaseErr != nil || !acquired {
		t.Fatalf("acquire tenant B line lease = (%+v, %v, %v)", leaseB, acquired, leaseErr)
	}
	defer func() {
		_, _ = repository.ReleaseConnectionLease(ctx, "tenant-b", "connection-b", "task8-lines-b", leaseB.FencingToken)
	}()
	if err = repository.ReplaceLinesFenced(ctx, "tenant-a", "connection-a", "task8-lines-a", leaseA.FencingToken, []LineRecord{lineA}); err != nil {
		t.Fatal(err)
	}
	if err = repository.ReplaceLinesFenced(ctx, "tenant-b", "connection-b", "task8-lines-b", leaseB.FencingToken, []LineRecord{lineB}); err != nil {
		t.Fatal(err)
	}
	lines, err := repository.ListLines(ctx, "tenant-a", "connection-a")
	if err != nil || len(lines) != 1 || lines[0].CarrierName != "Carrier A" || !lines[0].RCSEnabled ||
		lines[0].ProviderSIMNumber != 1 || lines[0].ProviderSIMPayloadType != 7 || lines[0].DiscoverySource != LineDiscoveryAuthenticatedGoogleSettings {
		t.Fatalf("authenticated line metadata = %#v, %v", lines, err)
	}
	if crossTenant, listErr := repository.ListLines(ctx, "tenant-a", "connection-b"); listErr != nil || len(crossTenant) != 0 {
		t.Fatalf("cross-tenant lines = %#v, %v", crossTenant, listErr)
	}

	// A Settings refresh must never delete a line that a durable message still
	// references. Retire the absent line, expose only the current snapshot, and
	// prove an exact repeat is idempotent under the production lease fence.
	const retainedLineMessageID = "task8-explicit-line-message"
	if err = inTenantExec(ctx, repository, "tenant-a", func(tx transaction) error {
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO messages (
            tenant_id, message_id, connection_id, conversation_id, ordering_key,
            direction, line_id, body_text, provider_tmp_id, current_state
        ) VALUES ($1, $2, $3, '', $2, 'outbound', $4, 'retained line reference', $5, 'sent')`,
			"tenant-a", retainedLineMessageID, "connection-a", string(lineA.Line.ID), "sx-task8-retained-line")
		return insertErr
	}); err != nil {
		t.Fatalf("seed explicit-line message: %v", err)
	}
	replacementPhone, _ := domain.ParseE164("+12025550105")
	replacementLine := LineRecord{
		Line: domain.Line{ID: "task8-line-current", TenantID: "tenant-a", ConnectionID: "connection-a",
			ProviderParticipantID: "participant-current", ProviderOutgoingID: "outgoing-current", DisplayName: "Current SIM"},
		Phone: replacementPhone, CarrierName: "Carrier Current", ColorHex: "#112233", RCSEnabled: true,
		ProviderSIMNumber: 3, ProviderSIMPayloadType: 9, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
	}
	for attempt := range 2 {
		if err = repository.ReplaceLinesFenced(ctx, "tenant-a", "connection-a", "task8-lines-a", leaseA.FencingToken, []LineRecord{replacementLine}); err != nil {
			t.Fatalf("FK-safe fenced line refresh attempt %d: %v", attempt+1, err)
		}
	}
	lines, err = repository.ListLines(ctx, "tenant-a", "connection-a")
	if err != nil || len(lines) != 1 || lines[0].Line.ID != replacementLine.Line.ID {
		t.Fatalf("active line snapshot after retirement = %#v, %v", lines, err)
	}
	var retainedActive bool
	var retainedMessageLine string
	if _, err = inTenant(ctx, repository, "tenant-a", func(tx transaction) (struct{}, error) {
		return struct{}{}, tx.QueryRowContext(ctx, `SELECT line.active, message.line_id
            FROM lines AS line
            JOIN messages AS message ON message.tenant_id = line.tenant_id AND message.line_id = line.line_id
            WHERE line.tenant_id = $1 AND line.line_id = $2 AND message.message_id = $3`,
			"tenant-a", string(lineA.Line.ID), retainedLineMessageID).Scan(&retainedActive, &retainedMessageLine)
	}); err != nil || retainedActive || retainedMessageLine != string(lineA.Line.ID) {
		t.Fatalf("retained explicit-line reference = active:%v line:%q error:%v", retainedActive, retainedMessageLine, err)
	}
	assertTenantCount(t, ctx, db, "tenant-a", `SELECT count(*) FROM lines
        WHERE tenant_id = $1 AND connection_id = $2`, []any{"tenant-a", "connection-a"}, 2)
	assertTenantCount(t, ctx, db, "tenant-a", `SELECT count(*) FROM lines
        WHERE tenant_id = $1 AND connection_id = $2 AND active`, []any{"tenant-a", "connection-a"}, 1)

	// Authenticated Settings discovery is committed by the durable inbox under
	// the same generation fence. A successful ACK therefore cannot race ahead
	// of line persistence, and a stale generation cannot mutate either table.
	inbox, err := ingress.NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	settingsLine := ingress.ProjectedLine{
		ID: "task8-settings-line-a", TenantID: "tenant-a", ConnectionID: "connection-a",
		ProviderParticipantID: "settings-participant-a", ProviderOutgoingID: "settings-outgoing-a",
		Phone: "+12025550103", DisplayName: "Authenticated SIM", CarrierName: "Carrier Settings", ColorHex: "#abcdef",
		RCSEnabled: true, ProviderSIMNumber: 3, ProviderSIMPayloadType: 9,
		DiscoverySource: ingress.LineDiscoveryAuthenticatedGoogleSettings,
	}
	settingsResult, err := inbox.Process(ctx, ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "task8-lines-a", FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task8-settings-atomic", Raw: []byte("task8-settings-atomic"),
		Projection: ingress.Projection{LineSnapshot: true, Lines: []ingress.ProjectedLine{settingsLine}},
	})
	if err != nil || !settingsResult.ACKEligible || settingsResult.Poisoned {
		t.Fatalf("atomic Settings commit = (%+v, %v)", settingsResult, err)
	}
	lines, err = repository.ListLines(ctx, "tenant-a", "connection-a")
	if err != nil || len(lines) != 1 || lines[0].Line.ID != settingsLine.ID || lines[0].CarrierName != settingsLine.CarrierName ||
		lines[0].ProviderSIMNumber != settingsLine.ProviderSIMNumber || lines[0].DiscoverySource != LineDiscoveryAuthenticatedGoogleSettings {
		t.Fatalf("atomic Settings line snapshot = %#v, %v", lines, err)
	}
	assertTenantCount(t, ctx, db, "tenant-a",
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3 AND ack_pending",
		[]any{"tenant-a", "connection-a", "task8-settings-atomic"}, 1)

	staleLine := settingsLine
	staleLine.ID = "task8-settings-stale"
	staleLine.ProviderParticipantID = "settings-participant-stale"
	staleLine.ProviderOutgoingID = "settings-outgoing-stale"
	staleLine.Phone = "+12025550104"
	if staleResult, staleErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "task8-lines-a", FencingToken: leaseA.FencingToken + 1,
		ProviderResponseID: "task8-settings-stale", Raw: []byte("task8-settings-stale"),
		Projection: ingress.Projection{LineSnapshot: true, Lines: []ingress.ProjectedLine{staleLine}},
	}); staleErr == nil || staleResult.ACKEligible {
		t.Fatalf("stale Settings generation = (%+v, %v)", staleResult, staleErr)
	}
	lines, err = repository.ListLines(ctx, "tenant-a", "connection-a")
	if err != nil || len(lines) != 1 || lines[0].Line.ID != settingsLine.ID {
		t.Fatalf("stale Settings mutated line snapshot = %#v, %v", lines, err)
	}
	assertTenantCount(t, ctx, db, "tenant-a",
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{"tenant-a", "connection-a", "task8-settings-stale"}, 0)

	terminalRaw := []byte("task8-impossible-settings-snapshot")
	terminal, terminalErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "task8-lines-a", FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task8-settings-terminal", Raw: terminalRaw,
		DecodeError: errors.New("authenticated settings snapshot exceeded the durable line cap"),
		ACKWithheld: true, PoisonReason: ingress.PoisonReasonInvalidSettingsSnapshot,
	})
	if terminalErr != nil || terminal.ACKEligible || !terminal.ACKWithheld || !terminal.Poisoned {
		t.Fatalf("terminal Settings poison = (%+v, %v)", terminal, terminalErr)
	}
	var ackPending bool
	var poisonReason string
	if _, err = inTenant(ctx, repository, "tenant-a", func(tx transaction) (struct{}, error) {
		return struct{}{}, tx.QueryRowContext(ctx, `SELECT ack_pending, poison_reason FROM provider_inbox
            WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3`,
			"tenant-a", "connection-a", "task8-settings-terminal").Scan(&ackPending, &poisonReason)
	}); err != nil || ackPending || poisonReason != ingress.PoisonReasonInvalidSettingsSnapshot {
		t.Fatalf("terminal Settings durable state = pending:%v reason:%q error:%v", ackPending, poisonReason, err)
	}
	restartedRepository, restartErr := New(db)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	restartedInbox, restartErr := ingress.NewService(restartedRepository)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	recovered, recoveredErr := restartedInbox.Process(ctx, ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "task8-lines-a", FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task8-settings-terminal", Raw: terminalRaw,
	})
	if recoveredErr != nil || recovered.ACKEligible || !recovered.ACKWithheld || !recovered.Poisoned || !recovered.Duplicate {
		t.Fatalf("restarted terminal Settings replay = (%+v, %v)", recovered, recoveredErr)
	}
}

func exerciseTask7Durability(t *testing.T, ctx context.Context, repository *Repository, db, adminDB *sql.DB, appDSN string) {
	t.Helper()
	const (
		tenantA      = domain.TenantID("task7-tenant-a")
		tenantB      = domain.TenantID("task7-tenant-b")
		connectionA  = domain.ConnectionID("task7-connection-a")
		connectionB  = domain.ConnectionID("task7-connection-b")
		conversation = "task7-conversation"
		ownerA       = "task7-owner-a"
		ownerB       = "task7-owner-b"
	)
	seedTenantAndConnection(t, ctx, repository, tenantA, connectionA, byte('7'))
	seedTenantAndConnection(t, ctx, repository, tenantB, connectionB, byte('8'))
	for _, pair := range []struct {
		tenant     domain.TenantID
		connection domain.ConnectionID
	}{
		{tenantA, connectionA}, {tenantB, connectionB},
	} {
		if err := inTenantExec(ctx, repository, pair.tenant, func(tx transaction) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO conversations
                (tenant_id, connection_id, conversation_id, provider_default_outgoing_id)
                VALUES ($1, $2, $3, 'outgoing-default')`, string(pair.tenant), string(pair.connection), conversation)
			return err
		}); err != nil {
			t.Fatalf("seed Task 7 conversation: %v", err)
		}
	}
	leaseA, acquired, err := repository.AcquireConnectionLease(ctx, tenantA, connectionA, ownerA, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire Task 7 tenant A lease = %#v, %v, %v", leaseA, acquired, err)
	}
	leaseB, acquired, err := repository.AcquireConnectionLease(ctx, tenantB, connectionB, ownerB, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire Task 7 tenant B lease = %#v, %v, %v", leaseB, acquired, err)
	}

	// A provider list cursor is not visible until every child in the staged
	// page is durable. This exercises the real RLS transaction and the restart
	// representation rather than only matching the SQL text.
	backfillPage := messaging.BackfillPage{
		NextCursor: []byte("task7-next-page"),
		Items: []messaging.BackfillItem{
			{Ordinal: 0, ConversationID: "task7-backfill-a", State: messaging.BackfillItemPending},
			{Ordinal: 1, ConversationID: "task7-backfill-b", State: messaging.BackfillItemPending},
		},
	}
	if err = repository.StageBackfillPageFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, backfillPage); err != nil {
		t.Fatalf("stage real backfill checkpoint: %v", err)
	}
	checkpoint, err := repository.LoadBackfillCheckpoint(ctx, tenantA, connectionA)
	if err != nil || checkpoint == nil || len(checkpoint.Items) != 2 {
		t.Fatalf("load staged backfill checkpoint = %+v, %v", checkpoint, err)
	}
	if err = repository.MarkBackfillItemFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, checkpoint.ID, 0, messaging.BackfillItemComplete, ""); err != nil {
		t.Fatalf("complete first backfill child: %v", err)
	}
	if err = repository.CompleteBackfillPageFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, checkpoint.ID); !errors.Is(err, messaging.ErrBackfillCheckpointConflict) {
		t.Fatalf("partial checkpoint advanced list cursor: %v", err)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantA, connectionA, "_provider_page"); cursorErr != nil || len(cursor) != 0 {
		t.Fatalf("partial checkpoint cursor = %q, %v", cursor, cursorErr)
	}
	if err = repository.MarkBackfillItemFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, checkpoint.ID, 1, messaging.BackfillItemComplete, ""); err != nil {
		t.Fatalf("complete second backfill child: %v", err)
	}
	if err = repository.CompleteBackfillPageFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, checkpoint.ID); err != nil {
		t.Fatalf("advance completed backfill page: %v", err)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantA, connectionA, "_provider_page"); cursorErr != nil || !bytes.Equal(cursor, backfillPage.NextCursor) {
		t.Fatalf("completed checkpoint cursor = %q, %v", cursor, cursorErr)
	}
	if checkpoint, err = repository.LoadBackfillCheckpoint(ctx, tenantA, connectionA); err != nil || checkpoint != nil {
		t.Fatalf("completed checkpoint survived = %+v, %v", checkpoint, err)
	}

	// Exercise the production durable response path for an empty nonterminal
	// message page. The requested child target, not message cardinality, owns
	// the cursor and the completed parent cursor remains unchanged.
	inbox, err := ingress.NewService(repository)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name             string
		providerMessage  string
		initialDirection string
		initialStatus    string
		initialState     domain.MessageState
		initialProv      ingress.MessageProvenance
	}{
		{name: "history", providerMessage: "task8-history-before-live", initialDirection: "inbound", initialStatus: "INCOMING_COMPLETE", initialState: domain.MessageStateDelivered, initialProv: ingress.MessageProvenanceHistory},
		{name: "pending MMS", providerMessage: "task8-pending-before-live", initialDirection: "unknown", initialStatus: "INCOMING_AUTO_DOWNLOADING", initialProv: ingress.MessageProvenanceLive},
		{name: "delivered-only status", providerMessage: "task8-delivered-before-complete", initialDirection: "inbound", initialStatus: "INCOMING_DELIVERED", initialState: domain.MessageStateDelivered, initialProv: ingress.MessageProvenanceLive},
	} {
		t.Run("actionable promotion after "+fixture.name, func(t *testing.T) {
			initialProjection := ingress.Projection{Messages: []ingress.ProjectedMessage{{
				ProviderMessageID: fixture.providerMessage, ConversationID: conversation,
				Direction: fixture.initialDirection, Provenance: fixture.initialProv, ProviderStatus: fixture.initialStatus,
				ProviderOccurredAt: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), Text: "pre-live snapshot",
				Transport: "mms", State: fixture.initialState,
			}}}
			if initial, initialErr := inbox.Process(ctx, ingress.Envelope{
				TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
				ProviderResponseID: "task8-initial-" + fixture.providerMessage, Raw: []byte("initial-" + fixture.providerMessage), Projection: initialProjection,
			}); initialErr != nil || !initial.ACKEligible || initial.Poisoned {
				t.Fatalf("initial nonactionable projection = (%+v, %v)", initial, initialErr)
			}
			var messageID string
			if queryErr := inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
				return tx.QueryRowContext(ctx, `SELECT message_id FROM messages
                    WHERE tenant_id = $1 AND connection_id = $2 AND provider_message_id = $3`,
					string(tenantA), string(connectionA), fixture.providerMessage).Scan(&messageID)
			}); queryErr != nil {
				t.Fatalf("load nonactionable message: %v", queryErr)
			}
			assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM gateway_events
                WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'message.received'`, []any{string(tenantA), messageID}, 0)

			restartedRepository, restartErr := New(db)
			if restartErr != nil {
				t.Fatal(restartErr)
			}
			restartedInbox, restartErr := ingress.NewService(restartedRepository)
			if restartErr != nil {
				t.Fatal(restartErr)
			}
			completeProjection := ingress.Projection{Messages: []ingress.ProjectedMessage{{
				ProviderMessageID: fixture.providerMessage, ConversationID: conversation,
				Direction: "inbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE",
				ProviderOccurredAt: time.Date(2026, 8, 25, 10, 20, 0, 0, time.UTC), Actionable: true,
				Sender: "+12025550100", Text: "live completion", Transport: "mms", State: domain.MessageStateDelivered,
			}}}
			for attempt := range 2 {
				result, processErr := restartedInbox.Process(ctx, ingress.Envelope{
					TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
					ProviderResponseID: fmt.Sprintf("task8-complete-%s-%d", fixture.providerMessage, attempt),
					Raw:                []byte(fmt.Sprintf("complete-%s-%d", fixture.providerMessage, attempt)), Projection: completeProjection,
				})
				if processErr != nil || !result.ACKEligible || result.Poisoned {
					t.Fatalf("live completion attempt %d = (%+v, %v)", attempt+1, result, processErr)
				}
			}
			assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM gateway_events
                WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'message.received'`, []any{string(tenantA), messageID}, 1)
			assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM event_outbox AS outbox
                JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
                WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2 AND event.event_type = 'message.received'`, []any{string(tenantA), messageID}, 2)
			var storedDirection string
			if queryErr := inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
				return tx.QueryRowContext(ctx, `SELECT direction FROM messages
                    WHERE tenant_id = $1 AND message_id = $2`, string(tenantA), messageID).Scan(&storedDirection)
			}); queryErr != nil || storedDirection != "inbound" {
				t.Fatalf("refined durable direction = %q, %v", storedDirection, queryErr)
			}
		})
	}
	inboundResult, err := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task8-actionable-event-response", Raw: []byte("task8-actionable-event-raw"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "task8-provider-message", ConversationID: conversation,
			Direction: "inbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE",
			ProviderOccurredAt: time.Date(2026, 8, 25, 10, 30, 0, 0, time.UTC), Actionable: true, Sender: "+12025550100",
			Text: "Can I schedule Tuesday?", Transport: "mms", State: domain.MessageStateDelivered,
		}}},
	})
	if err != nil || !inboundResult.ACKEligible || inboundResult.Poisoned {
		t.Fatalf("persist actionable message event = (%+v, %v)", inboundResult, err)
	}
	var canonicalBody []byte
	var actionableMessageID string
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		if queryErr := tx.QueryRowContext(ctx, `SELECT message_id FROM messages
			WHERE tenant_id = $1 AND connection_id = $2 AND provider_message_id = $3`,
			string(tenantA), string(connectionA), "task8-provider-message").Scan(&actionableMessageID); queryErr != nil {
			return queryErr
		}
		return tx.QueryRowContext(ctx, `SELECT canonical_body FROM gateway_events
			WHERE tenant_id = $1 AND event_type = 'message.received' AND aggregate_id = $2`,
			string(tenantA), actionableMessageID).Scan(&canonicalBody)
	}); err != nil {
		t.Fatalf("load actionable message event: %v", err)
	}
	var event map[string]any
	if err = json.Unmarshal(canonicalBody, &event); err != nil || event["version"] != float64(1) || event["tenant_id"] != string(tenantA) ||
		event["connection_id"] != string(connectionA) || event["conversation_id"] != conversation || event["message_id"] == "" ||
		event["provider_message_id"] != "task8-provider-message" || event["direction"] != "inbound" || event["sender"] != "+12025550100" ||
		event["text"] != "Can I schedule Tuesday?" || event["transport"] != "mms" || event["status"] != "delivered" ||
		event["provenance"] != string(ingress.MessageProvenanceLive) || event["provider_status"] != "INCOMING_COMPLETE" || event["actionable"] != true ||
		event["occurred_at"] != "2026-08-25T10:30:00Z" || event["ingested_at"] == "" {
		t.Fatalf("actionable message event contract = %#v, error %v", event, err)
	}
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM event_outbox AS outbox
		JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
		WHERE outbox.tenant_id = $1 AND event.aggregate_id = $2 AND outbox.destination IN ('webhook', 'kafka')
		  AND event.canonical_body = $3`, []any{string(tenantA), actionableMessageID, canonicalBody}, 2)

	// A different provider response after a process restart carries the same
	// message identity. The immutable actionable event and its two destination
	// outboxes remain singular.
	actionableRestartRepository, actionableRestartErr := New(db)
	if actionableRestartErr != nil {
		t.Fatal(actionableRestartErr)
	}
	actionableRestartInbox, actionableRestartErr := ingress.NewService(actionableRestartRepository)
	if actionableRestartErr != nil {
		t.Fatal(actionableRestartErr)
	}
	restartProjection := ingress.Projection{Messages: []ingress.ProjectedMessage{{
		ProviderMessageID: "task8-provider-message", ConversationID: conversation,
		Direction: "inbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE",
		ProviderOccurredAt: time.Date(2026, 8, 25, 10, 30, 0, 0, time.UTC), Actionable: true, Sender: "+12025550100",
		Text: "Can I schedule Tuesday?", Transport: "mms", State: domain.MessageStateDelivered,
	}}}
	if replay, replayErr := actionableRestartInbox.Process(ctx, ingress.Envelope{
		TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task8-actionable-event-response-restart", Raw: []byte("task8-actionable-event-raw-restart"), Projection: restartProjection,
	}); replayErr != nil || !replay.ACKEligible || replay.Poisoned {
		t.Fatalf("restart semantic replay = (%+v, %v)", replay, replayErr)
	}
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM gateway_events
		WHERE tenant_id = $1 AND event_type = 'message.received' AND aggregate_id = $2`, []any{string(tenantA), actionableMessageID}, 1)
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM event_outbox AS outbox
		JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
		WHERE outbox.tenant_id = $1 AND event.event_type = 'message.received' AND event.aggregate_id = $2`, []any{string(tenantA), actionableMessageID}, 2)

	concurrentProjection := ingress.Projection{Messages: []ingress.ProjectedMessage{{
		ProviderMessageID: "task8-provider-concurrent", ConversationID: conversation,
		Direction: "inbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE",
		ProviderOccurredAt: time.Date(2026, 8, 25, 10, 45, 0, 0, time.UTC), Actionable: true,
		Sender: "+12025550101", Text: "Concurrent hello", Transport: "sms", State: domain.MessageStateDelivered,
	}}}
	type semanticResult struct {
		result ingress.ProcessResult
		err    error
	}
	semanticResults := make(chan semanticResult, 2)
	semanticStart := make(chan struct{})
	for index := range 2 {
		index := index
		go func() {
			<-semanticStart
			result, processErr := actionableRestartInbox.Process(ctx, ingress.Envelope{
				TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
				ProviderResponseID: fmt.Sprintf("task8-semantic-race-%d", index), Raw: []byte(fmt.Sprintf("task8-semantic-raw-%d", index)),
				Projection: concurrentProjection,
			})
			semanticResults <- semanticResult{result: result, err: processErr}
		}()
	}
	close(semanticStart)
	for range 2 {
		outcome := <-semanticResults
		if outcome.err != nil || !outcome.result.ACKEligible || outcome.result.Poisoned {
			t.Fatalf("concurrent semantic response = (%+v, %v)", outcome.result, outcome.err)
		}
	}
	var concurrentMessageID string
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		return tx.QueryRowContext(ctx, `SELECT message_id FROM messages
			WHERE tenant_id = $1 AND connection_id = $2 AND provider_message_id = $3`,
			string(tenantA), string(connectionA), "task8-provider-concurrent").Scan(&concurrentMessageID)
	}); err != nil {
		t.Fatalf("load concurrent message identity: %v", err)
	}
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM gateway_events
		WHERE tenant_id = $1 AND event_type = 'message.received' AND aggregate_id = $2`, []any{string(tenantA), concurrentMessageID}, 1)
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM event_outbox AS outbox
		JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
		WHERE outbox.tenant_id = $1 AND event.event_type = 'message.received' AND event.aggregate_id = $2`, []any{string(tenantA), concurrentMessageID}, 2)
	for _, fixture := range []struct {
		name    string
		raw     [2][]byte
		wantErr int
	}{
		{name: "exact", raw: [2][]byte{[]byte("reservation-race-same"), []byte("reservation-race-same")}},
		{name: "different", raw: [2][]byte{[]byte("reservation-race-a"), []byte("reservation-race-b")}, wantErr: 1},
	} {
		t.Run("response reservation "+fixture.name, func(t *testing.T) {
			responseID := "task7-reservation-race-" + fixture.name
			start := make(chan struct{})
			type raceResult struct {
				result ingress.ProcessResult
				err    error
			}
			results := make(chan raceResult, 2)
			var wait sync.WaitGroup
			for index := range 2 {
				index := index
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					result, processErr := inbox.Process(ctx, ingress.Envelope{
						TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
						ProviderResponseID: responseID, Raw: fixture.raw[index],
					})
					results <- raceResult{result: result, err: processErr}
				}()
			}
			close(start)
			wait.Wait()
			close(results)
			errorsSeen, ackEligible := 0, 0
			for outcome := range results {
				if outcome.err != nil {
					if !errors.Is(outcome.err, ingress.ErrConflictingEnvelope) {
						t.Fatalf("reservation race error = %v", outcome.err)
					}
					errorsSeen++
				} else if outcome.result.ACKEligible {
					ackEligible++
				}
			}
			if errorsSeen != fixture.wantErr || ackEligible != 2-fixture.wantErr {
				t.Fatalf("reservation race outcomes errors=%d ACK=%d", errorsSeen, ackEligible)
			}
			assertTenantCount(t, ctx, db, string(tenantA),
				"SELECT count(*) FROM provider_response_reservations WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
				[]any{string(tenantA), string(connectionA), responseID}, 1)
			assertTenantCount(t, ctx, db, string(tenantA),
				"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
				[]any{string(tenantA), string(connectionA), responseID}, 1)
		})
	}
	exerciseProviderACKLinearization(t, ctx, repository, db, inbox, tenantA, connectionA, ownerA, leaseA.FencingToken)
	wantEmptyCursor := integrationProviderCursor(t, "task7-empty-message-cursor", 1)
	emptyResult, err := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task7-empty-message-page", Raw: []byte("task7-empty-message-page"),
		Projection: ingress.Projection{Cursor: wantEmptyCursor, CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-empty-child"},
	})
	if err != nil || !emptyResult.ACKEligible || emptyResult.Poisoned {
		t.Fatalf("empty durable message page = (%+v, %v)", emptyResult, err)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantA, connectionA, "task7-empty-child"); cursorErr != nil || !bytes.Equal(cursor, wantEmptyCursor) {
		t.Fatalf("empty child cursor = %x, %v", cursor, cursorErr)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantA, connectionA, "_provider_page"); cursorErr != nil || !bytes.Equal(cursor, backfillPage.NextCursor) {
		t.Fatalf("empty child page corrupted parent cursor = %x, %v", cursor, cursorErr)
	}
	// Cursor fingerprints survive repository/process replacement. Replaying the
	// same A -> B edge under a new provider response is recoverable (for the
	// crash between inbox commit and checkpoint staging), while B -> A is a
	// durable cycle poison and cannot regress the committed child cursor.
	cycleA, cycleB := integrationProviderCursor(t, "task7-cycle-a", 1), integrationProviderCursor(t, "task7-cycle-b", 2)
	cycleEnvelope := func(repo *Repository, responseID string, base, next []byte) (ingress.ProcessResult, error) {
		service, serviceErr := ingress.NewService(repo)
		if serviceErr != nil {
			return ingress.ProcessResult{}, serviceErr
		}
		return service.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: responseID, Raw: []byte(responseID),
			Projection: ingress.Projection{Cursor: next, CursorBase: base, CursorCandidate: next,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-cycle-child"},
		})
	}
	if cycleResult, cycleErr := cycleEnvelope(repository, "task7-cycle-a-b", cycleA, cycleB); cycleErr != nil || cycleResult.Poisoned {
		t.Fatalf("first cursor edge = (%+v, %v)", cycleResult, cycleErr)
	}
	restartedRepository, restartErr := New(db)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	if cycleResult, cycleErr := cycleEnvelope(restartedRepository, "task7-cycle-a-b-replay", cycleA, cycleB); cycleErr != nil || cycleResult.Poisoned {
		t.Fatalf("same edge after restart = (%+v, %v)", cycleResult, cycleErr)
	}
	restartedAgainRepository, restartErr := New(db)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	if cycleResult, cycleErr := cycleEnvelope(restartedAgainRepository, "task7-cycle-b-a", cycleB, cycleA); cycleErr != nil || !cycleResult.Poisoned {
		t.Fatalf("cycle after second restart = (%+v, %v)", cycleResult, cycleErr)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantA, connectionA, "task7-cycle-child"); cursorErr != nil || !bytes.Equal(cursor, cycleB) {
		t.Fatalf("cycle regressed committed cursor = %q, %v", cursor, cursorErr)
	}
	evictionBase := integrationProviderCursor(t, "task7-eviction-000", 1000)
	for index := 1; index <= 70; index++ {
		next := integrationProviderCursor(t, fmt.Sprintf("task7-eviction-%03d", index), int64(1000+index))
		evictionResult, evictionErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: fmt.Sprintf("task7-eviction-response-%03d", index), Raw: []byte(fmt.Sprintf("task7-eviction-raw-%03d", index)),
			Projection: ingress.Projection{Cursor: next, CursorBase: evictionBase, CursorCandidate: next,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-eviction-child"},
		})
		if evictionErr != nil || evictionResult.Poisoned {
			t.Fatalf("eviction cursor %d = (%+v, %v)", index, evictionResult, evictionErr)
		}
		evictionBase = next
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_cursor_history WHERE tenant_id = $1 AND cursor_scope = $2",
		[]any{string(tenantA), "task7-eviction-child"}, 64)

	// The short history deliberately evicts old fingerprints. A 65-node ring
	// therefore exercises the independent persistent page budget. Construct a
	// fresh Repository on every response to prove the cap survives restarts.
	const ringSize = 65
	ring := make([][]byte, ringSize)
	for index := range ring {
		ring[index] = integrationProviderCursor(t, fmt.Sprintf("task7-long-cycle-%03d", index), int64(2000+index))
	}
	base := ring[0]
	for step := 1; step <= ingress.MaxProviderCursorAdvances+1; step++ {
		next := ring[step%ringSize]
		restarted, restartErr := New(db)
		if restartErr != nil {
			t.Fatal(restartErr)
		}
		service, serviceErr := ingress.NewService(restarted)
		if serviceErr != nil {
			t.Fatal(serviceErr)
		}
		result, processErr := service.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: fmt.Sprintf("task7-long-cycle-response-%03d", step), Raw: []byte(fmt.Sprintf("task7-long-cycle-raw-%03d", step)),
			Projection: ingress.Projection{Cursor: next, CursorBase: base, CursorCandidate: next,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-long-cycle-child"},
		})
		if processErr != nil {
			t.Fatalf("long cycle step %d: %v", step, processErr)
		}
		if step <= ingress.MaxProviderCursorAdvances && result.Poisoned {
			t.Fatalf("legitimate under-cap cursor step %d poisoned: %+v", step, result)
		}
		if step == ingress.MaxProviderCursorAdvances+1 && !result.Poisoned {
			t.Fatalf("over-cap cursor step was not durably poisoned: %+v", result)
		}
		base = next
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_cursor_budgets WHERE tenant_id = $1 AND cursor_scope = $2 AND accepted_advances = $3 AND exhausted",
		[]any{string(tenantA), "task7-long-cycle-child", ingress.MaxProviderCursorAdvances}, 1)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND poison_reason = 'provider_cursor_budget_exhausted'",
		[]any{string(tenantA), string(connectionA)}, 1)
	for step := 1; step <= 300; step++ {
		next := integrationProviderCursor(t, fmt.Sprintf("task7-post-budget-%03d", step), int64(5000+step))
		result, processErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: fmt.Sprintf("task7-post-budget-response-%03d", step), Raw: []byte(fmt.Sprintf("task7-post-budget-raw-%03d", step)),
			Projection: ingress.Projection{Cursor: next, CursorBase: base, CursorCandidate: next,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-long-cycle-child"},
		})
		if step <= ingress.MaxRejectedProviderResponses {
			if processErr != nil || !result.Poisoned {
				t.Fatalf("sticky post-budget step %d = (%+v, %v)", step, result, processErr)
			}
		} else if !errors.Is(processErr, ingress.ErrProviderResponseCapacity) || !errors.Is(processErr, ingress.ErrConflictingEnvelope) {
			t.Fatalf("over-cap sticky response %d = (%+v, %v)", step, result, processErr)
		}
		base = next
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_cursor_history WHERE tenant_id = $1 AND cursor_scope = $2",
		[]any{string(tenantA), "task7-long-cycle-child"}, 64)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM gateway_events WHERE tenant_id = $1 AND aggregate_id LIKE 'task7-long-cycle-response-%' AND event_type = 'inbox.poisoned'",
		[]any{string(tenantA)}, 1)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id LIKE 'task7-post-budget-response-%'",
		[]any{string(tenantA), string(connectionA)}, 0)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_rejected_responses WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id LIKE 'task7-post-budget-response-%'",
		[]any{string(tenantA), string(connectionA)}, ingress.MaxRejectedProviderResponses)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_response_reservations WHERE tenant_id = $1 AND connection_id = $2 AND disposition = 'rejected'",
		[]any{string(tenantA), string(connectionA)}, ingress.MaxRejectedProviderResponses)
	seenRejected := make(map[string]struct{}, ingress.MaxRejectedProviderResponses)
	for page := 0; page < 16; page++ {
		restartedACKs, restartErr := New(db)
		if restartErr != nil {
			t.Fatal(restartErr)
		}
		pending, pendingErr := restartedACKs.ListPendingProviderACKsFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, 256)
		if pendingErr != nil {
			t.Fatal(pendingErr)
		}
		if len(pending) == 0 {
			break
		}
		for _, responseID := range pending {
			if strings.HasPrefix(responseID, "task7-post-budget-response-") {
				seenRejected[responseID] = struct{}{}
			}
		}
		if marked, markErr := restartedACKs.MarkProviderACKedFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, pending); markErr != nil || !marked {
			t.Fatalf("mark mixed raw/rejected ACK page = (%v, %v)", marked, markErr)
		}
	}
	if len(seenRejected) != ingress.MaxRejectedProviderResponses {
		t.Fatalf("restarted ACK refill saw %d/%d rejected response IDs", len(seenRejected), ingress.MaxRejectedProviderResponses)
	}
	exactRejected, exactRejectedErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task7-post-budget-response-001", Raw: []byte("task7-post-budget-raw-001"),
		Projection: ingress.Projection{Cursor: base, CursorBase: ring[0], CursorCandidate: base,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-long-cycle-child"},
	})
	if exactRejectedErr != nil || !exactRejected.Poisoned || !exactRejected.Duplicate {
		t.Fatalf("exact rejected redelivery = (%+v, %v)", exactRejected, exactRejectedErr)
	}
	pending, pendingErr := repository.ListPendingProviderACKsFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, 256)
	if pendingErr != nil || len(pending) != 1 || pending[0] != "task7-post-budget-response-001" {
		t.Fatalf("exact rejected redelivery ACK refill = (%v, %v)", pending, pendingErr)
	}
	// Different 4 MiB bytes for the same rejected identity never enter either
	// raw inbox or conflict payload storage. The first collision marks the one
	// authoritative reservation conflicted; restarts converge on that bounded
	// record without opening another storage family.
	largeConflict := bytes.Repeat([]byte{'x'}, ingress.MaxRawEnvelopeBytes)
	for attempt := 0; attempt < 8; attempt++ {
		restarted, restartErr := New(db)
		if restartErr != nil {
			t.Fatal(restartErr)
		}
		service, serviceErr := ingress.NewService(restarted)
		if serviceErr != nil {
			t.Fatal(serviceErr)
		}
		largeConflict[len(largeConflict)-1] = byte(attempt)
		_, conflictErr := service.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: "task7-post-budget-response-001", Raw: largeConflict,
			Projection: ingress.Projection{Cursor: base, CursorCandidate: base,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "task7-long-cycle-child"},
		})
		if !errors.Is(conflictErr, ingress.ErrConflictingEnvelope) {
			t.Fatalf("large rejected conflict %d error = %v", attempt, conflictErr)
		}
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{string(tenantA), string(connectionA), "task7-post-budget-response-001"}, 0)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_response_reservations WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3 AND conflicted",
		[]any{string(tenantA), string(connectionA), "task7-post-budget-response-001"}, 1)

	maxPoison := bytes.Repeat([]byte{'p'}, ingress.MaxRawEnvelopeBytes)
	for index := 0; index <= ingress.MaxPoisonedProviderInboxBytes/ingress.MaxRawEnvelopeBytes; index++ {
		maxPoison[len(maxPoison)-1] = byte(index)
		result, poisonErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantB, ConnectionID: connectionB, OwnerID: ownerB, FencingToken: leaseB.FencingToken,
			ProviderResponseID: fmt.Sprintf("task7-max-poison-%02d", index), Raw: maxPoison,
			DecodeError: errors.New("provider frame malformed"),
		})
		if index < ingress.MaxPoisonedProviderInboxBytes/ingress.MaxRawEnvelopeBytes {
			if poisonErr != nil || !result.Poisoned {
				t.Fatalf("max poison %d = (%+v, %v)", index, result, poisonErr)
			}
		} else if !errors.Is(poisonErr, ingress.ErrProviderResponseCapacity) {
			t.Fatalf("poison byte-cap response = (%+v, %v)", result, poisonErr)
		}
	}
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND poisoned",
		[]any{string(tenantB), string(connectionB)}, ingress.MaxPoisonedProviderInboxBytes/ingress.MaxRawEnvelopeBytes)
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT COALESCE(sum(octet_length(raw_envelope)), 0) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND poisoned",
		[]any{string(tenantB), string(connectionB)}, ingress.MaxPoisonedProviderInboxBytes)
	for attempt := 0; attempt < 4; attempt++ {
		maxPoison[len(maxPoison)-2] = byte(attempt + 1)
		_, conflictErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantB, ConnectionID: connectionB, OwnerID: ownerB, FencingToken: leaseB.FencingToken,
			ProviderResponseID: "task7-max-poison-00", Raw: maxPoison,
			DecodeError: errors.New("different malformed frame"),
		})
		if !errors.Is(conflictErr, ingress.ErrConflictingEnvelope) {
			t.Fatalf("bounded poison conflict %d = %v", attempt, conflictErr)
		}
	}
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3 AND octet_length(conflicting_raw_envelope) <= 256 AND conflicting_envelope_size = $4",
		[]any{string(tenantB), string(connectionB), "task7-max-poison-00", ingress.MaxRawEnvelopeBytes}, 1)
	// A previously valid raw inbox row that becomes conflicted after the poison
	// byte budget is full is atomically compacted into non-ACK rejected metadata.
	// Restarted processing cannot recreate either full raw copy.
	lateOriginal := bytes.Repeat([]byte{'n'}, ingress.MaxRawEnvelopeBytes)
	lateInserted, lateInsertErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantB, ConnectionID: connectionB, OwnerID: ownerB, FencingToken: leaseB.FencingToken,
		ProviderResponseID: "task7-late-conflict", Raw: lateOriginal,
	})
	if lateInsertErr != nil || !lateInserted.ACKEligible {
		t.Fatalf("seed late raw conflict = (%+v, %v)", lateInserted, lateInsertErr)
	}
	// Fill the bounded rejected family too. The conflict below must still
	// commit its authoritative reservation/ACK fence rather than rolling the
	// whole transaction back because neither optional evidence family has room.
	rejectedCapDigest := sha256.Sum256([]byte("task7-tenant-b-rejected-cap"))
	if err = inTenantExec(ctx, repository, tenantB, func(tx transaction) error {
		_, fillErr := tx.ExecContext(ctx, `WITH identities AS MATERIALIZED (
            SELECT 'task7-b-rejected-' || lpad(ordinal::text, 3, '0') AS provider_response_id
            FROM generate_series(0, $4 - 1) AS ordinal
        ), reservations AS (
            INSERT INTO provider_response_reservations (
                tenant_id, connection_id, provider_response_id, envelope_digest,
                disposition, conflicted, occurrence_count
            )
            SELECT $1, $2, identities.provider_response_id, $3, 'rejected', false, 1
            FROM identities
            RETURNING provider_response_id, envelope_digest
        )
        INSERT INTO provider_rejected_responses (
            tenant_id, connection_id, provider_response_id, envelope_digest, reason, ack_pending
        )
        SELECT $1, $2, reservations.provider_response_id, reservations.envelope_digest,
               'provider_cursor_budget_exhausted', false
        FROM reservations`, string(tenantB), string(connectionB), rejectedCapDigest[:], ingress.MaxRejectedProviderResponses)
		return fillErr
	}); err != nil {
		t.Fatalf("fill rejected evidence cap: %v", err)
	}
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_response_reservations WHERE tenant_id = $1 AND connection_id = $2 AND disposition = 'rejected'",
		[]any{string(tenantB), string(connectionB)}, ingress.MaxRejectedProviderResponses)
	lateConflict := bytes.Repeat([]byte{'q'}, ingress.MaxRawEnvelopeBytes)
	if _, conflictErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantB, ConnectionID: connectionB, OwnerID: ownerB, FencingToken: leaseB.FencingToken,
		ProviderResponseID: "task7-late-conflict", Raw: lateConflict,
	}); !errors.Is(conflictErr, ingress.ErrConflictingEnvelope) {
		t.Fatalf("late raw conflict at byte cap = %v", conflictErr)
	}
	released, releaseErr := repository.ReleaseConnectionLease(ctx, tenantB, connectionB, ownerB, leaseB.FencingToken)
	if releaseErr != nil || !released {
		t.Fatalf("release quarantined generation before explicit resume = %v, %v", released, releaseErr)
	}
	leaseB, acquired, err = repository.AcquireConnectionLease(ctx, tenantB, connectionB, ownerB, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("explicitly resume tenant B generation = %#v, %v, %v", leaseB, acquired, err)
	}
	lateRestartedRepository, lateRestartErr := New(db)
	if lateRestartErr != nil {
		t.Fatal(lateRestartErr)
	}
	restartedInbox, lateRestartErr := ingress.NewService(lateRestartedRepository)
	if lateRestartErr != nil {
		t.Fatal(lateRestartErr)
	}
	if _, replayErr := restartedInbox.Process(ctx, ingress.Envelope{
		TenantID: tenantB, ConnectionID: connectionB, OwnerID: ownerB, FencingToken: leaseB.FencingToken,
		ProviderResponseID: "task7-late-conflict", Raw: lateOriginal,
	}); !errors.Is(replayErr, ingress.ErrConflictingEnvelope) {
		t.Fatalf("restarted conflicted response replay = %v", replayErr)
	}
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{string(tenantB), string(connectionB), "task7-late-conflict"}, 1)
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{string(tenantB), string(connectionB), "task7-late-conflict"}, 1)
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT count(*) FROM provider_rejected_responses WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{string(tenantB), string(connectionB), "task7-late-conflict"}, 0)
	assertTenantCount(t, ctx, db, string(tenantB),
		`SELECT count(*) FROM provider_response_reservations AS reservation
         JOIN provider_inbox AS inbox USING (tenant_id, connection_id, provider_response_id)
         WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
           AND reservation.provider_response_id = $3 AND reservation.conflicted
           AND NOT inbox.ack_pending AND NOT inbox.poisoned`,
		[]any{string(tenantB), string(connectionB), "task7-late-conflict"}, 1)
	assertTenantCount(t, ctx, db, string(tenantB),
		"SELECT COALESCE(sum(octet_length(raw_envelope)), 0) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND poisoned",
		[]any{string(tenantB), string(connectionB)}, ingress.MaxPoisonedProviderInboxBytes)
	if err = repository.StageBackfillPageFenced(ctx, tenantB, connectionB, ownerB, leaseB.FencingToken, messaging.BackfillPage{Terminal: true}); err != nil {
		t.Fatalf("stage tenant B terminal backfill checkpoint: %v", err)
	}

	createMessage := func(tenant domain.TenantID, connection domain.ConnectionID, messageID, idempotencyKey string, offset int64) messaging.CreateResult {
		t.Helper()
		payloadDigest := sha256.Sum256([]byte(idempotencyKey))
		result, createErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
			IdempotencyKey: idempotencyKey, RequestDigest: messaging.CanonicalRequestDigest(messaging.SendInput{
				ConnectionID: connection, ConversationID: conversation, Text: "hello",
			}),
			CommandAudit: &messaging.CommandAudit{
				Topic: gatewaykafka.DefaultCommandsTopic, Partition: 0, Offset: offset,
				ProducerIdentity: "task7-producer", CorrelationID: "task7-correlation", PayloadDigest: payloadDigest,
			},
			Message: messaging.OutboundMessage{
				ID: domain.MessageID(messageID), TenantID: tenant, ConnectionID: connection, ConversationID: conversation,
				Text: "hello", ProviderTmpID: messaging.ProviderTemporaryID(tenant, domain.MessageID(messageID)),
				State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
			},
		})
		if createErr != nil || result.Outcome != messaging.CreateInserted {
			t.Fatalf("create Task 7 message %s = (%+v, %v)", messageID, result, createErr)
		}
		return result
	}
	createMessage(tenantA, connectionA, "task7-message-a1", "task7-idem-a1", 1)
	createMessage(tenantA, connectionA, "task7-message-a2", "task7-idem-a2", 2)
	createMessage(tenantB, connectionB, "task7-message-b1", "task7-idem-b1", 3)
	assertTenantCount(t, ctx, db, string(tenantA), "SELECT count(*) FROM message_status_history WHERE tenant_id = $1 AND message_id IN ($2, $3)", []any{string(tenantA), "task7-message-a1", "task7-message-a2"}, 2)
	assertTenantCount(t, ctx, db, string(tenantA), "SELECT count(*) FROM gateway_events WHERE tenant_id = $1 AND event_type = 'message.queued' AND aggregate_id IN ($2, $3)", []any{string(tenantA), "task7-message-a1", "task7-message-a2"}, 2)
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*) FROM event_outbox AS outbox
        JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
        WHERE outbox.tenant_id = $1 AND event.aggregate_id IN ($2, $3)`, []any{string(tenantA), "task7-message-a1", "task7-message-a2"}, 4)

	newChatInput := messaging.SendInput{ConnectionID: connectionA, Recipient: "+12025550188", RouteMode: messaging.RouteModePhoneDefault, Text: "new chat"}
	newChat, newChatErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
		IdempotencyKey: "task7-new-chat-lane", RequestDigest: messaging.CanonicalRequestDigest(newChatInput),
		Message: messaging.OutboundMessage{
			ID: "task7-new-chat-message", TenantID: tenantA, ConnectionID: connectionA,
			Recipient: newChatInput.Recipient, RouteMode: newChatInput.RouteMode, Text: newChatInput.Text,
			ProviderTmpID: "task7-new-chat-tmp", State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
		},
	})
	if newChatErr != nil || newChat.Outcome != messaging.CreateInserted {
		t.Fatalf("create new-chat canonical lane fixture = %+v, %v", newChat, newChatErr)
	}
	newChatLane := messaging.LaneKey{TenantID: tenantA, ConnectionID: connectionA, ConversationID: "new:" + newChatInput.Recipient}
	newChatClaim, claimedNewChat, claimNewChatErr := repository.ClaimNext(ctx, newChatLane, ownerA)
	if claimNewChatErr != nil || !claimedNewChat {
		t.Fatalf("claim new-chat canonical lane = %+v, %v, %v", newChatClaim, claimedNewChat, claimNewChatErr)
	}
	if owned, beginNewChatErr := repository.BeginProviderIO(ctx, newChatClaim, ownerA); beginNewChatErr != nil || !owned {
		t.Fatalf("begin new-chat provider I/O = %v, %v", owned, beginNewChatErr)
	}
	if recordErr := repository.RecordCreatedConversationFenced(ctx, tenantA, connectionA, newChatClaim.Message.ID,
		conversation, "outgoing-default", false, ownerA, newChatClaim.FencingToken,
	); !errors.Is(recordErr, messaging.ErrCanonicalLaneBusy) {
		t.Fatalf("preexisting queued lane was migrated under in-flight new chat: %v", recordErr)
	}
	var preservedOrderingKey string
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE(NULLIF(ordering_key, ''), conversation_id) FROM conversations
            WHERE tenant_id = $1 AND connection_id = $2 AND conversation_id = $3`,
			string(tenantA), string(connectionA), conversation).Scan(&preservedOrderingKey)
	}); err != nil || preservedOrderingKey != conversation {
		t.Fatalf("preexisting route ordering key = %q, %v", preservedOrderingKey, err)
	}

	// Hold the exact transaction advisory lock used by CreateOutbound and
	// observe the second session waiting in PostgreSQL before releasing it.
	// This is a causal serialization assertion rather than simultaneous-start
	// timing that might accidentally execute sequentially.
	blockedDSN, parseErr := url.Parse(appDSN)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	blockedQuery := blockedDSN.Query()
	blockedQuery.Set("application_name", "task7-idempotency-waiter")
	blockedDSN.RawQuery = blockedQuery.Encode()
	waiterDB, openErr := sql.Open("postgres", blockedDSN.String())
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer waiterDB.Close()
	waiterDB.SetMaxOpenConns(1)
	waiterRepository, repoErr := New(waiterDB)
	if repoErr != nil {
		t.Fatal(repoErr)
	}
	blocker, beginErr := db.BeginTx(ctx, nil)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	const causalKey = "task7-causal-idempotency"
	if _, lockErr := blocker.ExecContext(ctx, lockMessageIdempotencySQL, string(tenantA), causalKey); lockErr != nil {
		_ = blocker.Rollback()
		t.Fatal(lockErr)
	}
	causalInput := messaging.SendInput{ConnectionID: connectionA, ConversationID: conversation, Text: "causal-idempotency"}
	causalDigest := messaging.CanonicalRequestDigest(causalInput)
	type causalResult struct {
		result messaging.CreateResult
		err    error
	}
	causalDone := make(chan causalResult, 1)
	go func() {
		result, createErr := waiterRepository.CreateOutbound(ctx, messaging.CreateOutbound{
			IdempotencyKey: causalKey, RequestDigest: causalDigest,
			Message: messaging.OutboundMessage{
				ID: "task7-causal-message", TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
				Text: causalInput.Text, ProviderTmpID: "task7-causal-tmp", State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
			},
		})
		causalDone <- causalResult{result: result, err: createErr}
	}()
	lockCtx, cancelLock := context.WithTimeout(ctx, 5*time.Second)
	waitForPostgresLockWait(t, lockCtx, adminDB, "task7-idempotency-waiter")
	cancelLock()
	select {
	case early := <-causalDone:
		_ = blocker.Rollback()
		t.Fatalf("CreateOutbound escaped held idempotency lock: %+v, %v", early.result, early.err)
	default:
	}
	if commitErr := blocker.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}
	causal := <-causalDone
	if causal.err != nil || causal.result.Outcome != messaging.CreateInserted {
		t.Fatalf("causal first-use idempotency = (%+v, %v)", causal.result, causal.err)
	}
	causalDuplicate, duplicateErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
		IdempotencyKey: causalKey, RequestDigest: causalDigest,
		Message: messaging.OutboundMessage{
			ID: "task7-causal-duplicate", TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
			Text: causalInput.Text, ProviderTmpID: "task7-causal-duplicate-tmp", State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
		},
	})
	if duplicateErr != nil || causalDuplicate.Outcome != messaging.CreateDuplicate || causalDuplicate.Message.ID != causal.result.Message.ID {
		t.Fatalf("causal exact duplicate = (%+v, %v)", causalDuplicate, duplicateErr)
	}
	causalConflict, causalConflictErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
		IdempotencyKey: causalKey,
		RequestDigest:  messaging.CanonicalRequestDigest(messaging.SendInput{ConnectionID: connectionA, ConversationID: conversation, Text: "causal-conflict"}),
		Message: messaging.OutboundMessage{
			ID: "task7-causal-conflict", TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
			Text: "causal-conflict", ProviderTmpID: "task7-causal-conflict-tmp", State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
		},
	})
	if causalConflictErr != nil || causalConflict.Outcome != messaging.CreateConflict {
		t.Fatalf("causal conflicting duplicate = (%+v, %v)", causalConflict, causalConflictErr)
	}

	concurrentInput := messaging.SendInput{ConnectionID: connectionA, ConversationID: conversation, Text: "concurrent-idempotency"}
	concurrentDigest := messaging.CanonicalRequestDigest(concurrentInput)
	concurrentStart := make(chan struct{})
	concurrentResults := make(chan messaging.CreateResult, 2)
	concurrentErrors := make(chan error, 2)
	var concurrentWait sync.WaitGroup
	for index := range 2 {
		concurrentWait.Add(1)
		go func(index int) {
			defer concurrentWait.Done()
			<-concurrentStart
			messageID := domain.MessageID(fmt.Sprintf("task7-concurrent-idem-%d", index))
			result, createErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
				IdempotencyKey: "task7-concurrent-first-use", RequestDigest: concurrentDigest,
				Message: messaging.OutboundMessage{
					ID: messageID, TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
					Text: concurrentInput.Text, ProviderTmpID: messaging.ProviderTemporaryID(tenantA, messageID),
					State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
				},
			})
			if createErr != nil {
				concurrentErrors <- createErr
				return
			}
			concurrentResults <- result
		}(index)
	}
	close(concurrentStart)
	concurrentWait.Wait()
	close(concurrentResults)
	close(concurrentErrors)
	for concurrentErr := range concurrentErrors {
		t.Fatalf("concurrent first-use idempotency: %v", concurrentErr)
	}
	var canonicalMessageID domain.MessageID
	inserted, duplicate := 0, 0
	for result := range concurrentResults {
		switch result.Outcome {
		case messaging.CreateInserted:
			inserted++
		case messaging.CreateDuplicate:
			duplicate++
		default:
			t.Fatalf("concurrent first-use outcome = %v", result.Outcome)
		}
		if canonicalMessageID == "" {
			canonicalMessageID = result.Message.ID
		} else if result.Message.ID != canonicalMessageID {
			t.Fatalf("concurrent idempotency returned %q and %q", canonicalMessageID, result.Message.ID)
		}
	}
	if inserted != 1 || duplicate != 1 {
		t.Fatalf("concurrent idempotency outcomes inserted=%d duplicate=%d", inserted, duplicate)
	}
	conflictDigest := messaging.CanonicalRequestDigest(messaging.SendInput{ConnectionID: connectionA, ConversationID: conversation, Text: "different"})
	conflict, conflictErr := repository.CreateOutbound(ctx, messaging.CreateOutbound{
		IdempotencyKey: "task7-concurrent-first-use", RequestDigest: conflictDigest,
		Message: messaging.OutboundMessage{
			ID: "task7-concurrent-conflict", TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
			Text: "different", ProviderTmpID: "task7-concurrent-conflict", State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
		},
	})
	if conflictErr != nil || conflict.Outcome != messaging.CreateConflict {
		t.Fatalf("stable idempotency conflict = (%+v, %v)", conflict, conflictErr)
	}
	assertTenantCount(t, ctx, db, string(tenantA), "SELECT count(*) FROM messages WHERE tenant_id = $1 AND message_id = $2", []any{string(tenantA), string(canonicalMessageID)}, 1)

	rollbackDigest := messaging.CanonicalRequestDigest(messaging.SendInput{ConnectionID: connectionA, ConversationID: conversation, Text: "rollback", MediaIDs: []domain.MediaID{"missing-media"}})
	_, err = repository.CreateOutbound(ctx, messaging.CreateOutbound{
		IdempotencyKey: "task7-rollback", RequestDigest: rollbackDigest,
		Message: messaging.OutboundMessage{
			ID: "task7-message-rollback", TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation,
			Text: "rollback", MediaIDs: []domain.MediaID{"missing-media"}, ProviderTmpID: "task7-tmp-rollback",
			State: domain.MessageStateQueued, CreatedAt: time.Now().UTC(),
		},
	})
	if err == nil {
		t.Fatal("outbound UoW with missing media unexpectedly committed")
	}
	assertTenantCount(t, ctx, db, string(tenantA), "SELECT count(*) FROM messages WHERE tenant_id = $1 AND message_id = 'task7-message-rollback'", []any{string(tenantA)}, 0)

	processEnvelope := func(tenant domain.TenantID, connection domain.ConnectionID, owner string, token uint64, responseID string, raw []byte) (ingress.ProcessResult, error) {
		return inbox.Process(ctx, ingress.Envelope{
			TenantID: tenant, ConnectionID: connection, OwnerID: owner, FencingToken: token,
			ProviderResponseID: responseID, Raw: raw,
			Projection: ingress.Projection{
				Conversations: []ingress.ProjectedConversation{{ConversationID: conversation, DefaultOutgoingID: "outgoing-default"}},
				Messages:      []ingress.ProjectedMessage{{ProviderMessageID: responseID + "-message", ConversationID: conversation, Text: "inbound", State: domain.MessageStateDelivered}},
				Cursor:        integrationProviderCursor(t, "cursor-"+responseID, time.Now().UnixNano()), CursorSource: ingress.CursorSourceListMessages,
				CursorConversationID: conversation,
			},
		})
	}
	firstEnvelope, err := processEnvelope(tenantA, connectionA, ownerA, leaseA.FencingToken, "task7-response-a", []byte("task7-envelope-a"))
	if err != nil || !firstEnvelope.ACKEligible || firstEnvelope.Duplicate {
		t.Fatalf("first durable envelope = (%+v, %v)", firstEnvelope, err)
	}
	envelopeDuplicate, err := processEnvelope(tenantA, connectionA, ownerA, leaseA.FencingToken, "task7-response-a", []byte("task7-envelope-a"))
	if err != nil || !envelopeDuplicate.ACKEligible || !envelopeDuplicate.Duplicate {
		t.Fatalf("exact durable envelope duplicate = (%+v, %v)", envelopeDuplicate, err)
	}
	if _, err = processEnvelope(tenantA, connectionA, ownerA, leaseA.FencingToken, "task7-response-a", []byte("task7-envelope-conflict")); !errors.Is(err, ingress.ErrConflictingEnvelope) {
		t.Fatalf("conflicting durable envelope error = %v", err)
	}
	if _, err = processEnvelope(tenantB, connectionB, ownerB, leaseB.FencingToken, "task7-response-b", []byte("task7-envelope-b")); err != nil {
		t.Fatal(err)
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3",
		[]any{string(tenantA), string(connectionA), "task7-response-a"}, 1)

	mediaSecret := session.Envelope{
		Version: session.EnvelopeVersion, Provider: "gmessages-media", Ciphertext: bytes.Repeat([]byte{1}, 16),
		WrappedDEK: []byte{2}, Nonce: bytes.Repeat([]byte{3}, 12), KeyID: "task7-media-key", KeyVersion: 1,
	}
	twoImageEnvelope := ingress.Envelope{
		TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
		ProviderResponseID: "task7-response-two-images", Raw: []byte("task7-envelope-two-images"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "task7-two-image-message", ConversationID: conversation,
			Text: "two images", Transport: "mms", State: domain.MessageStateDelivered,
		}}},
		Media: []ingress.MediaLocator{
			{ProviderMessageID: "task7-two-image-message", Locator: "gmessages:first", Position: 0, MIMEType: "image/png", DeclaredSize: 7, DisplayFilename: "first.png", KeyEnvelope: mediaSecret, KeyDigest: sha256.Sum256([]byte("task7-media-key-material"))},
			{ProviderMessageID: "task7-two-image-message", Locator: "gmessages:second", Position: 1, MIMEType: "image/png", DeclaredSize: 8, DisplayFilename: "second.png", KeyEnvelope: mediaSecret, KeyDigest: sha256.Sum256([]byte("task7-media-key-material"))},
		},
	}
	twoImageResult, err := inbox.Process(ctx, twoImageEnvelope)
	if err != nil || !twoImageResult.ACKEligible || twoImageResult.Duplicate {
		t.Fatalf("two-image durable envelope = (%+v, %v)", twoImageResult, err)
	}
	twoImageDuplicate, err := inbox.Process(ctx, twoImageEnvelope)
	if err != nil || !twoImageDuplicate.ACKEligible || !twoImageDuplicate.Duplicate {
		t.Fatalf("two-image exact redelivery = (%+v, %v)", twoImageDuplicate, err)
	}
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*)
FROM message_media AS link
JOIN messages AS message ON message.tenant_id = link.tenant_id AND message.message_id = link.message_id
WHERE link.tenant_id = $1 AND message.provider_message_id = $2`, []any{string(tenantA), "task7-two-image-message"}, 2)

	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*)
FROM gateway_events AS event
JOIN media_objects AS media ON media.tenant_id = event.tenant_id AND media.media_id = event.aggregate_id
JOIN messages AS message ON message.tenant_id = media.tenant_id AND message.message_id = media.message_id
WHERE event.tenant_id = $1 AND event.event_type = 'media.pending' AND message.provider_message_id = $2`, []any{string(tenantA), "task7-two-image-message"}, 2)
	assertTenantCount(t, ctx, db, string(tenantA), `SELECT count(*)
FROM event_outbox AS outbox
JOIN gateway_events AS event ON event.tenant_id = outbox.tenant_id AND event.event_id = outbox.event_id
JOIN media_objects AS media ON media.tenant_id = event.tenant_id AND media.media_id = event.aggregate_id
JOIN messages AS message ON message.tenant_id = media.tenant_id AND message.message_id = media.message_id
WHERE outbox.tenant_id = $1 AND event.event_type = 'media.pending' AND message.provider_message_id = $2`, []any{string(tenantA), "task7-two-image-message"}, 4)

	receiptMessage, err := repository.GetMessage(ctx, tenantA, "task7-message-a1")
	if err != nil {
		t.Fatalf("load outbound for receipt projection: %v", err)
	}
	for index, state := range []domain.MessageState{
		domain.MessageStateSent,
		domain.MessageStateDelivered,
		domain.MessageStateRead,
		domain.MessageStateSent,
		domain.MessageStateFailed,
	} {
		result, receiptErr := inbox.Process(ctx, ingress.Envelope{
			TenantID: tenantA, ConnectionID: connectionA, OwnerID: ownerA, FencingToken: leaseA.FencingToken,
			ProviderResponseID: fmt.Sprintf("task7-receipt-%d", index), Raw: []byte(fmt.Sprintf("task7-receipt-body-%d", index)),
			Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
				ProviderMessageID: "task7-provider-message-a1", ProviderTmpID: receiptMessage.ProviderTmpID,
				ConversationID: conversation, Text: "hello", Transport: "sms", State: state,
			}}},
		})
		if receiptErr != nil || !result.ACKEligible {
			t.Fatalf("receipt projection %s = (%+v, %v)", state, result, receiptErr)
		}
	}
	reconciled, err := repository.GetMessage(ctx, tenantA, receiptMessage.ID)
	if err != nil || reconciled.State != domain.MessageStateRead || reconciled.ProviderMessageID != "task7-provider-message-a1" || reconciled.Transport != "sms" {
		t.Fatalf("non-regressing receipt reconciliation = %+v, %v", reconciled, err)
	}
	assertTenantCount(t, ctx, db, string(tenantA), "SELECT count(*) FROM message_status_history WHERE tenant_id = $1 AND message_id = $2 AND state = 'read'", []any{string(tenantA), string(receiptMessage.ID)}, 1)

	importClaim, claimedImport, err := repository.ClaimFetch(ctx, tenantA, "task7-media-importer")
	if err != nil || !claimedImport {
		t.Fatalf("claim media before import crash = (%+v, %v, %v)", importClaim, claimedImport, err)
	}
	importedBytes := []byte("imported")
	importedDigest := sha256.Sum256(importedBytes)
	importedRecord := media.Record{
		ID: importClaim.MediaID, TenantID: tenantA, ObjectKey: "objects/task7/imported", MIMEType: "image/png",
		Size: int64(len(importedBytes)), SHA256: importedDigest[:], Width: 1, Height: 1,
		DisplayFilename: importClaim.DisplayFilename, State: "ready", CreatedAt: time.Now().UTC(),
	}
	if err = repository.Create(ctx, importedRecord); err != nil {
		t.Fatalf("persist imported bytes metadata before simulated crash: %v", err)
	}
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		_, expireErr := tx.ExecContext(ctx, `UPDATE media_fetch_jobs
SET claim_expires_at = clock_timestamp() - interval '1 second'
WHERE tenant_id = $1 AND job_id = $2`, string(tenantA), importClaim.JobID)
		return expireErr
	}); err != nil {
		t.Fatalf("expire media claim after import crash: %v", err)
	}
	restartClaim, restarted, err := repository.ClaimFetch(ctx, tenantA, "task7-media-restart")
	if err != nil || !restarted || restartClaim.JobID != importClaim.JobID || restartClaim.Imported == nil ||
		restartClaim.Imported.Size != importedRecord.Size || !bytes.Equal(restartClaim.Imported.SHA256, importedRecord.SHA256) {
		t.Fatalf("media import crash recovery = (%+v, %v, %v)", restartClaim, restarted, err)
	}
	if renewed, renewErr := repository.RenewFetch(ctx, restartClaim); renewErr != nil || !renewed {
		t.Fatalf("renew slow media claim = %v, %v", renewed, renewErr)
	}
	if renewed, renewErr := repository.RenewFetch(ctx, importClaim); renewErr != nil || renewed {
		t.Fatalf("stale media claim renewal = %v, %v", renewed, renewErr)
	}
	if err = repository.CompleteReady(ctx, restartClaim, importedRecord, "task7-media-ready", []byte(`{"id":"task7-media-ready","type":"media.ready"}`)); err != nil {
		t.Fatalf("complete imported media after restart: %v", err)
	}
	if err = repository.CompleteFailed(ctx, importClaim, "stale worker", "task7-media-stale-failed", []byte(`{"id":"task7-media-stale-failed","type":"media.failed"}`), time.Time{}); !errors.Is(err, media.ErrFetchFenceLost) {
		t.Fatalf("stale media completion error = %v, want ErrFetchFenceLost", err)
	}
	readyAfterStale, err := repository.Get(ctx, tenantA, importClaim.MediaID)
	if err != nil || readyAfterStale.State != "ready" || !bytes.Equal(readyAfterStale.SHA256, importedRecord.SHA256) {
		t.Fatalf("stale worker downgraded ready media = %+v, %v", readyAfterStale, err)
	}

	readyDigest := sha256.Sum256([]byte("task7-media"))
	for _, record := range []media.Record{
		{ID: "task7-media-a", TenantID: tenantA, ObjectKey: "objects/task7-a/media", MIMEType: "image/png", Size: 7, SHA256: readyDigest[:], Width: 1, Height: 1, DisplayFilename: "a.png", State: "ready", CreatedAt: time.Now().UTC()},
		{ID: "task7-media-b", TenantID: tenantB, ObjectKey: "objects/task7-b/media", MIMEType: "image/png", Size: 7, SHA256: readyDigest[:], Width: 1, Height: 1, DisplayFilename: "b.png", State: "ready", CreatedAt: time.Now().UTC()},
	} {
		if err = repository.Create(ctx, record); err != nil {
			t.Fatalf("create Task 7 media metadata: %v", err)
		}
	}

	lane := messaging.LaneKey{TenantID: tenantA, ConnectionID: connectionA, ConversationID: conversation}
	type claimResult struct {
		claim messaging.DispatchClaim
		ok    bool
		err   error
	}

	// Hold the first row in the canonical dispatch lock order and prove that a
	// real ClaimNext session waits on that row before it can touch the lease,
	// lane, attempt, or message. Releasing the pre-I/O claim restores the same
	// message so the takeover/concurrency assertions below remain independent.
	dispatchDSN, parseDispatchErr := url.Parse(appDSN)
	if parseDispatchErr != nil {
		t.Fatal(parseDispatchErr)
	}
	dispatchQuery := dispatchDSN.Query()
	dispatchQuery.Set("application_name", "task7-dispatch-lock-waiter")
	dispatchDSN.RawQuery = dispatchQuery.Encode()
	dispatchDB, openDispatchErr := sql.Open("postgres", dispatchDSN.String())
	if openDispatchErr != nil {
		t.Fatal(openDispatchErr)
	}
	defer dispatchDB.Close()
	dispatchDB.SetMaxOpenConns(1)
	dispatchRepository, dispatchRepositoryErr := New(dispatchDB)
	if dispatchRepositoryErr != nil {
		t.Fatal(dispatchRepositoryErr)
	}
	dispatchBlocker, beginDispatchErr := db.BeginTx(ctx, nil)
	if beginDispatchErr != nil {
		t.Fatal(beginDispatchErr)
	}
	if _, err = dispatchBlocker.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", string(tenantA)); err != nil {
		_ = dispatchBlocker.Rollback()
		t.Fatal(err)
	}
	var lockedConnection string
	if err = dispatchBlocker.QueryRowContext(ctx, `SELECT connection_id FROM connections
WHERE tenant_id = $1 AND connection_id = $2 FOR UPDATE`, string(tenantA), string(connectionA)).Scan(&lockedConnection); err != nil {
		_ = dispatchBlocker.Rollback()
		t.Fatal(err)
	}
	dispatchDone := make(chan claimResult, 1)
	go func() {
		claim, ok, claimErr := dispatchRepository.ClaimNext(ctx, lane, ownerA)
		dispatchDone <- claimResult{claim: claim, ok: ok, err: claimErr}
	}()
	dispatchLockCtx, cancelDispatchLock := context.WithTimeout(ctx, 5*time.Second)
	waitForPostgresLockWait(t, dispatchLockCtx, adminDB, "task7-dispatch-lock-waiter")
	cancelDispatchLock()
	select {
	case early := <-dispatchDone:
		_ = dispatchBlocker.Rollback()
		t.Fatalf("ClaimNext escaped held connection lock: %+v", early)
	default:
	}
	if err = dispatchBlocker.Commit(); err != nil {
		t.Fatal(err)
	}
	dispatchClaim := <-dispatchDone
	if dispatchClaim.err != nil || !dispatchClaim.ok {
		t.Fatalf("causal dispatch claim after connection unlock = %+v", dispatchClaim)
	}
	if err = repository.ReleaseBeforeDispatch(ctx, dispatchClaim.claim, "causal_lock_order_probe"); err != nil {
		t.Fatalf("release causal dispatch claim: %v", err)
	}

	claims := make(chan claimResult, 2)
	var claimWait sync.WaitGroup
	for range 2 {
		claimWait.Add(1)
		go func() {
			defer claimWait.Done()
			claim, ok, claimErr := repository.ClaimNext(ctx, lane, ownerA)
			claims <- claimResult{claim: claim, ok: ok, err: claimErr}
		}()
	}
	claimWait.Wait()
	close(claims)
	var claimed messaging.DispatchClaim
	claimCount := 0
	for result := range claims {
		if result.err != nil {
			t.Fatalf("concurrent lane claim: %v", result.err)
		}
		if result.ok {
			claimCount++
			claimed = result.claim
		}
	}
	if claimCount != 1 {
		t.Fatalf("concurrent lane claims = %d, want 1", claimCount)
	}
	if owned, beginErr := repository.BeginProviderIO(ctx, claimed, ownerA); beginErr != nil || !owned {
		t.Fatalf("begin provider I/O = %v, %v", owned, beginErr)
	}
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		_, updateErr := tx.ExecContext(ctx, `UPDATE message_lanes
SET claim_expires_at = clock_timestamp() - interval '1 second'
WHERE tenant_id = $1 AND connection_id = $2 AND ordering_key = $3 AND lane_token = $4`,
			string(tenantA), string(connectionA), conversation, claimed.LaneToken,
		)
		return updateErr
	}); err != nil {
		t.Fatalf("simulate dispatch crash after provider I/O: %v", err)
	}
	if recoveryClaim, recoveryOK, recoveryErr := repository.ClaimNext(ctx, lane, ownerA); recoveryErr != nil || recoveryOK {
		t.Fatalf("stale dispatch recovery = (%+v, %v, %v), want recovery-only transaction", recoveryClaim, recoveryOK, recoveryErr)
	}
	uncertain, err := repository.GetMessage(ctx, tenantA, claimed.Message.ID)
	if err != nil || uncertain.State != domain.MessageStateUncertain {
		t.Fatalf("uncertain message after stale-claim restart recovery = %+v, %v", uncertain, err)
	}

	secret := session.Envelope{Version: 1, Provider: "webhook", Ciphertext: bytes.Repeat([]byte{1}, 16), WrappedDEK: []byte{2}, Nonce: bytes.Repeat([]byte{3}, 12), KeyID: "task7-key", KeyVersion: 1}
	for _, endpoint := range []webhook.EndpointRecord{
		{Endpoint: webhook.Endpoint{ID: "task7-endpoint-a", TenantID: tenantA, Destination: "https://example.com/a", KeyID: "task7-signing-a", Active: true, CreatedAt: time.Now().UTC()}, Secret: secret},
		{Endpoint: webhook.Endpoint{ID: "task7-endpoint-b", TenantID: tenantB, Destination: "https://example.com/b", KeyID: "task7-signing-b", Active: true, CreatedAt: time.Now().UTC()}, Secret: secret},
	} {
		if err = repository.CreateEndpoint(ctx, endpoint, webhook.DefaultMaxEndpointsPerTenant); err != nil {
			t.Fatalf("create Task 7 webhook endpoint: %v", err)
		}
	}
	const webhookEventID = "task7-webhook-delivery"
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		// Earlier Task 7 sections intentionally create events before webhook
		// endpoints exist. Close those historical webhook outbox rows so this
		// delivery/DLQ fixture exercises only its explicitly seeded event.
		if _, updateErr := tx.ExecContext(ctx, `UPDATE event_outbox
            SET published_at = clock_timestamp()
            WHERE tenant_id = $1 AND destination = 'webhook' AND published_at IS NULL`,
			string(tenantA)); updateErr != nil {
			return updateErr
		}
		if _, insertErr := tx.ExecContext(ctx, `INSERT INTO gateway_events
            (tenant_id, event_id, event_type, aggregate_type, aggregate_id, connection_id, canonical_body)
            VALUES ($1, $2, 'task7.webhook', 'test', $2, $3, '{}'::bytea)`,
			string(tenantA), webhookEventID, string(connectionA)); insertErr != nil {
			return insertErr
		}
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO event_outbox
            (tenant_id, outbox_id, event_id, destination)
            VALUES ($1, $2, $3, 'webhook')`,
			string(tenantA), webhookEventID+":webhook", webhookEventID)
		return insertErr
	}); err != nil {
		t.Fatalf("create Task 7 webhook event: %v", err)
	}
	deliveries, err := repository.Claim(ctx, tenantA, "task7-webhook-a", 1)
	if err != nil || len(deliveries) != 1 || deliveries[0].EventID != webhookEventID {
		t.Fatalf("claim Task 7 webhook = %+v, %v", deliveries, err)
	}
	if err = repository.CompleteAttempt(ctx, webhook.AttemptResult{
		TenantID: tenantA, DeliveryID: deliveries[0].DeliveryID, Attempt: deliveries[0].Attempt,
		OwnerID: deliveries[0].OwnerID, ClaimToken: deliveries[0].ClaimToken,
		Dead: true, SafeError: "integration exhausted",
	}); err != nil {
		t.Fatalf("dead-letter Task 7 webhook: %v", err)
	}
	var webhookDLQID string
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		return tx.QueryRowContext(ctx, "SELECT dlq_id FROM webhook_dlq WHERE tenant_id = $1 AND delivery_id = $2", string(tenantA), deliveries[0].DeliveryID).Scan(&webhookDLQID)
	}); err != nil {
		t.Fatalf("read Task 7 webhook DLQ: %v", err)
	}
	if err = repository.ReplayDLQ(ctx, tenantA, webhookDLQID); err != nil {
		t.Fatalf("replay Task 7 webhook DLQ: %v", err)
	}
	replayedWebhook, err := repository.Claim(ctx, tenantA, "task7-webhook-replay", 1)
	if err != nil || len(replayedWebhook) != 1 || replayedWebhook[0].DeliveryID != deliveries[0].DeliveryID {
		t.Fatalf("replayed Task 7 webhook = %+v, %v", replayedWebhook, err)
	}
	if _, err = repository.DeleteEndpoint(ctx, tenantA, replayedWebhook[0].EndpointID); err != nil {
		t.Fatalf("delete endpoint with replay delivery leased: %v", err)
	}
	if err = repository.CompleteAttempt(ctx, webhook.AttemptResult{
		TenantID: tenantA, DeliveryID: replayedWebhook[0].DeliveryID, Attempt: replayedWebhook[0].Attempt,
		OwnerID: replayedWebhook[0].OwnerID, ClaimToken: replayedWebhook[0].ClaimToken,
		StatusCode: 204, Succeeded: true,
	}); err == nil {
		t.Fatal("deleted endpoint accepted stale in-flight webhook completion")
	}
	if err = repository.ReplayDLQ(ctx, tenantA, webhookDLQID); err == nil {
		t.Fatal("deleted endpoint accepted DLQ replay")
	}

	// Fan-out is deliberately smaller than the active endpoint set. Repeated
	// claims must progressively materialize every pair before publishing the
	// immutable outbox row. The final two concurrent creates also prove the
	// tenant quota is serialized by PostgreSQL, not a process-local count.
	fanoutSecret := session.Envelope{Version: 1, Provider: "webhook", Ciphertext: make([]byte, 16), WrappedDEK: []byte{2}, Nonce: make([]byte, 12), KeyID: "task7-key", KeyVersion: 1}
	for index := range 5 {
		endpointID := fmt.Sprintf("task7-fanout-%02d", index)
		if err = repository.CreateEndpoint(ctx, webhook.EndpointRecord{
			Endpoint: webhook.Endpoint{ID: endpointID, TenantID: tenantA, Destination: fmt.Sprintf("https://example.com/fanout/%d", index), KeyID: endpointID + "-key", Active: true, CreatedAt: time.Now().UTC()},
			Secret:   fanoutSecret,
		}, 6); err != nil {
			t.Fatalf("create fan-out endpoint %d: %v", index, err)
		}
	}
	const fanoutEventID = "task7-webhook-fanout"
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		if _, insertErr := tx.ExecContext(ctx, `INSERT INTO gateway_events
            (tenant_id, event_id, event_type, aggregate_type, aggregate_id, connection_id, canonical_body, created_at)
            VALUES ($1, $2, 'task7.fanout', 'test', $2, $3, '{}'::bytea, '2000-01-01T00:00:00Z')`,
			string(tenantA), fanoutEventID, string(connectionA)); insertErr != nil {
			return insertErr
		}
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO event_outbox
            (tenant_id, outbox_id, event_id, destination, available_at)
            VALUES ($1, $2, $2, 'webhook', '2000-01-01T00:00:00Z')`, string(tenantA), fanoutEventID)
		return insertErr
	}); err != nil {
		t.Fatalf("seed webhook fan-out event: %v", err)
	}
	for page := range 3 {
		claimedPage, claimErr := repository.Claim(ctx, tenantA, fmt.Sprintf("task7-fanout-worker-%d", page), 2)
		if claimErr != nil {
			t.Fatalf("claim webhook fan-out page %d: %v", page, claimErr)
		}
		for _, claimedDelivery := range claimedPage {
			if claimedDelivery.EventID != fanoutEventID {
				continue
			}
			if completeErr := repository.CompleteAttempt(ctx, webhook.AttemptResult{
				TenantID: tenantA, DeliveryID: claimedDelivery.DeliveryID, Attempt: claimedDelivery.Attempt,
				OwnerID: claimedDelivery.OwnerID, ClaimToken: claimedDelivery.ClaimToken,
				StatusCode: 204, Succeeded: true,
			}); completeErr != nil {
				t.Fatalf("complete webhook fan-out delivery: %v", completeErr)
			}
		}
	}
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM webhook_deliveries WHERE tenant_id = $1 AND event_id = $2",
		[]any{string(tenantA), fanoutEventID}, 5)
	assertTenantCount(t, ctx, db, string(tenantA),
		"SELECT count(*) FROM event_outbox WHERE tenant_id = $1 AND event_id = $2 AND published_at IS NOT NULL",
		[]any{string(tenantA), fanoutEventID}, 1)
	quotaStart := make(chan struct{})
	quotaResults := make(chan error, 2)
	for index := range 2 {
		go func(index int) {
			<-quotaStart
			endpointID := fmt.Sprintf("task7-quota-race-%d", index)
			quotaResults <- repository.CreateEndpoint(ctx, webhook.EndpointRecord{
				Endpoint: webhook.Endpoint{ID: endpointID, TenantID: tenantA, Destination: fmt.Sprintf("https://example.com/quota/%d", index), KeyID: endpointID + "-key", Active: true, CreatedAt: time.Now().UTC()},
				Secret:   fanoutSecret,
			}, 6)
		}(index)
	}
	close(quotaStart)
	quotaSuccess, quotaRejected := 0, 0
	for range 2 {
		quotaErr := <-quotaResults
		switch {
		case quotaErr == nil:
			quotaSuccess++
		case errors.Is(quotaErr, webhook.ErrEndpointQuotaExceeded):
			quotaRejected++
		default:
			t.Fatalf("concurrent endpoint quota error: %v", quotaErr)
		}
	}
	if quotaSuccess != 1 || quotaRejected != 1 {
		t.Fatalf("concurrent endpoint quota success=%d rejected=%d", quotaSuccess, quotaRejected)
	}

	kafkaFirst, err := repository.ClaimEvents(ctx, tenantA, "task7-kafka-a", 256)
	if err != nil || len(kafkaFirst) == 0 {
		t.Fatalf("first Task 7 Kafka claim = %+v, %v", kafkaFirst, err)
	}
	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		_, expireErr := tx.ExecContext(ctx, `UPDATE event_outbox SET claim_expires_at = clock_timestamp() - interval '1 second'
            WHERE tenant_id = $1 AND destination = 'kafka' AND published_at IS NULL`, string(tenantA))
		return expireErr
	}); err != nil {
		t.Fatalf("expire Task 7 Kafka publication claim: %v", err)
	}
	kafkaReplay, err := repository.ClaimEvents(ctx, tenantA, "task7-kafka-replay", 256)
	if err != nil || !containsKafkaEvent(kafkaReplay, kafkaFirst[0].EventID) {
		t.Fatalf("Task 7 Kafka acknowledgment-crash replay missing %q: %+v, %v", kafkaFirst[0].EventID, kafkaReplay, err)
	}
	if _, err = repository.ClaimEvents(ctx, tenantB, "task7-kafka-b", 256); err != nil {
		t.Fatalf("materialize tenant B Kafka event: %v", err)
	}

	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		_, expireErr := tx.ExecContext(ctx, `UPDATE connection_leases SET expires_at = clock_timestamp() - interval '1 second'
            WHERE tenant_id = $1 AND connection_id = $2`, string(tenantA), string(connectionA))
		return expireErr
	}); err != nil {
		t.Fatalf("expire Task 7 inbox lease: %v", err)
	}
	if _, acquired, err = repository.AcquireConnectionLease(ctx, tenantA, connectionA, "task7-takeover", time.Minute); err != nil || !acquired {
		t.Fatalf("take over Task 7 inbox lease = %v, %v", acquired, err)
	}
	if acked, ackErr := repository.MarkProviderACKedFenced(ctx, tenantA, connectionA, ownerA, leaseA.FencingToken, []string{"task7-response-a"}); ackErr != nil || acked {
		t.Fatalf("stale Task 7 ACK mark = %v, %v", acked, ackErr)
	}

	if err = inTenantExec(ctx, repository, tenantA, func(tx transaction) error {
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO connections
            (tenant_id, connection_id, name, state, provider_device_fingerprint)
            SELECT $1, 'task7-quota-' || lpad(value::text, 3, '0'), 'quota fixture', 'connected',
                   decode(lpad(to_hex(value), 64, '0'), 'hex')
            FROM generate_series(1, 126) AS value`, string(tenantA))
		return insertErr
	}); err != nil {
		t.Fatalf("seed connection quota boundary: %v", err)
	}
	connectionQuotaStart := make(chan struct{})
	connectionQuotaResults := make(chan error, 2)
	for index := range 2 {
		go func(index int) {
			<-connectionQuotaStart
			connectionQuotaResults <- repository.SaveConnection(ctx, tenantA, ConnectionRecord{
				Connection: domain.Connection{
					ID: domain.ConnectionID(fmt.Sprintf("task7-quota-race-%d", index)), TenantID: tenantA,
					Name: "quota race", State: domain.ConnectionStateConnected,
				},
				ProviderDeviceFingerprint: bytes.Repeat([]byte{byte(index + 10)}, sha256.Size),
			})
		}(index)
	}
	close(connectionQuotaStart)
	connectionQuotaSuccess, connectionQuotaRejected := 0, 0
	for range 2 {
		quotaErr := <-connectionQuotaResults
		switch {
		case quotaErr == nil:
			connectionQuotaSuccess++
		case errors.Is(quotaErr, ErrConnectionQuotaExceeded):
			connectionQuotaRejected++
		default:
			t.Fatalf("concurrent connection quota error: %v", quotaErr)
		}
	}
	if connectionQuotaSuccess != 1 || connectionQuotaRejected != 1 {
		t.Fatalf("concurrent connection quota success=%d rejected=%d", connectionQuotaSuccess, connectionQuotaRejected)
	}
	if err = inTenantExec(ctx, repository, tenantB, func(tx transaction) error {
		if _, insertErr := tx.ExecContext(ctx, `INSERT INTO provider_response_id_quarantine
            (tenant_id, quarantine_id, connection_id, data_family, response_id_fingerprint,
             response_id_octets, reason)
            VALUES ($1, 'task7-quarantine-b', $2, 'cursor_budget', $3, 257, 'invalid_provider_response_id')`,
			string(tenantB), string(connectionB), strings.Repeat("f", 32)); insertErr != nil {
			return insertErr
		}
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO provider_response_overflow_audits
            (tenant_id, connection_id, overflow_poison_rows, overflow_poison_bytes, overflow_rejected_rows)
            VALUES ($1, $2, 1, 1024, 0)`, string(tenantB), string(connectionB))
		return insertErr
	}); err != nil {
		t.Fatalf("seed bounded response audit families: %v", err)
	}

	families := []struct {
		table     string
		predicate string
		identity  string
	}{
		{"provider_inbox", "provider_response_id", "task7-response-b"},
		{"messages", "message_id", "task7-message-b1"},
		{"media_objects", "media_id", "task7-media-b"},
		{"gateway_events", "event_type", "message.queued"},
		{"webhook_endpoints", "endpoint_id", "task7-endpoint-b"},
		{"kafka_commands", "idempotency_key", "task7-idem-b1"},
		{"provider_backfill_checkpoints", "connection_id", string(connectionB)},
		{"provider_cursor_history", "cursor_scope", conversation},
		{"provider_cursor_budgets", "cursor_scope", conversation},
		{"provider_response_reservations", "provider_response_id", "task7-b-rejected-000"},
		{"provider_response_overflow_audits", "connection_id", string(connectionB)},
		{"provider_rejected_responses", "provider_response_id", "task7-b-rejected-000"},
		{"provider_response_id_quarantine", "quarantine_id", "task7-quarantine-b"},
	}
	for _, family := range families {
		table := family.table
		assertTenantCount(t, ctx, db, string(tenantB), "SELECT count(*) FROM "+table+" WHERE tenant_id = $1 AND "+family.predicate+" = $2", []any{string(tenantB), family.identity}, 1)
		tx, beginErr := db.BeginTx(ctx, nil)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if _, beginErr = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", string(tenantA)); beginErr != nil {
			tx.Rollback()
			t.Fatal(beginErr)
		}
		var crossTenant int
		beginErr = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE tenant_id = $1", string(tenantB)).Scan(&crossTenant)
		_ = tx.Rollback()
		if beginErr != nil || crossTenant != 0 {
			t.Fatalf("tenant A read tenant B %s rows = %d, %v", table, crossTenant, beginErr)
		}
	}
}

func exerciseProviderACKLinearization(
	t *testing.T,
	ctx context.Context,
	repository *Repository,
	db *sql.DB,
	inbox *ingress.Service,
	tenantID domain.TenantID,
	connectionID domain.ConnectionID,
	ownerID string,
	fencingToken uint64,
) {
	t.Helper()
	other, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	otherInbox, err := ingress.NewService(other)
	if err != nil {
		t.Fatal(err)
	}
	process := func(service *ingress.Service, responseID string, raw []byte) (ingress.ProcessResult, error) {
		return service.Process(ctx, ingress.Envelope{
			TenantID: tenantID, ConnectionID: connectionID, OwnerID: ownerID, FencingToken: fencingToken,
			ProviderResponseID: responseID, Raw: raw,
		})
	}

	if result, processErr := process(inbox, "task7-ack-conflict-first", []byte("ack-conflict-first-a")); processErr != nil || !result.ACKEligible {
		t.Fatalf("seed conflict-first ACK = (%+v, %v)", result, processErr)
	}
	if _, processErr := process(otherInbox, "task7-ack-conflict-first", []byte("ack-conflict-first-b")); !errors.Is(processErr, ingress.ErrConflictingEnvelope) {
		t.Fatalf("commit conflict before ACK admission = %v", processErr)
	}
	providerCalls := 0
	filtered, coordinateErr := repository.CoordinateProviderACKsFenced(ctx, tenantID, connectionID, ownerID, fencingToken,
		time.Minute, []string{"task7-ack-conflict-first"}, func(context.Context, []string) error { providerCalls++; return nil })
	if coordinateErr != nil || len(filtered.AdmittedIDs) != 0 || providerCalls != 0 {
		t.Fatalf("conflict-first ACK = (%+v, %v), provider calls=%d", filtered, coordinateErr, providerCalls)
	}

	if result, processErr := process(inbox, "task7-ack-transaction-first", []byte("ack-transaction-first-a")); processErr != nil || !result.ACKEligible {
		t.Fatalf("seed transaction-first ACK = (%+v, %v)", result, processErr)
	}
	type ackOutcome struct {
		result ingress.ACKCoordinationResult
		err    error
	}
	wireStarted := make(chan struct{})
	releaseWire := make(chan struct{})
	ackDone := make(chan ackOutcome, 1)
	go func() {
		result, coordinateErr := repository.CoordinateProviderACKsFenced(ctx, tenantID, connectionID, ownerID, fencingToken,
			time.Minute, []string{"task7-ack-transaction-first"}, func(context.Context, []string) error {
				close(wireStarted)
				<-releaseWire
				return nil
			})
		ackDone <- ackOutcome{result: result, err: coordinateErr}
	}()
	<-wireStarted
	conflictDone := make(chan error, 1)
	go func() {
		_, processErr := process(otherInbox, "task7-ack-transaction-first", []byte("ack-transaction-first-b"))
		conflictDone <- processErr
	}()
	takeoverDone := make(chan error, 1)
	go func() {
		_, _, acquireErr := other.AcquireConnectionLease(ctx, tenantID, connectionID, "task7-ack-takeover", time.Minute)
		takeoverDone <- acquireErr
	}()
	select {
	case conflictErr := <-conflictDone:
		t.Fatalf("conflict committed while provider ACK transaction held locks: %v", conflictErr)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case takeoverErr := <-takeoverDone:
		t.Fatalf("lease takeover completed while provider ACK transaction held locks: %v", takeoverErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseWire)
	if outcome := <-ackDone; outcome.err != nil || outcome.result.ProviderError != nil ||
		!reflect.DeepEqual(outcome.result.AdmittedIDs, []string{"task7-ack-transaction-first"}) {
		t.Fatalf("transaction-first coordinated ACK = (%+v, %v)", outcome.result, outcome.err)
	}
	if conflictErr := <-conflictDone; !errors.Is(conflictErr, ingress.ErrConflictingEnvelope) {
		t.Fatalf("post-ACK conflict = %v", conflictErr)
	}
	if takeoverErr := <-takeoverDone; takeoverErr != nil {
		t.Fatalf("post-ACK takeover attempt = %v", takeoverErr)
	}
	assertTenantCount(t, ctx, db, string(tenantID),
		`SELECT count(*) FROM provider_response_reservations AS reservation
         JOIN provider_inbox AS inbox USING (tenant_id, connection_id, provider_response_id)
         WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
           AND reservation.provider_response_id = $3 AND reservation.conflicted AND NOT inbox.ack_pending`,
		[]any{string(tenantID), string(connectionID), "task7-ack-transaction-first"}, 1)

	if result, processErr := process(inbox, "task7-ack-post-wire-rollback", []byte("ack-post-wire-rollback-a")); processErr != nil || !result.ACKEligible {
		t.Fatalf("seed post-wire rollback ACK = (%+v, %v)", result, processErr)
	}
	ackCtx, cancelACK := context.WithCancel(ctx)
	postWireCalls := 0
	rolledBack, coordinateErr := repository.CoordinateProviderACKsFenced(ackCtx, tenantID, connectionID, ownerID, fencingToken,
		time.Minute, []string{"task7-ack-post-wire-rollback"}, func(context.Context, []string) error {
			postWireCalls++
			cancelACK()
			return nil
		})
	if coordinateErr == nil || postWireCalls != 1 || !reflect.DeepEqual(rolledBack.AdmittedIDs, []string{"task7-ack-post-wire-rollback"}) {
		t.Fatalf("post-wire rollback = (%+v, %v), provider calls=%d", rolledBack, coordinateErr, postWireCalls)
	}
	assertTenantCount(t, ctx, db, string(tenantID),
		`SELECT count(*) FROM provider_response_reservations AS reservation
         JOIN provider_inbox AS inbox USING (tenant_id, connection_id, provider_response_id)
         WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
           AND reservation.provider_response_id = $3 AND NOT reservation.conflicted AND inbox.ack_pending`,
		[]any{string(tenantID), string(connectionID), "task7-ack-post-wire-rollback"}, 1)
	if _, processErr := process(otherInbox, "task7-ack-post-wire-rollback", []byte("ack-post-wire-rollback-b")); !errors.Is(processErr, ingress.ErrConflictingEnvelope) {
		t.Fatalf("conflict after post-wire rollback = %v", processErr)
	}
	restarted, restartErr := New(db)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	retry, retryErr := restarted.CoordinateProviderACKsFenced(ctx, tenantID, connectionID, ownerID, fencingToken,
		time.Minute, []string{"task7-ack-post-wire-rollback"}, func(context.Context, []string) error { postWireCalls++; return nil })
	if retryErr != nil || len(retry.AdmittedIDs) != 0 || postWireCalls != 1 {
		t.Fatalf("conflict suppressed ambiguous ACK retry = (%+v, %v), provider calls=%d", retry, retryErr, postWireCalls)
	}

	// Renew the exact owner/token lease inside ACK admission before provider
	// I/O. The original lease expires while the callback is blocked, but a
	// takeover on a second repository must wait for the ACK transaction and
	// still observe the renewed lease afterward.
	if err = inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, updateErr := tx.ExecContext(ctx, `UPDATE connection_leases
            SET expires_at = clock_timestamp() + interval '500 milliseconds'
            WHERE tenant_id = $1 AND connection_id = $2 AND owner_id = $3 AND fencing_token = $4`,
			string(tenantID), string(connectionID), ownerID, fencingToken)
		return updateErr
	}); err != nil {
		t.Fatalf("shorten ACK lease fixture: %v", err)
	}
	if result, processErr := process(inbox, "task7-ack-near-expiry", []byte("ack-near-expiry")); processErr != nil || !result.ACKEligible {
		t.Fatalf("seed near-expiry ACK = (%+v, %v)", result, processErr)
	}
	nearWireStarted := make(chan struct{})
	releaseNearWire := make(chan struct{})
	nearACKDone := make(chan ackOutcome, 1)
	go func() {
		result, coordinateErr := repository.CoordinateProviderACKsFenced(ctx, tenantID, connectionID, ownerID, fencingToken,
			time.Minute, []string{"task7-ack-near-expiry"}, func(context.Context, []string) error {
				close(nearWireStarted)
				<-releaseNearWire
				return nil
			})
		nearACKDone <- ackOutcome{result: result, err: coordinateErr}
	}()
	<-nearWireStarted
	time.Sleep(600 * time.Millisecond)
	type takeoverOutcome struct {
		acquired bool
		err      error
	}
	nearTakeoverDone := make(chan takeoverOutcome, 1)
	go func() {
		_, acquired, acquireErr := other.AcquireConnectionLease(ctx, tenantID, connectionID, "task7-near-expiry-takeover", time.Minute)
		nearTakeoverDone <- takeoverOutcome{acquired: acquired, err: acquireErr}
	}()
	select {
	case takeover := <-nearTakeoverDone:
		t.Fatalf("near-expiry takeover escaped ACK locks: %+v", takeover)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseNearWire)
	if outcome := <-nearACKDone; outcome.err != nil || outcome.result.ProviderError != nil ||
		!reflect.DeepEqual(outcome.result.AdmittedIDs, []string{"task7-ack-near-expiry"}) {
		t.Fatalf("near-expiry ACK = (%+v, %v)", outcome.result, outcome.err)
	}
	if takeover := <-nearTakeoverDone; takeover.err != nil || takeover.acquired {
		t.Fatalf("renewed ACK lease allowed takeover = %+v", takeover)
	}
	assertTenantCount(t, ctx, db, string(tenantID),
		`SELECT count(*) FROM connection_leases
         WHERE tenant_id = $1 AND connection_id = $2 AND owner_id = $3 AND fencing_token = $4
           AND expires_at > clock_timestamp() + interval '50 seconds'`,
		[]any{string(tenantID), string(connectionID), ownerID, fencingToken}, 1)
}

func containsKafkaEvent(events []gatewaykafka.OutboxEvent, eventID string) bool {
	for _, event := range events {
		if event.EventID == eventID {
			return true
		}
	}
	return false
}

func exerciseConnectionLeaseFencing(t *testing.T, ctx context.Context, first *Repository) {
	t.Helper()
	second := newRepository(first.db)
	leaseA, acquired, err := first.AcquireConnectionLease(ctx, "tenant-a", "connection-a", "owner-a", time.Minute)
	if err != nil || !acquired || leaseA.FencingToken == 0 {
		t.Fatalf("first lease = %#v, acquired=%v, error=%v", leaseA, acquired, err)
	}
	if _, acquired, err = second.AcquireConnectionLease(ctx, "tenant-a", "connection-a", "owner-b", time.Minute); err != nil || acquired {
		t.Fatalf("fresh lease stolen: acquired=%v, error=%v", acquired, err)
	}
	if err = inTenantExec(ctx, first, "tenant-a", func(tx transaction) error {
		_, updateErr := tx.ExecContext(ctx, `UPDATE connection_leases SET expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND connection_id = $2`, "tenant-a", "connection-a")
		return updateErr
	}); err != nil {
		t.Fatalf("expire lease with database time: %v", err)
	}
	leaseB, acquired, err := second.AcquireConnectionLease(ctx, "tenant-a", "connection-a", "owner-b", time.Minute)
	if err != nil || !acquired || leaseB.FencingToken <= leaseA.FencingToken {
		t.Fatalf("takeover lease = %#v, acquired=%v, error=%v", leaseB, acquired, err)
	}
	stale := ConnectionActorHealth{ActorState: "ready", ConnectionState: domain.ConnectionStateConnected, LastSafeReason: "none"}
	if written, err := first.WriteConnectionHealthFenced(ctx, "tenant-a", "connection-a", "owner-a", leaseA.FencingToken, stale); err != nil || written {
		t.Fatalf("stale health write = %v, %v", written, err)
	}
	envelope, err := second.LoadEncryptedSession(ctx, "tenant-a", "connection-a")
	if err != nil {
		t.Fatalf("load session for fencing test: %v", err)
	}
	if swapped, err := first.CompareAndSwapEncryptedSessionFenced(ctx, "tenant-a", "connection-a", "owner-a", leaseA.FencingToken, envelope.Revision, envelope); err != nil || swapped {
		t.Fatalf("stale session write = %v, %v", swapped, err)
	}
	if swapped, err := second.CompareAndSwapEncryptedSessionFenced(ctx, "tenant-a", "connection-a", "owner-b", leaseB.FencingToken, envelope.Revision, envelope); err != nil || !swapped {
		t.Fatalf("fresh fenced session write = %v, %v", swapped, err)
	}
	if renewed, err := first.RenewConnectionLease(ctx, "tenant-a", "connection-a", "owner-a", leaseA.FencingToken, time.Minute); err != nil || renewed {
		t.Fatalf("stale renew = %v, %v", renewed, err)
	}
	if released, err := first.ReleaseConnectionLease(ctx, "tenant-a", "connection-a", "owner-a", leaseA.FencingToken); err != nil || released {
		t.Fatalf("stale release = %v, %v", released, err)
	}
	fresh := ConnectionActorHealth{ActorState: "ready", ConnectionState: domain.ConnectionStateConnected, LastSafeReason: "none"}
	if written, err := second.WriteConnectionHealthFenced(ctx, "tenant-a", "connection-a", "owner-b", leaseB.FencingToken, fresh); err != nil || !written {
		t.Fatalf("fresh health write = %v, %v", written, err)
	}
	transition, err := second.MarkReauthorizationRequired(ctx, "tenant-a", "connection-a")
	if err != nil || !transition.Transitioned {
		t.Fatalf("control-plane reauthorization = %#v, %v", transition, err)
	}
	if renewed, err := second.RenewConnectionLease(ctx, "tenant-a", "connection-a", "owner-b", leaseB.FencingToken, time.Minute); err != nil || renewed {
		t.Fatalf("reauthorization left actor lease valid = %v, %v", renewed, err)
	}
	if written, err := second.WriteConnectionHealthFenced(ctx, "tenant-a", "connection-a", "owner-b", leaseB.FencingToken, fresh); err != nil || written {
		t.Fatalf("reauthorization accepted stale health = %v, %v", written, err)
	}
	envelope, err = second.LoadEncryptedSession(ctx, "tenant-a", "connection-a")
	if err != nil {
		t.Fatalf("reload session after reauthorization: %v", err)
	}
	if swapped, err := second.CompareAndSwapEncryptedSessionFenced(ctx, "tenant-a", "connection-a", "owner-b", leaseB.FencingToken, envelope.Revision, envelope); err != nil || swapped {
		t.Fatalf("reauthorization accepted stale session = %v, %v", swapped, err)
	}
	health, err := second.GetConnectionHealth(ctx, "tenant-a", "connection-a")
	if err != nil || health.ConnectionState != domain.ConnectionStateReauthorizationRequired || !health.RequiresReauthorization || health.ActorState != "" {
		t.Fatalf("reauthorization health = %#v, %v", health, err)
	}
}

func exerciseReauthorizationHealthLockOrder(t *testing.T, ctx context.Context, repository *Repository, appDB, adminDB *sql.DB, appDSN string) {
	t.Helper()
	const (
		tenantID     = domain.TenantID("tenant-lock-order")
		connectionID = domain.ConnectionID("connection-lock-order")
		ownerID      = "owner-lock-order"
	)
	seedTenantAndConnection(t, ctx, repository, tenantID, connectionID, byte('l'))
	envelope := session.Envelope{
		Version: 1, Provider: "gmessages", Ciphertext: bytes.Repeat([]byte{3}, 16), WrappedDEK: []byte{4},
		Nonce: bytes.Repeat([]byte{5}, 12), KeyID: "lock-order-key", KeyVersion: 1,
	}
	if err := repository.SaveEncryptedSession(ctx, tenantID, connectionID, envelope); err != nil {
		t.Fatalf("seed lock-order session: %v", err)
	}
	loaded, err := repository.LoadEncryptedSession(ctx, tenantID, connectionID)
	if err != nil {
		t.Fatalf("load lock-order session: %v", err)
	}
	lease, acquired, err := repository.AcquireConnectionLease(ctx, tenantID, connectionID, ownerID, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire lock-order lease = %#v, %v, %v", lease, acquired, err)
	}

	applicationName := "sirenaix_health_lock_" + uuid.NewString()
	parsedDSN, err := url.Parse(appDSN)
	if err != nil {
		t.Fatalf("parse health lock-order DSN: %v", err)
	}
	query := parsedDSN.Query()
	query.Set("application_name", applicationName)
	parsedDSN.RawQuery = query.Encode()
	healthDB, err := sql.Open("postgres", parsedDSN.String())
	if err != nil {
		t.Fatalf("open health lock-order database: %v", err)
	}
	healthDB.SetMaxOpenConns(1)
	healthDB.SetMaxIdleConns(1)
	defer healthDB.Close()
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = healthDB.PingContext(lockCtx); err != nil {
		t.Fatalf("ping health lock-order database: %v", err)
	}
	healthRepository, err := New(healthDB)
	if err != nil {
		t.Fatalf("create health lock-order repository: %v", err)
	}

	connectionLocked := make(chan struct{})
	releaseReauthorization := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseReauthorization)
		}
	}()
	type reauthorizationResult struct {
		transition pairing.AuthorizationTransition
		err        error
	}
	reauthorizationDone := make(chan reauthorizationResult, 1)
	go func() {
		transition, transitionErr := repository.markReauthorizationRequired(lockCtx, tenantID, connectionID, func() {
			close(connectionLocked)
			<-releaseReauthorization
		})
		reauthorizationDone <- reauthorizationResult{transition: transition, err: transitionErr}
	}()
	select {
	case <-connectionLocked:
	case <-lockCtx.Done():
		t.Fatalf("reauthorization did not acquire connection lock: %v", lockCtx.Err())
	}

	type healthWriteResult struct {
		written bool
		err     error
	}
	healthDone := make(chan healthWriteResult, 1)
	go func() {
		written, writeErr := healthRepository.WriteConnectionHealthFenced(lockCtx, tenantID, connectionID, ownerID, lease.FencingToken, ConnectionActorHealth{
			ActorState: "ready", ConnectionState: domain.ConnectionStateConnected, LastSafeReason: "none",
		})
		healthDone <- healthWriteResult{written: written, err: writeErr}
	}()
	waitForPostgresLockWait(t, lockCtx, adminDB, applicationName)
	close(releaseReauthorization)
	released = true

	var reauthorization reauthorizationResult
	select {
	case reauthorization = <-reauthorizationDone:
	case <-lockCtx.Done():
		t.Fatalf("reauthorization lock-order transaction timed out: %v", lockCtx.Err())
	}
	if reauthorization.err != nil || !reauthorization.transition.Transitioned {
		t.Fatalf("reauthorization lock-order result = %#v, %v", reauthorization.transition, reauthorization.err)
	}
	var healthWrite healthWriteResult
	select {
	case healthWrite = <-healthDone:
	case <-lockCtx.Done():
		t.Fatalf("health lock-order transaction timed out: %v", lockCtx.Err())
	}
	if healthWrite.err != nil || healthWrite.written {
		t.Fatalf("old actor health after reauthorization = %v, %v", healthWrite.written, healthWrite.err)
	}
	if swapped, swapErr := healthRepository.CompareAndSwapEncryptedSessionFenced(lockCtx, tenantID, connectionID, ownerID, lease.FencingToken, loaded.Revision, loaded); swapErr != nil || swapped {
		t.Fatalf("old actor session after reauthorization = %v, %v", swapped, swapErr)
	}
	reloaded, err := repository.LoadEncryptedSession(lockCtx, tenantID, connectionID)
	if err != nil || reloaded.Revision != loaded.Revision {
		t.Fatalf("session changed across stale actor write: before=%d after=%d error=%v", loaded.Revision, reloaded.Revision, err)
	}
	assertTenantCount(t, lockCtx, appDB, string(tenantID), "SELECT count(*) FROM connection_actor_health WHERE tenant_id = $1 AND connection_id = $2", []any{string(tenantID), string(connectionID)}, 0)
	health, err := repository.GetConnectionHealth(lockCtx, tenantID, connectionID)
	if err != nil || health.ConnectionState != domain.ConnectionStateReauthorizationRequired || !health.RequiresReauthorization || health.ActorState != "" {
		t.Fatalf("authoritative health after lock-order race = %#v, %v", health, err)
	}
}

func waitForPostgresLockWait(t *testing.T, ctx context.Context, adminDB *sql.DB, applicationName string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := adminDB.QueryRowContext(ctx, `SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE application_name = $1 AND state = 'active' AND wait_event_type = 'Lock'
        )`, applicationName).Scan(&waiting); err != nil {
			t.Fatalf("inspect health lock wait: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("health transaction never blocked behind the connection lock: %v", ctx.Err())
		}
	}
}

func exerciseActorTableTenantIsolation(t *testing.T, ctx context.Context, repository *Repository, db *sql.DB) {
	t.Helper()
	lease, acquired, err := repository.AcquireConnectionLease(ctx, "tenant-b", "connection-b", "owner-tenant-b", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("tenant B lease = %#v, %v, %v", lease, acquired, err)
	}
	health := ConnectionActorHealth{ActorState: "ready", ConnectionState: domain.ConnectionStateConnected, LastSafeReason: "none"}
	if written, err := repository.WriteConnectionHealthFenced(ctx, "tenant-b", "connection-b", "owner-tenant-b", lease.FencingToken, health); err != nil || !written {
		t.Fatalf("tenant B health = %v, %v", written, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin actor RLS check: %v", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "tenant-a"); err != nil {
		t.Fatalf("set actor RLS tenant: %v", err)
	}
	for _, table := range []string{"connection_leases", "connection_actor_health"} {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE tenant_id = 'tenant-b'").Scan(&count); err != nil {
			t.Fatalf("read %s through RLS: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("tenant A read %d tenant B rows from %s", count, table)
		}
		result, updateErr := tx.ExecContext(ctx, "UPDATE "+table+" SET updated_at = clock_timestamp() WHERE tenant_id = 'tenant-b'")
		if updateErr != nil {
			t.Fatalf("update %s through RLS: %v", table, updateErr)
		}
		updated, rowsErr := result.RowsAffected()
		if rowsErr != nil || updated != 0 {
			t.Fatalf("tenant A updated tenant B %s rows = %d, %v", table, updated, rowsErr)
		}
	}
}

func TestPostgresIntegrationUpgradesLegacyPairingRowsSafely(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	appDB := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, appDB, "0001_gateway.sql", "0002_pairing_sessions.sql")

	tx, err := appDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin legacy seed: %v", err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-tenant"); err != nil {
		t.Fatalf("set legacy tenant: %v", err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", "legacy-tenant", "Legacy"); err != nil {
		t.Fatalf("seed legacy tenant: %v", err)
	}
	legacyRows := []struct {
		id          string
		state       string
		fingerprint []byte
	}{
		{id: "legacy-pairing", state: "pairing", fingerprint: bytes.Repeat([]byte{'p'}, 16)},
		{id: "legacy-reauth", state: "reauthorization-required", fingerprint: bytes.Repeat([]byte{'r'}, 32)},
		{id: "legacy-invalid", state: "connected", fingerprint: bytes.Repeat([]byte{'i'}, 16)},
		{id: "legacy-valid", state: "disconnected", fingerprint: bytes.Repeat([]byte{'v'}, 32)},
	}
	for _, row := range legacyRows {
		if _, err = tx.ExecContext(ctx, `INSERT INTO connections
            (tenant_id, connection_id, name, state, provider_device_fingerprint)
            VALUES ($1, $2, $3, $4, $5)`, "legacy-tenant", row.id, row.id, row.state, row.fingerprint); err != nil {
			t.Fatalf("seed %s: %v", row.id, err)
		}
	}
	for _, id := range []string{"legacy-pairing", "legacy-invalid", "legacy-valid"} {
		if _, err = tx.ExecContext(ctx, `INSERT INTO connection_sessions
            (tenant_id, connection_id, ciphertext, wrapped_dek, nonce, key_id, key_version, envelope_version, provider)
            VALUES ($1, $2, $3, $4, $5, $6, 1, 1, 'gmessages')`,
			"legacy-tenant", id, []byte{1}, []byte{2}, []byte{3}, "legacy-key"); err != nil {
			t.Fatalf("seed session %s: %v", id, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit legacy seed: %v", err)
	}

	applyNamedMigrations(t, ctx, appDB, "0003_pairing_upgrade_support.sql", "0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql")

	tx, err = appDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin upgrade assertions: %v", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-tenant"); err != nil {
		t.Fatalf("set assertion tenant: %v", err)
	}
	for _, id := range []string{"legacy-pairing", "legacy-invalid"} {
		var state string
		var fingerprint []byte
		var attemptID, prior sql.NullString
		var started sql.NullTime
		if err = tx.QueryRowContext(ctx, `SELECT state, provider_device_fingerprint, pairing_attempt_id, pairing_prior_state, pairing_started_at
            FROM connections WHERE tenant_id = $1 AND connection_id = $2`, "legacy-tenant", id).
			Scan(&state, &fingerprint, &attemptID, &prior, &started); err != nil {
			t.Fatalf("read reconciled %s: %v", id, err)
		}
		if state != "unpaired" || fingerprint != nil || attemptID.Valid || prior.Valid || started.Valid {
			t.Fatalf("unsafe reconciled row %s: state=%s fingerprint=%x attempt=%#v prior=%#v started=%#v", id, state, fingerprint, attemptID, prior, started)
		}
		var sessionCount int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM connection_sessions WHERE tenant_id = $1 AND connection_id = $2", "legacy-tenant", id).Scan(&sessionCount); err != nil || sessionCount != 0 {
			t.Fatalf("unusable session %s count=%d error=%v", id, sessionCount, err)
		}
	}
	var eventID string
	if err = tx.QueryRowContext(ctx, "SELECT reauthorization_event_id FROM connections WHERE tenant_id = $1 AND connection_id = $2", "legacy-tenant", "legacy-reauth").Scan(&eventID); err != nil {
		t.Fatalf("read legacy reauthorization event: %v", err)
	}
	wantEventID := fmt.Sprintf("legacy-reauth-%x", md5.Sum([]byte("legacy-tenant\x1flegacy-reauth")))
	if eventID != wantEventID {
		t.Fatalf("legacy event ID = %q, want %q", eventID, wantEventID)
	}
	var revision int64
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM connection_sessions WHERE tenant_id = $1 AND connection_id = $2", "legacy-tenant", "legacy-valid").Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("backfilled revision = %d, %v", revision, err)
	}
	var invalidConstraints int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint
        WHERE conname IN ('connections_fingerprint_matches_state', 'connections_reauthorization_event_required',
                          'connections_pairing_metadata_required', 'connection_sessions_revision_positive')
          AND NOT convalidated`).Scan(&invalidConstraints); err != nil || invalidConstraints != 0 {
		t.Fatalf("unvalidated constraints = %d, %v", invalidConstraints, err)
	}
	var forcedTables int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('connections', 'connection_sessions')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 2 {
		t.Fatalf("RLS/FORCE not restored on migrated tables: count=%d error=%v", forcedTables, err)
	}
}

func TestPostgresIntegrationUpgradesSeeded0007ConversationThroughForcedRLS(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	appDB := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, appDB,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql", "0007_durable_messaging.sql",
	)
	tx, err := appDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-task7"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", "legacy-task7", "Legacy Task7"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint)
        VALUES ($1, $2, $3, 'connected', $4)`, "legacy-task7", "connection-a", "Connection", bytes.Repeat([]byte{'x'}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO conversations
        (tenant_id, connection_id, conversation_id, provider_default_outgoing_id, is_group)
        VALUES ($1, $2, $3, $4, false)`, "legacy-task7", "connection-a", "conversation-a", "outgoing-a"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	applyNamedMigrations(t, ctx, appDB, "0008_task7_review_hardening.sql")
	tx, err = appDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-task7"); err != nil {
		t.Fatal(err)
	}
	var orderingKey string
	if err = tx.QueryRowContext(ctx, `SELECT ordering_key FROM conversations
        WHERE tenant_id = $1 AND connection_id = $2 AND conversation_id = $3`, "legacy-task7", "connection-a", "conversation-a").Scan(&orderingKey); err != nil {
		t.Fatal(err)
	}
	if orderingKey != "conversation-a" {
		t.Fatalf("0008 ordering key = %q", orderingKey)
	}
	var forced bool
	if err = tx.QueryRowContext(ctx, "SELECT relforcerowsecurity FROM pg_class WHERE relname = 'conversations'").Scan(&forced); err != nil || !forced {
		t.Fatalf("conversations FORCE RLS restored = %v, %v", forced, err)
	}
}

func TestPostgresIntegrationFailed0008BackfillCannotLeaveForceRLSDisabled(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	appDB := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, appDB,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql", "0007_durable_messaging.sql",
	)
	if _, err := appDB.ExecContext(ctx, `CREATE FUNCTION task7_fail_backfill() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected task7 backfill failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := appDB.ExecContext(ctx, `CREATE TRIGGER task7_fail_backfill BEFORE UPDATE ON conversations
FOR EACH STATEMENT EXECUTE FUNCTION task7_fail_backfill()`); err != nil {
		t.Fatal(err)
	}
	contents, err := Migrations.ReadFile("migrations/0008_task7_review_hardening.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = appDB.ExecContext(ctx, string(contents)); err == nil {
		t.Fatal("injected 0008 backfill unexpectedly succeeded")
	}
	var forced bool
	if err = appDB.QueryRowContext(ctx, "SELECT relforcerowsecurity FROM pg_class WHERE relname = 'conversations'").Scan(&forced); err != nil || !forced {
		t.Fatalf("failed migration left FORCE RLS disabled: forced=%v error=%v", forced, err)
	}
}

func TestPostgresIntegrationFailed0013RemediationRollsBackAndRestoresForceRLS(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, db,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql",
		"0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql",
		"0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql",
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "rollback-0013"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", "rollback-0013", "Rollback 0013"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint)
        VALUES ($1, 'connection-bad', 'connection-bad', 'connected', $2)`, "rollback-0013", bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_leases
        (tenant_id, connection_id, owner_id, fencing_token, expires_at)
        VALUES ($1, 'connection-bad', 'owner-bad', 7, clock_timestamp() + interval '1 hour')`, "rollback-0013"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_actor_health
        (tenant_id, connection_id, fencing_token, actor_state, connection_state,
         reconnect_count, current_backoff_microseconds, last_safe_reason, requires_reauthorization)
        VALUES ($1, 'connection-bad', 7, 'ready', 'connected', 0, 0, 'none', false)`, "rollback-0013"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'legacy-invalid', 'connection-bad', $2, $3, 'legacy', 'owner-bad', 7, clock_timestamp())`,
		"rollback-0013", "legacy‮response", bytes.Repeat([]byte{2}, 32)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE FUNCTION task7_fail_response_remediation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected response remediation failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TRIGGER task7_fail_response_remediation BEFORE UPDATE ON connection_leases
FOR EACH STATEMENT EXECUTE FUNCTION task7_fail_response_remediation()`); err != nil {
		t.Fatal(err)
	}
	contents, err := Migrations.ReadFile("migrations/0013_task7_provider_response_id_boundary.sql")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, string(contents)); err == nil {
		t.Fatal("injected 0013 remediation unexpectedly succeeded")
	}
	if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
		t.Fatalf("rollback failed migration: %v", rollbackErr)
	}
	var quarantineTable sql.NullString
	if err = conn.QueryRowContext(ctx, "SELECT to_regclass('provider_response_id_quarantine')::text").Scan(&quarantineTable); err != nil {
		t.Fatal(err)
	}
	if quarantineTable.Valid {
		t.Fatalf("failed migration retained quarantine table %q", quarantineTable.String)
	}
	var forcedTables int
	if err = conn.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('connections', 'connection_leases', 'connection_actor_health',
                          'provider_inbox', 'provider_inbox_conflicts', 'provider_cursor_history', 'provider_cursor_budgets')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 7 {
		t.Fatalf("failed 0013 FORCE RLS tables = %d, %v", forcedTables, err)
	}
	checkTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer checkTx.Rollback()
	if _, err = checkTx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "rollback-0013"); err != nil {
		t.Fatal(err)
	}
	var invalidRows int
	if err = checkTx.QueryRowContext(ctx, "SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND provider_response_id = $2", "rollback-0013", "legacy‮response").Scan(&invalidRows); err != nil || invalidRows != 1 {
		t.Fatalf("failed migration changed legacy row count = %d, %v", invalidRows, err)
	}
}

func TestPostgresIntegration0013QuarantinesLegacyInvalidResponseIDsWithoutStoppingHealthyTenant(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, db,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql",
		"0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql",
		"0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql",
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", "legacy-invalid", "Legacy invalid"); err != nil {
		t.Fatal(err)
	}
	for _, connectionID := range []string{"connection-bad"} {
		if _, err = tx.ExecContext(ctx, `INSERT INTO connections
            (tenant_id, connection_id, name, state, provider_device_fingerprint)
            VALUES ($1, $2, $2, 'connected', $3)`, "legacy-invalid", connectionID, bytes.Repeat([]byte(connectionID[:1]), 32)); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO connection_leases
            (tenant_id, connection_id, owner_id, fencing_token, expires_at)
            VALUES ($1, $2, $3, 7, clock_timestamp() + interval '1 hour')`, "legacy-invalid", connectionID, "owner-"+connectionID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO connection_actor_health
            (tenant_id, connection_id, fencing_token, actor_state, connection_state,
             reconnect_count, current_backoff_microseconds, last_safe_reason, requires_reauthorization)
            VALUES ($1, $2, 7, 'ready', 'connected', 0, 0, 'none', false)`, "legacy-invalid", connectionID); err != nil {
			t.Fatal(err)
		}
	}
	const badInboxID = "legacy-inbox"
	badResponseID := "legacy\u202eresponse"
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 7, clock_timestamp())`,
		"legacy-invalid", badInboxID, "connection-bad", badResponseID, bytes.Repeat([]byte{1}, 32), []byte("legacy-raw"), "owner-connection-bad"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox_conflicts
        (tenant_id, conflict_id, connection_id, provider_response_id, conflicting_digest, conflicting_raw_envelope)
        VALUES ($1, $2, $3, $4, $5, $6)`, "legacy-invalid", "legacy-conflict", "connection-bad", badResponseID, bytes.Repeat([]byte{2}, 32), []byte("conflict")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_cursor_history
        (tenant_id, connection_id, cursor_scope, cursor_digest, provider_response_id)
        VALUES ($1, $2, $3, $4, $5)`, "legacy-invalid", "connection-bad", "conversation-bad", bytes.Repeat([]byte{3}, 32), "history\ue000response"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_cursor_budgets
        (tenant_id, connection_id, cursor_scope, accepted_advances, exhausted, last_provider_response_id)
        VALUES ($1, $2, $3, 1, false, $4)`, "legacy-invalid", "connection-bad", "conversation-bad", " budget-response "); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	healthyTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-healthy"); err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", "legacy-healthy", "Legacy healthy"); err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint)
        VALUES ($1, 'connection-good', 'connection-good', 'connected', $2)`, "legacy-healthy", bytes.Repeat([]byte{4}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, `INSERT INTO connection_leases
        (tenant_id, connection_id, owner_id, fencing_token, expires_at)
        VALUES ($1, 'connection-good', 'owner-connection-good', 7, clock_timestamp() + interval '1 hour')`, "legacy-healthy"); err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, `INSERT INTO connection_actor_health
        (tenant_id, connection_id, fencing_token, actor_state, connection_state,
         reconnect_count, current_backoff_microseconds, last_safe_reason, requires_reauthorization)
        VALUES ($1, 'connection-good', 7, 'ready', 'connected', 0, 0, 'none', false)`, "legacy-healthy"); err != nil {
		t.Fatal(err)
	}
	if _, err = healthyTx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'healthy-inbox', 'connection-good', 'healthy-response', $2, 'healthy',
                'owner-connection-good', 7, clock_timestamp())`, "legacy-healthy", bytes.Repeat([]byte{4}, 32)); err != nil {
		t.Fatal(err)
	}
	if err = healthyTx.Commit(); err != nil {
		t.Fatal(err)
	}

	applyNamedMigrations(t, ctx, db, "0013_task7_provider_response_id_boundary.sql", "0014_task7_rejected_provider_responses.sql")

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-invalid"); err != nil {
		t.Fatal(err)
	}
	var quarantineCount, activeBadCount, invalidConstraints, forcedTables int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM provider_response_id_quarantine WHERE tenant_id = $1", "legacy-invalid").Scan(&quarantineCount); err != nil || quarantineCount != 4 {
		t.Fatalf("legacy response quarantine rows = %d, %v", quarantineCount, err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = 'connection-bad') +
        (SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = 'connection-bad') +
        (SELECT count(*) FROM provider_cursor_history WHERE tenant_id = $1 AND connection_id = 'connection-bad') +
        (SELECT count(*) FROM provider_cursor_budgets WHERE tenant_id = $1 AND connection_id = 'connection-bad')`, "legacy-invalid").Scan(&activeBadCount); err != nil || activeBadCount != 0 {
		t.Fatalf("active invalid legacy rows = %d, %v", activeBadCount, err)
	}
	var badState, badOwner, badActorState, badReason string
	var badToken int64
	if err = tx.QueryRowContext(ctx, `SELECT connection.state, COALESCE(lease.owner_id, ''), lease.fencing_token,
        health.actor_state, health.last_safe_reason
        FROM connections AS connection
        JOIN connection_leases AS lease USING (tenant_id, connection_id)
        JOIN connection_actor_health AS health USING (tenant_id, connection_id)
        WHERE connection.tenant_id = $1 AND connection.connection_id = 'connection-bad'`, "legacy-invalid").
		Scan(&badState, &badOwner, &badToken, &badActorState, &badReason); err != nil {
		t.Fatal(err)
	}
	if badState != "suspended" || badOwner != "" || badToken != 8 || badActorState != "stopped" || badReason != "provider-protocol" {
		t.Fatalf("bad connection quarantine = state=%s owner=%q token=%d actor=%s reason=%s", badState, badOwner, badToken, badActorState, badReason)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint
        WHERE conname IN ('provider_inbox_response_id_boundary', 'provider_inbox_conflicts_response_id_boundary',
                          'provider_cursor_history_response_id_boundary', 'provider_cursor_budgets_response_id_boundary')
          AND NOT convalidated`).Scan(&invalidConstraints); err != nil || invalidConstraints != 0 {
		t.Fatalf("unvalidated response constraints = %d, %v", invalidConstraints, err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('provider_inbox', 'provider_inbox_conflicts', 'provider_cursor_history',
                          'provider_cursor_budgets', 'provider_response_id_quarantine')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 5 {
		t.Fatalf("response migration FORCE RLS tables = %d, %v", forcedTables, err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
		VALUES ($1, 'must-fail', 'connection-bad', $2, $3, 'x', 'owner-connection-bad', 8, clock_timestamp())`,
		"legacy-invalid", "new\u202einvalid", bytes.Repeat([]byte{9}, 32)); err == nil {
		t.Fatal("validated response-ID constraint admitted a Unicode format character")
	}
	healthyCheck, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer healthyCheck.Rollback()
	if _, err = healthyCheck.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "legacy-healthy"); err != nil {
		t.Fatal(err)
	}
	var goodState, goodOwner string
	var goodToken int64
	var goodInboxCount int
	if err = healthyCheck.QueryRowContext(ctx, `SELECT connection.state, lease.owner_id, lease.fencing_token
        FROM connections AS connection JOIN connection_leases AS lease USING (tenant_id, connection_id)
        WHERE connection.tenant_id = $1 AND connection.connection_id = 'connection-good'`, "legacy-healthy").Scan(&goodState, &goodOwner, &goodToken); err != nil {
		t.Fatal(err)
	}
	if goodState != "connected" || goodOwner != "owner-connection-good" || goodToken != 7 {
		t.Fatalf("healthy tenant changed = state=%s owner=%s token=%d", goodState, goodOwner, goodToken)
	}
	if err = healthyCheck.QueryRowContext(ctx, "SELECT count(*) FROM provider_inbox WHERE tenant_id = $1", "legacy-healthy").Scan(&goodInboxCount); err != nil || goodInboxCount != 1 {
		t.Fatalf("healthy tenant inbox count = %d, %v", goodInboxCount, err)
	}
}

func TestPostgresIntegration0015ReconcilesLegacyConflictsAndQuotasAtomically(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, db,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql",
		"0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql",
		"0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql",
		"0013_task7_provider_response_id_boundary.sql", "0014_task7_rejected_provider_responses.sql",
	)
	const tenantID = "legacy-0015"
	const connectionID = "connection-0015"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", tenantID, "Legacy 0015"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint)
        VALUES ($1, $2, $2, 'connected', $3)`, tenantID, connectionID, bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_leases
        (tenant_id, connection_id, owner_id, fencing_token, expires_at)
        VALUES ($1, $2, 'owner-0015', 7, clock_timestamp() + interval '1 hour')`, tenantID, connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_actor_health
        (tenant_id, connection_id, fencing_token, actor_state, connection_state,
         reconnect_count, current_backoff_microseconds, last_safe_reason, requires_reauthorization)
        VALUES ($1, $2, 7, 'ready', 'connected', 0, 0, 'none', false)`, tenantID, connectionID); err != nil {
		t.Fatal(err)
	}
	sameDigest := sha256.Sum256([]byte("same-family-digest"))
	preconflictedDigest := sha256.Sum256([]byte("same-family-preconflicted-digest"))
	conflictDigest := sha256.Sum256([]byte("conflict-family-digest"))
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'inbox-same', $2, 'response-same', $3, 'same', 'owner-0015', 7, clock_timestamp())`, tenantID, connectionID, sameDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_rejected_responses
        (tenant_id, connection_id, provider_response_id, envelope_digest, reason, occurrence_count)
        VALUES ($1, $2, 'response-same', $3, 'provider_cursor_budget_exhausted', 4)`, tenantID, connectionID, sameDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'inbox-same-preconflicted', $2, 'response-same-preconflicted', $3, 'same-preconflicted',
                'owner-0015', 7, clock_timestamp())`, tenantID, connectionID, preconflictedDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_rejected_responses
        (tenant_id, connection_id, provider_response_id, envelope_digest, reason,
         ack_pending, conflicted, occurrence_count)
        VALUES ($1, $2, 'response-same-preconflicted', $3, 'provider_cursor_budget_exhausted',
                false, true, 6)`, tenantID, connectionID, preconflictedDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'inbox-conflict', $2, 'response-conflict', $3, 'conflict', 'owner-0015', 7, clock_timestamp())`, tenantID, connectionID, conflictDigest[:]); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("conflict-%d", index)))
		if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox_conflicts
            (tenant_id, conflict_id, connection_id, provider_response_id, conflicting_digest, conflicting_raw_envelope, observed_at)
            VALUES ($1, $2, $3, 'response-conflict', $4, $5, clock_timestamp() + ($6 * interval '1 microsecond'))`,
			tenantID, fmt.Sprintf("conflict-%d", index), connectionID, digest[:], bytes.Repeat([]byte{byte(index + 1)}, 1024), index); err != nil {
			t.Fatal(err)
		}
	}
	largeRaw := bytes.Repeat([]byte{'p'}, 4<<20)
	for index := 0; index < 9; index++ {
		responseID := fmt.Sprintf("response-poison-%02d", index)
		digest := sha256.Sum256([]byte(responseID))
		if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
            (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
             poisoned, poison_reason, owner_id, fencing_token, received_at)
            VALUES ($1, $2, $3, $4, $5, $6, true, 'legacy_poison', 'owner-0015', 7,
                    clock_timestamp() + ($7 * interval '1 microsecond'))`,
			tenantID, "inbox-"+responseID, connectionID, responseID, digest[:], largeRaw, index); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 260; index++ {
		responseID := fmt.Sprintf("response-rejected-%03d", index)
		digest := sha256.Sum256([]byte(responseID))
		if _, err = tx.ExecContext(ctx, `INSERT INTO provider_rejected_responses
            (tenant_id, connection_id, provider_response_id, envelope_digest, reason, first_seen_at)
            VALUES ($1, $2, $3, $4, 'provider_cursor_budget_exhausted',
                    clock_timestamp() + ($5 * interval '1 microsecond'))`, tenantID, connectionID, responseID, digest[:], index); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	applyNamedMigrations(t, ctx, db, "0015_task7_provider_response_reservations.sql")
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	var sameInbox, sameRejected int
	var sameDisposition string
	var sameConflicted bool
	var sameOccurrences int64
	if err = tx.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-same'),
        (SELECT count(*) FROM provider_rejected_responses WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-same'),
        disposition, conflicted, occurrence_count
        FROM provider_response_reservations
        WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-same'`, tenantID, connectionID).
		Scan(&sameInbox, &sameRejected, &sameDisposition, &sameConflicted, &sameOccurrences); err != nil {
		t.Fatal(err)
	}
	if sameInbox != 1 || sameRejected != 0 || sameDisposition != "inbox" || sameConflicted || sameOccurrences != 5 {
		t.Fatalf("same-digest canonical disposition = inbox=%d rejected=%d disposition=%s conflict=%v occurrences=%d",
			sameInbox, sameRejected, sameDisposition, sameConflicted, sameOccurrences)
	}
	var preconflictedInbox, preconflictedRejected int
	var preconflictedDisposition string
	var preconflictedReservationConflict, preconflictedInboxPoisoned bool
	var preconflictedInboxACK, preconflictedRejectedACK, preconflictedRejectedConflict bool
	var preconflictedOccurrences int64
	if err = tx.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-same-preconflicted'),
        (SELECT count(*) FROM provider_rejected_responses WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-same-preconflicted'),
        reservation.disposition, reservation.conflicted, reservation.occurrence_count,
        inbox.poisoned, inbox.ack_pending, rejected.conflicted, rejected.ack_pending
        FROM provider_response_reservations AS reservation
        JOIN provider_inbox AS inbox USING (tenant_id, connection_id, provider_response_id)
        JOIN provider_rejected_responses AS rejected USING (tenant_id, connection_id, provider_response_id)
        WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
          AND reservation.provider_response_id = 'response-same-preconflicted'`, tenantID, connectionID).
		Scan(&preconflictedInbox, &preconflictedRejected, &preconflictedDisposition,
			&preconflictedReservationConflict, &preconflictedOccurrences, &preconflictedInboxPoisoned,
			&preconflictedInboxACK, &preconflictedRejectedConflict, &preconflictedRejectedACK); err != nil {
		t.Fatal(err)
	}
	if preconflictedInbox != 1 || preconflictedRejected != 1 || preconflictedDisposition != "inbox" ||
		!preconflictedReservationConflict || preconflictedOccurrences != 7 || !preconflictedInboxPoisoned ||
		preconflictedInboxACK || !preconflictedRejectedConflict || preconflictedRejectedACK {
		t.Fatalf("same-digest preconflicted disposition = inbox=%d rejected=%d disposition=%s reservation=(%v,%d) inbox=(%v,%v) rejected=(%v,%v)",
			preconflictedInbox, preconflictedRejected, preconflictedDisposition,
			preconflictedReservationConflict, preconflictedOccurrences, preconflictedInboxPoisoned,
			preconflictedInboxACK, preconflictedRejectedConflict, preconflictedRejectedACK)
	}
	var conflictRows int
	var conflictOccurrences, reservationOccurrences int64
	var reservationConflicted, inboxPoisoned, inboxACKPending bool
	if err = tx.QueryRowContext(ctx, `SELECT count(*), max(occurrence_count)
        FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = 'response-conflict'`, tenantID, connectionID).
		Scan(&conflictRows, &conflictOccurrences); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT reservation.conflicted, reservation.occurrence_count, inbox.poisoned, inbox.ack_pending
        FROM provider_response_reservations AS reservation
        JOIN provider_inbox AS inbox USING (tenant_id, connection_id, provider_response_id)
        WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2 AND reservation.provider_response_id = 'response-conflict'`, tenantID, connectionID).
		Scan(&reservationConflicted, &reservationOccurrences, &inboxPoisoned, &inboxACKPending); err != nil {
		t.Fatal(err)
	}
	if conflictRows != 1 || conflictOccurrences != 3 || !reservationConflicted || reservationOccurrences != 4 || !inboxPoisoned || inboxACKPending {
		t.Fatalf("legacy conflicts = rows=%d occurrences=%d reservation=(%v,%d) inbox=(%v,%v)",
			conflictRows, conflictOccurrences, reservationConflicted, reservationOccurrences, inboxPoisoned, inboxACKPending)
	}
	var poisonRows, rejectedRows int
	var poisonBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*), COALESCE(sum(octet_length(raw_envelope)), 0)
        FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND poisoned`, tenantID, connectionID).Scan(&poisonRows, &poisonBytes); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_rejected_responses
        WHERE tenant_id = $1 AND connection_id = $2`, tenantID, connectionID).Scan(&rejectedRows); err != nil {
		t.Fatal(err)
	}
	wantPoisonBytes := int64(7*(4<<20) + len("same-preconflicted") + len("conflict"))
	if poisonRows != 9 || poisonBytes != wantPoisonBytes || rejectedRows != 256 || poisonBytes > 32<<20 {
		t.Fatalf("post-upgrade quota = poison rows=%d bytes=%d rejected=%d, want 9/%d/256",
			poisonRows, poisonBytes, rejectedRows, wantPoisonBytes)
	}
	var overflowPoison, overflowPoisonBytes, overflowRejected int64
	if err = tx.QueryRowContext(ctx, `SELECT overflow_poison_rows, overflow_poison_bytes, overflow_rejected_rows
        FROM provider_response_overflow_audits WHERE tenant_id = $1 AND connection_id = $2`, tenantID, connectionID).
		Scan(&overflowPoison, &overflowPoisonBytes, &overflowRejected); err != nil ||
		overflowPoison != 2 || overflowPoisonBytes != 8<<20 || overflowRejected != 5 {
		t.Fatalf("overflow audit = poison=%d bytes=%d rejected=%d error=%v",
			overflowPoison, overflowPoisonBytes, overflowRejected, err)
	}
	var state, owner, actorState, reason string
	var token int64
	if err = tx.QueryRowContext(ctx, `SELECT connection.state, COALESCE(lease.owner_id, ''), lease.fencing_token,
        health.actor_state, health.last_safe_reason
        FROM connections AS connection
        JOIN connection_leases AS lease USING (tenant_id, connection_id)
        JOIN connection_actor_health AS health USING (tenant_id, connection_id)
        WHERE connection.tenant_id = $1 AND connection.connection_id = $2`, tenantID, connectionID).
		Scan(&state, &owner, &token, &actorState, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "suspended" || owner != "" || token != 8 || actorState != "stopped" || reason != "provider-protocol" {
		t.Fatalf("0015 quarantine = state=%s owner=%q token=%d actor=%s reason=%s", state, owner, token, actorState, reason)
	}
	var forcedTables int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('provider_inbox', 'provider_inbox_conflicts', 'provider_rejected_responses',
                          'provider_response_reservations', 'provider_response_overflow_audits',
                          'connections', 'connection_leases', 'connection_actor_health')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 8 {
		t.Fatalf("0015 FORCE RLS tables = %d, %v", forcedTables, err)
	}
}

func TestPostgresIntegrationFailed0015ReconciliationRollsBackAndRestoresForceRLS(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, db,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql",
		"0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql",
		"0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql",
		"0013_task7_provider_response_id_boundary.sql", "0014_task7_rejected_provider_responses.sql",
	)
	const tenantID = "rollback-0015"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", tenantID, "Rollback 0015"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint)
        VALUES ($1, 'connection-rollback', 'connection-rollback', 'connected', $2)`, tenantID, bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_leases
        (tenant_id, connection_id, owner_id, fencing_token, expires_at)
        VALUES ($1, 'connection-rollback', 'owner-rollback', 7, clock_timestamp() + interval '1 hour')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connection_actor_health
        (tenant_id, connection_id, fencing_token, actor_state, connection_state,
         reconnect_count, current_backoff_microseconds, last_safe_reason, requires_reauthorization)
        VALUES ($1, 'connection-rollback', 7, 'ready', 'connected', 0, 0, 'none', false)`, tenantID); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("rollback-conflict"))
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox
        (tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest, raw_envelope,
         owner_id, fencing_token, received_at)
        VALUES ($1, 'inbox-rollback', 'connection-rollback', 'response-rollback', $2, 'raw', 'owner-rollback', 7, clock_timestamp())`, tenantID, digest[:]); err != nil {
		t.Fatal(err)
	}
	conflictDigest := sha256.Sum256([]byte("rollback-conflict-other"))
	if _, err = tx.ExecContext(ctx, `INSERT INTO provider_inbox_conflicts
        (tenant_id, conflict_id, connection_id, provider_response_id, conflicting_digest, conflicting_raw_envelope)
        VALUES ($1, 'conflict-rollback', 'connection-rollback', 'response-rollback', $2, 'other')`, tenantID, conflictDigest[:]); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE FUNCTION task7_fail_0015_reconciliation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected 0015 reconciliation failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TRIGGER task7_fail_0015_reconciliation BEFORE UPDATE ON connection_leases
FOR EACH STATEMENT EXECUTE FUNCTION task7_fail_0015_reconciliation()`); err != nil {
		t.Fatal(err)
	}
	contents, err := Migrations.ReadFile("migrations/0015_task7_provider_response_reservations.sql")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, string(contents)); err == nil {
		t.Fatal("injected 0015 reconciliation unexpectedly succeeded")
	}
	if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
		t.Fatalf("rollback failed migration: %v", rollbackErr)
	}
	var reservationTable, auditTable sql.NullString
	if err = conn.QueryRowContext(ctx, `SELECT to_regclass('provider_response_reservations')::text,
        to_regclass('provider_response_overflow_audits')::text`).Scan(&reservationTable, &auditTable); err != nil {
		t.Fatal(err)
	}
	if reservationTable.Valid || auditTable.Valid {
		t.Fatalf("failed 0015 retained new tables reservation=%q audit=%q", reservationTable.String, auditTable.String)
	}
	var forcedTables int
	if err = conn.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('provider_inbox', 'provider_inbox_conflicts', 'provider_rejected_responses',
                          'connections', 'connection_leases', 'connection_actor_health')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 6 {
		t.Fatalf("failed 0015 FORCE RLS tables = %d, %v", forcedTables, err)
	}
	checkTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer checkTx.Rollback()
	if _, err = checkTx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	var inboxRows, conflictRows int
	if err = checkTx.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM provider_inbox WHERE tenant_id = $1 AND connection_id = 'connection-rollback'),
        (SELECT count(*) FROM provider_inbox_conflicts WHERE tenant_id = $1 AND connection_id = 'connection-rollback')`, tenantID).
		Scan(&inboxRows, &conflictRows); err != nil || inboxRows != 1 || conflictRows != 1 {
		t.Fatalf("failed 0015 changed legacy state inbox=%d conflicts=%d error=%v", inboxRows, conflictRows, err)
	}
}

func TestPostgresIntegrationTask8MigrationRepairsLegacyReauthorizationDelivery(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newUpgradeIntegrationDatabase(t, ctx, adminDSN)
	applyNamedMigrations(t, ctx, db,
		"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql",
		"0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql",
		"0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql",
		"0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql",
		"0013_task7_provider_response_id_boundary.sql", "0014_task7_rejected_provider_responses.sql",
		"0015_task7_provider_response_reservations.sql",
	)
	const (
		tenantID     = "task8-legacy-reauth"
		connectionID = "task8-legacy-connection"
		eventID      = "task8-stable-reauth-event"
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)", tenantID, "Task 8 legacy reauth"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO connections
        (tenant_id, connection_id, name, state, provider_device_fingerprint, reauthorization_event_id)
        VALUES ($1, $2, $2, 'reauthorization-required', $3, $4)`,
		tenantID, connectionID, bytes.Repeat([]byte{8}, 32), eventID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO lines (
        tenant_id, line_id, connection_id, provider_participant_id,
        provider_outgoing_id, normalized_phone, display_name
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tenantID, "task8-legacy-line", connectionID, "legacy-participant", "legacy-outgoing", "+12025550109", "Legacy SIM"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	applyNamedMigrations(t, ctx, db, "0017_task8_line_metadata.sql")

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatal(err)
	}
	var legacyLineActive bool
	if err = tx.QueryRowContext(ctx, `SELECT active FROM lines
        WHERE tenant_id = $1 AND line_id = $2`, tenantID, "task8-legacy-line").Scan(&legacyLineActive); err != nil || !legacyLineActive {
		t.Fatalf("legacy line active adoption = %v, %v", legacyLineActive, err)
	}
	var eventType, aggregateType, aggregateID, storedConnectionID string
	var canonicalBody []byte
	if err = tx.QueryRowContext(ctx, `SELECT event_type, aggregate_type, aggregate_id, COALESCE(connection_id, ''), canonical_body
        FROM gateway_events WHERE tenant_id = $1 AND event_id = $2`, tenantID, eventID).
		Scan(&eventType, &aggregateType, &aggregateID, &storedConnectionID, &canonicalBody); err != nil {
		t.Fatal(err)
	}
	if eventType != "connection.reauthorization_required" || aggregateType != "connection" ||
		aggregateID != connectionID || storedConnectionID != connectionID {
		t.Fatalf("repaired legacy event identity = %s/%s/%s/%s", eventType, aggregateType, aggregateID, storedConnectionID)
	}
	var body map[string]any
	if err = json.Unmarshal(canonicalBody, &body); err != nil || body["event_id"] != eventID ||
		body["tenant_id"] != tenantID || body["connection_id"] != connectionID ||
		body["type"] != "connection.reauthorization_required" || body["version"] != float64(1) || body["occurred_at"] == "" {
		t.Fatalf("repaired legacy event body = %#v, %v", body, err)
	}
	var outboxes int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM event_outbox
        WHERE tenant_id = $1 AND event_id = $2 AND destination IN ('webhook', 'kafka')`, tenantID, eventID).Scan(&outboxes); err != nil || outboxes != 2 {
		t.Fatalf("repaired legacy outboxes = %d, %v", outboxes, err)
	}
	var forcedTables int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_class
        WHERE relnamespace = current_schema()::regnamespace
          AND relname IN ('connections', 'gateway_events', 'event_outbox')
          AND relrowsecurity AND relforcerowsecurity`).Scan(&forcedTables); err != nil || forcedTables != 3 {
		t.Fatalf("Task 8 repair FORCE RLS tables = %d, %v", forcedTables, err)
	}
}

func applicationDSN(t *testing.T, adminDSN, role, password, schema string) string {
	t.Helper()
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		t.Fatalf("test DSN must be a postgres URL, got scheme %q", parsed.Scheme)
	}
	parsed.User = url.UserPassword(role, password)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func seedTenantAndConnection(t *testing.T, ctx context.Context, repository *Repository, tenantID domain.TenantID, connectionID domain.ConnectionID, fingerprintByte byte) {
	t.Helper()
	if err := repository.SaveTenant(ctx, domain.Tenant{ID: tenantID, Name: string(tenantID)}); err != nil {
		t.Fatalf("save tenant %s: %v", tenantID, err)
	}
	if err := repository.SaveConnection(ctx, tenantID, ConnectionRecord{
		Connection:                domain.Connection{ID: connectionID, TenantID: tenantID, Name: string(connectionID), State: domain.ConnectionStateConnected},
		ProviderDeviceFingerprint: bytes.Repeat([]byte{fingerprintByte}, 32),
	}); err != nil {
		t.Fatalf("save connection %s: %v", connectionID, err)
	}
}

func exercisePairingHardening(t *testing.T, ctx context.Context, repository *Repository) {
	t.Helper()
	if err := repository.SaveConnection(ctx, "tenant-a", ConnectionRecord{Connection: domain.Connection{ID: "pairing-new", TenantID: "tenant-a", Name: "New", State: domain.ConnectionStateUnpaired}}); err != nil {
		t.Fatalf("save unpaired connection: %v", err)
	}
	if prior, err := repository.BeginPairing(ctx, "tenant-a", "pairing-new", "attempt-initial", 5*time.Minute); err != nil || prior != domain.ConnectionStateUnpaired {
		t.Fatalf("begin initial pairing: %v", err)
	}
	if _, err := repository.BeginPairing(ctx, "tenant-a", "pairing-new", "attempt-second", 5*time.Minute); !errors.Is(err, pairing.ErrAttemptActive) {
		t.Fatalf("fresh second pairing = %v", err)
	}
	if _, err := repository.RestorePairing(ctx, "tenant-a", "pairing-new", "attempt-old"); !errors.Is(err, pairing.ErrAttemptSuperseded) {
		t.Fatalf("old restore = %v", err)
	}
	if restored, err := repository.RestorePairing(ctx, "tenant-a", "pairing-new", "attempt-initial"); err != nil || restored != domain.ConnectionStateUnpaired {
		t.Fatalf("restore initial pairing = %q, %v", restored, err)
	}
	transition, err := repository.MarkReauthorizationRequired(ctx, "tenant-a", "connection-a")
	if err != nil || !transition.Transitioned || transition.EventID == "" {
		t.Fatalf("mark reauthorization = %#v, %v", transition, err)
	}
	if prior, err := repository.BeginPairing(ctx, "tenant-a", "connection-a", "attempt-reauth", 5*time.Minute); err != nil || prior != domain.ConnectionStateReauthorizationRequired {
		t.Fatalf("begin reauthorization: %v", err)
	}
	if restored, err := repository.ReconcileStalePairings(ctx, "tenant-a", 5*time.Minute); err != nil || restored != 0 {
		t.Fatalf("fresh reconciliation = %d, %v", restored, err)
	}
	if err := agePairingAttemptForIntegration(ctx, repository, "tenant-a", "connection-a"); err != nil {
		t.Fatalf("age reauthorization pairing: %v", err)
	}
	if restored, err := repository.ReconcileStalePairings(ctx, "tenant-a", 5*time.Minute); err != nil || restored != 1 {
		t.Fatalf("reconcile reauthorization = %d, %v", restored, err)
	}
	envelope := session.Envelope{Version: 1, Provider: "gmessages", Ciphertext: make([]byte, 16), WrappedDEK: []byte{1}, Nonce: make([]byte, 12), KeyID: "integration-key", KeyVersion: 1}
	if err := repository.SaveEncryptedSession(ctx, "tenant-a", "connection-a", envelope); err != nil {
		t.Fatalf("save encrypted session: %v", err)
	}
	loaded, err := repository.LoadEncryptedSession(ctx, "tenant-a", "connection-a")
	if err != nil || loaded.Revision != 1 {
		t.Fatalf("load encrypted session = %#v, %v", loaded, err)
	}
	loaded.KeyVersion = 2
	if swapped, err := repository.CompareAndSwapEncryptedSession(ctx, "tenant-a", "connection-a", loaded.Revision, loaded); err != nil || !swapped {
		t.Fatalf("CAS encrypted session = %v, %v", swapped, err)
	}
	if swapped, err := repository.CompareAndSwapEncryptedSession(ctx, "tenant-a", "connection-a", loaded.Revision, loaded); err != nil || swapped {
		t.Fatalf("stale CAS encrypted session = %v, %v", swapped, err)
	}
	if err := inTenantExec(ctx, repository, "tenant-a", func(tx transaction) error {
		_, resetErr := tx.ExecContext(ctx, `UPDATE connections
            SET state = 'connected', reauthorization_event_id = NULL,
                pairing_prior_state = NULL, pairing_started_at = NULL, pairing_attempt_id = NULL
            WHERE tenant_id = $1 AND connection_id = $2`, "tenant-a", "connection-a")
		return resetErr
	}); err != nil {
		t.Fatalf("restore shared connection fixture: %v", err)
	}
}

func agePairingAttemptForIntegration(ctx context.Context, repository *Repository, tenantID domain.TenantID, connectionID domain.ConnectionID) error {
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, `UPDATE connections
            SET pairing_started_at = clock_timestamp() - interval '6 minutes'
            WHERE tenant_id = $1 AND connection_id = $2 AND state = 'pairing'`, string(tenantID), string(connectionID))
		return err
	})
}

func newUpgradeIntegrationDatabase(t *testing.T, ctx context.Context, adminDSN string) *sql.DB {
	t.Helper()
	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}
	if err = adminDB.PingContext(ctx); err != nil {
		adminDB.Close()
		t.Fatalf("ping admin database: %v", err)
	}
	suffix := fmt.Sprintf("upgrade_%d", time.Now().UnixNano())
	role, schema, password := "sirenaix_it_"+suffix, "sirenaix_it_"+suffix, "integration-"+uuid.NewString()
	if _, err = adminDB.ExecContext(ctx, "CREATE ROLE "+pq.QuoteIdentifier(role)+" LOGIN PASSWORD "+pq.QuoteLiteral(password)+" NOSUPERUSER NOBYPASSRLS"); err != nil {
		adminDB.Close()
		t.Fatalf("create upgrade role: %v", err)
	}
	if _, err = adminDB.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)+" AUTHORIZATION "+pq.QuoteIdentifier(role)); err != nil {
		_, _ = adminDB.ExecContext(ctx, "DROP ROLE "+pq.QuoteIdentifier(role))
		adminDB.Close()
		t.Fatalf("create upgrade schema: %v", err)
	}
	appDB, err := sql.Open("postgres", applicationDSN(t, adminDSN, role, password, schema))
	if err != nil {
		t.Fatalf("open upgrade database: %v", err)
	}
	t.Cleanup(func() {
		_ = appDB.Close()
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
		_, _ = adminDB.ExecContext(context.Background(), "DROP ROLE "+pq.QuoteIdentifier(role))
		_ = adminDB.Close()
	})
	if err = appDB.PingContext(ctx); err != nil {
		t.Fatalf("ping upgrade database: %v", err)
	}
	return appDB
}

func applyNamedMigrations(t *testing.T, ctx context.Context, db *sql.DB, names ...string) {
	t.Helper()
	for _, name := range names {
		contents, err := Migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err = db.ExecContext(ctx, string(contents)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func assertApplicationRoleCannotBypassRLS(t *testing.T, ctx context.Context, db *sql.DB, role string) {
	t.Helper()
	var superuser, bypassRLS bool
	if err := db.QueryRowContext(ctx, "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = $1", role).Scan(&superuser, &bypassRLS); err != nil {
		t.Fatalf("inspect application role: %v", err)
	}
	if superuser || bypassRLS {
		t.Fatalf("application role has unsafe flags: superuser=%v bypassrls=%v", superuser, bypassRLS)
	}
}

func assertTenantARLSCannotReadOrWriteTenantB(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var unscopedCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM tenants").Scan(&unscopedCount); err != nil {
		t.Fatalf("query without tenant context: %v", err)
	}
	if unscopedCount != 0 {
		t.Fatalf("unscoped application query read %d tenant rows", unscopedCount)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin direct RLS check: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", "tenant-a"); err != nil {
		t.Fatalf("set direct tenant context: %v", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM tenants WHERE tenant_id = 'tenant-b'").Scan(&count); err != nil {
		t.Fatalf("query cross-tenant row: %v", err)
	}
	if count != 0 {
		t.Fatalf("tenant A read %d tenant B rows through RLS", count)
	}
	result, err := tx.ExecContext(ctx, "UPDATE tenants SET name = 'blocked' WHERE tenant_id = 'tenant-b'")
	if err != nil {
		t.Fatalf("cross-tenant update returned unexpected database error: %v", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("read cross-tenant update count: %v", err)
	}
	if updated != 0 {
		t.Fatalf("tenant A updated %d tenant B rows through RLS", updated)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO tenants (tenant_id, name) VALUES ('tenant-b-write', 'blocked')"); err == nil {
		t.Fatal("tenant A wrote a tenant B row through RLS")
	}
}

func assertTenantCount(t *testing.T, ctx context.Context, db *sql.DB, tenantID string, query string, args []any, want int) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin count transaction: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", tenantID); err != nil {
		t.Fatalf("set count tenant context: %v", err)
	}
	var got int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatalf("count tenant rows: %v", err)
	}
	if got != want {
		t.Fatalf("tenant count = %d, want %d for %s", got, want, query)
	}
}
