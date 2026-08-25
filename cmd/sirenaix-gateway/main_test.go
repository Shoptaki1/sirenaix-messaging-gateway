package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/app"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ops"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

func TestTrustedTenantAndKafkaTopicParsingIsStrict(t *testing.T) {
	tenants, err := parseTenants("tenant-a,tenant-b")
	if err != nil || len(tenants) != 2 {
		t.Fatalf("parseTenants() = (%v, %v)", tenants, err)
	}
	bindings, err := parseTopicBindings("commands.tenant-a=tenant-a=producer-a,commands.tenant-b=tenant-b=producer-b", tenants)
	if err != nil || len(bindings) != 2 || bindings["commands.tenant-a"].TenantID != domain.TenantID("tenant-a") {
		t.Fatalf("parseTopicBindings() = (%+v, %v)", bindings, err)
	}
	for _, value := range []string{"", "tenant-a,tenant-a", "tenant-a,,tenant-b", "tenant-a, ,tenant-b"} {
		if _, err = parseTenants(value); err == nil {
			t.Fatalf("parseTenants(%q) unexpectedly succeeded", value)
		}
	}
	for _, value := range []string{
		"commands=tenant-c=producer-c", "commands=tenant-a", "commands=tenant-a=", "commands=tenant-a=producer-a,commands=tenant-a=producer-a",
	} {
		if _, err = parseTopicBindings(value, tenants); err == nil {
			t.Fatalf("parseTopicBindings(%q) unexpectedly succeeded", value)
		}
	}
}

func TestDatabasePoolConfigUsesSafeBounds(t *testing.T) {
	getenv := mapEnvironment(map[string]string{
		"SIRENAIX_DB_MAX_OPEN_CONNS":     "48",
		"SIRENAIX_DB_MAX_IDLE_CONNS":     "12",
		"SIRENAIX_DB_CONN_MAX_LIFETIME":  "20m",
		"SIRENAIX_DB_CONN_MAX_IDLE_TIME": "4m",
	})
	config, err := parseDatabasePoolConfig(getenv)
	if err != nil || config.MaxOpen != 48 || config.MaxIdle != 12 || config.MaxLifetime != 20*time.Minute || config.MaxIdleTime != 4*time.Minute {
		t.Fatalf("pool config = (%+v, %v)", config, err)
	}
	for key, value := range map[string]string{
		"SIRENAIX_DB_MAX_OPEN_CONNS":     "257",
		"SIRENAIX_DB_MAX_IDLE_CONNS":     "49",
		"SIRENAIX_DB_CONN_MAX_LIFETIME":  "30s",
		"SIRENAIX_DB_CONN_MAX_IDLE_TIME": "31m",
	} {
		values := map[string]string{"SIRENAIX_DB_MAX_OPEN_CONNS": "48", "SIRENAIX_DB_MAX_IDLE_CONNS": "12"}
		values[key] = value
		if _, err = parseDatabasePoolConfig(mapEnvironment(values)); !errors.Is(err, app.ErrInvalidRuntime) {
			t.Fatalf("pool override %s=%s error = %v", key, value, err)
		}
	}
}

func TestVersionCommandIsOfflineStableAndContainsBuildMetadata(t *testing.T) {
	previousVersion, previousRevision, previousDate := version, revision, buildDate
	version, revision, buildDate = "1.2.3", "abc123", "2026-08-25T00:00:00Z"
	t.Cleanup(func() { version, revision, buildDate = previousVersion, previousRevision, previousDate })
	var output bytes.Buffer
	if err := runCommand(context.Background(), []string{"version"}, func(string) string { return "" }, &output); err != nil {
		t.Fatalf("version command error = %v", err)
	}
	if got, want := output.String(), "sirenaix-gateway 1.2.3 (revision abc123, built 2026-08-25T00:00:00Z)\n"; got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestCommandFailureMessagesAreActionableButNeverRenderRawErrors(t *testing.T) {
	secret := "postgres://user:secret@example.invalid/database"
	cases := []struct {
		args []string
		err  error
		want string
	}{
		{[]string{"migrate", "status", "--check"}, fmt.Errorf("%w: %s", postgres.ErrMigrationPending, secret), "sirenaix-gateway: database migrations are pending\n"},
		{[]string{"migrate", "up"}, fmt.Errorf("%w: %s", postgres.ErrMigrationDrift, secret), "sirenaix-gateway: migration ledger validation failed\n"},
		{[]string{"tenant", "suspend"}, fmt.Errorf("%w: %s", postgres.ErrTenantPairingActive, secret), "sirenaix-gateway: tenant has an active pairing attempt\n"},
		{[]string{"serve"}, errors.New(secret), "sirenaix-gateway stopped safely; inspect bounded service metrics\n"},
	}
	for _, test := range cases {
		message := safeCommandFailure(test.args, test.err)
		if message != test.want || strings.Contains(message, secret) {
			t.Fatalf("safeCommandFailure(%v) = %q, want %q", test.args, message, test.want)
		}
	}
}

func TestCommandFailureClassificationIsFixedAndNeverUsesErrorContents(t *testing.T) {
	secret := "postgres://user:password@db.invalid/private?topic=tenant-secret"
	for _, test := range []struct {
		args []string
		err  error
		want commandFailureClass
	}{
		{[]string{"migrate", "status"}, fmt.Errorf("%w: %s", errCommandDatabaseUnavailable, secret), commandFailureDatabaseUnavailable},
		{[]string{"migrate", "up"}, fmt.Errorf("%w: %s", postgres.ErrMigrationDrift, secret), commandFailureMigrationLedger},
		{[]string{"tenant", "status"}, fmt.Errorf("%w: %s", postgres.ErrTenantNotFound, secret), commandFailureTenantNotFound},
		{[]string{"tenant", "provision"}, fmt.Errorf("%w: %s", app.ErrInvalidRuntime, secret), commandFailureInvalidInput},
		{[]string{"serve"}, errors.New(secret), commandFailureServiceUnavailable},
	} {
		class := classifyCommandFailure(test.args, test.err)
		message := safeCommandFailure(test.args, test.err)
		if class != test.want || strings.Contains(string(class), secret) || strings.Contains(message, secret) {
			t.Fatalf("failure (%v, %v) = class %q, message %q", test.args, test.err, class, message)
		}
	}
}

func TestKafkaReadinessFailureClassificationDistinguishesUnverifiableAuthorization(t *testing.T) {
	for _, test := range []struct {
		err  error
		want ops.FailureClass
	}{
		{kafka.ErrKafkaTopicMissing, ops.FailureMissing},
		{kafka.ErrKafkaTopicUnauthorized, ops.FailureAuthorization},
		{kafka.ErrKafkaGroupUnauthorized, ops.FailureAuthorization},
		{kafka.ErrKafkaClusterUnauthorized, ops.FailureAuthorization},
		{kafka.ErrKafkaAuthorizationUnverifiable, ops.FailureClass("authorization_unverifiable")},
		{kafka.ErrKafkaMetadataUnavailable, ops.FailureUnavailable},
	} {
		if got := classifyKafkaReadinessFailure(test.err); got != test.want {
			t.Fatalf("classifyKafkaReadinessFailure(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestUnsupportedOperationalCommandsFailBeforeCredentialAccess(t *testing.T) {
	for _, args := range [][]string{{"migrate", "down"}, {"tenant", "delete", "--id", "tenant-a"}} {
		credentialRead := false
		err := runCommand(context.Background(), args, func(string) string {
			credentialRead = true
			return "sensitive-credential"
		}, io.Discard)
		if !errors.Is(err, app.ErrInvalidRuntime) || credentialRead {
			t.Fatalf("runCommand(%v) = %v, credentialRead=%v", args, err, credentialRead)
		}
	}
}

func mapEnvironment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestDevelopmentMasterKeyParsingRequiresExactBase64Key(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(make([]byte, 32))
	key, err := parseDevelopmentMasterKey(encoded)
	if err != nil || len(key) != 32 {
		t.Fatalf("parseDevelopmentMasterKey() = (%x, %v)", key, err)
	}
	for _, value := range []string{"", "not-base64", base64.StdEncoding.EncodeToString(make([]byte, 31)), encoded + "="} {
		if _, err = parseDevelopmentMasterKey(value); err == nil {
			t.Fatalf("development key %q unexpectedly accepted", value)
		}
	}
}

func TestKMSKeyVersionParsingRetainsHistoricalKeysAndRequiresCurrent(t *testing.T) {
	keys, current, err := parseKMSKeys("1=alias/sirenaix-old,2=alias/sirenaix-current", "2")
	if err != nil || current != 2 || keys[1] == "" || keys[2] == "" {
		t.Fatalf("parseKMSKeys() = (%+v, %d, %v)", keys, current, err)
	}
	for _, test := range []struct{ keys, current string }{
		{"", "1"}, {"1=alias/a", "2"}, {"1=alias/a,1=alias/b", "1"}, {"x=alias/a", "1"},
	} {
		if _, _, err = parseKMSKeys(test.keys, test.current); err == nil {
			t.Fatalf("parseKMSKeys(%q, %q) unexpectedly succeeded", test.keys, test.current)
		}
	}
}

func TestOptionalResourceLimitRejectsMalformedOrNonPositiveValues(t *testing.T) {
	if value, err := parseOptionalPositiveInt64("", 10); err != nil || value != 10 {
		t.Fatalf("default = (%d, %v)", value, err)
	}
	for _, value := range []string{"0", "-1", "not-a-number"} {
		if _, err := parseOptionalPositiveInt64(value, 10); err == nil {
			t.Fatalf("parseOptionalPositiveInt64(%q) unexpectedly succeeded", value)
		}
	}
}

func TestOptionalDurationRequiresPositiveBoundedSyntax(t *testing.T) {
	if value, err := parseOptionalDuration("", 4*time.Second); err != nil || value != 4*time.Second {
		t.Fatalf("duration default = (%v, %v)", value, err)
	}
	if value, err := parseOptionalDuration("750ms", 4*time.Second); err != nil || value != 750*time.Millisecond {
		t.Fatalf("duration override = (%v, %v)", value, err)
	}
	for _, value := range []string{"0s", "-1s", "not-a-duration"} {
		if _, err := parseOptionalDuration(value, 4*time.Second); err == nil {
			t.Fatalf("parseOptionalDuration(%q) unexpectedly succeeded", value)
		}
	}
}
