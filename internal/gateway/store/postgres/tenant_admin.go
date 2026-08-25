package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	ErrInvalidTenantAdminInput = errors.New("invalid tenant administration input")
	ErrTenantNotFound          = errors.New("tenant not found")
	ErrTenantPairingActive     = errors.New("tenant has an active pairing attempt")
)

type TenantAdminAction string

const (
	TenantActionProvision TenantAdminAction = "provision"
	TenantActionStatus    TenantAdminAction = "status"
	TenantActionSuspend   TenantAdminAction = "suspend"
	TenantActionResume    TenantAdminAction = "resume"
)

func (action TenantAdminAction) Validate() error {
	switch action {
	case TenantActionProvision, TenantActionStatus, TenantActionSuspend, TenantActionResume:
		return nil
	default:
		return ErrInvalidTenantAdminInput
	}
}

var tenantAdminIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)

type TenantAdminInput struct {
	TenantID       string
	Name           string
	MaxConnections int
	Actor          string
}

func (input TenantAdminInput) Validate() error {
	if !tenantAdminIDPattern.MatchString(input.TenantID) || !safeAdminText(input.Name, 256) ||
		!safeAdminText(input.Actor, 128) || input.MaxConnections < 1 || input.MaxConnections > DefaultMaxConnectionsPerTenant {
		return ErrInvalidTenantAdminInput
	}
	return nil
}

func safeAdminText(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return false
		}
	}
	return true
}

type TenantOperationalStatus struct {
	TenantID        string `json:"tenant_id"`
	Name            string `json:"name"`
	Status          string `json:"status"`
	MaxConnections  int    `json:"max_connections"`
	ConnectionCount int    `json:"connection_count"`
}

type TenantAdministrator struct {
	db    *sql.DB
	actor string
	newID func() string
}

func NewTenantAdministrator(db *sql.DB, actor string) (*TenantAdministrator, error) {
	if db == nil || !safeAdminText(actor, 128) {
		return nil, ErrInvalidTenantAdminInput
	}
	return &TenantAdministrator{db: db, actor: actor, newID: uuid.NewString}, nil
}

func (admin *TenantAdministrator) Provision(ctx context.Context, input TenantAdminInput) (TenantOperationalStatus, error) {
	if admin == nil {
		return TenantOperationalStatus{}, ErrInvalidTenantAdminInput
	}
	input.Actor = admin.actor
	if err := input.Validate(); err != nil {
		return TenantOperationalStatus{}, err
	}
	tx, err := admin.beginTenant(ctx, input.TenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	defer tx.Rollback()

	var oldName, oldStatus string
	var oldQuota int
	err = tx.QueryRowContext(ctx, `SELECT name, status, max_connections FROM tenants WHERE tenant_id = $1 FOR UPDATE`, input.TenantID).Scan(&oldName, &oldStatus, &oldQuota)
	action := ""
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err = tx.ExecContext(ctx, `INSERT INTO tenants (tenant_id, name, max_connections) VALUES ($1, $2, $3)`, input.TenantID, input.Name, input.MaxConnections); err != nil {
			return TenantOperationalStatus{}, errors.New("provision tenant")
		}
		action = "provision"
	case err != nil:
		return TenantOperationalStatus{}, errors.New("read tenant for provision")
	case oldName != input.Name || oldQuota != input.MaxConnections:
		if _, err = tx.ExecContext(ctx, `UPDATE tenants SET name = $2, max_connections = $3, updated_at = clock_timestamp() WHERE tenant_id = $1`, input.TenantID, input.Name, input.MaxConnections); err != nil {
			return TenantOperationalStatus{}, errors.New("update tenant configuration")
		}
		action = "update"
	}
	if action != "" {
		if err = admin.audit(ctx, tx, input.TenantID, action, input.Name, input.MaxConnections); err != nil {
			return TenantOperationalStatus{}, err
		}
	}
	status, err := readTenantOperationalStatus(ctx, tx, input.TenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	if err = tx.Commit(); err != nil {
		return TenantOperationalStatus{}, errors.New("commit tenant provision")
	}
	return status, nil
}

func (admin *TenantAdministrator) Status(ctx context.Context, tenantID string) (TenantOperationalStatus, error) {
	if admin == nil || !tenantAdminIDPattern.MatchString(tenantID) {
		return TenantOperationalStatus{}, ErrInvalidTenantAdminInput
	}
	tx, err := admin.beginTenant(ctx, tenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	defer tx.Rollback()
	status, err := readTenantOperationalStatus(ctx, tx, tenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	if err = tx.Commit(); err != nil {
		return TenantOperationalStatus{}, errors.New("commit tenant status")
	}
	return status, nil
}

func (admin *TenantAdministrator) Suspend(ctx context.Context, tenantID string) (TenantOperationalStatus, error) {
	return admin.changeSuspension(ctx, tenantID, true)
}

func (admin *TenantAdministrator) Resume(ctx context.Context, tenantID string) (TenantOperationalStatus, error) {
	return admin.changeSuspension(ctx, tenantID, false)
}

func (admin *TenantAdministrator) changeSuspension(ctx context.Context, tenantID string, suspend bool) (TenantOperationalStatus, error) {
	if admin == nil || !tenantAdminIDPattern.MatchString(tenantID) {
		return TenantOperationalStatus{}, ErrInvalidTenantAdminInput
	}
	tx, err := admin.beginTenant(ctx, tenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	defer tx.Rollback()
	var name, status string
	var quota int
	if err = tx.QueryRowContext(ctx, `SELECT name, status, max_connections FROM tenants WHERE tenant_id = $1 FOR UPDATE`, tenantID).Scan(&name, &status, &quota); errors.Is(err, sql.ErrNoRows) {
		return TenantOperationalStatus{}, ErrTenantNotFound
	} else if err != nil {
		return TenantOperationalStatus{}, errors.New("lock tenant status")
	}
	wantStatus := "active"
	if suspend {
		wantStatus = "suspended"
	}
	if status != wantStatus {
		if suspend {
			var activePairing bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM connections WHERE tenant_id = $1 AND state = 'pairing')`, tenantID).Scan(&activePairing); err != nil {
				return TenantOperationalStatus{}, errors.New("check tenant pairing state")
			}
			if activePairing {
				return TenantOperationalStatus{}, ErrTenantPairingActive
			}
			if _, err = tx.ExecContext(ctx, `UPDATE connections
SET tenant_suspend_prior_state = state, state = 'suspended', updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND state IN ('unpaired', 'connected', 'degraded', 'reauthorization-required', 'suspended', 'disconnected')
  AND tenant_suspend_prior_state IS NULL`, tenantID); err != nil {
				return TenantOperationalStatus{}, errors.New("suspend tenant connections")
			}
			if _, err = tx.ExecContext(ctx, `UPDATE connection_leases
SET owner_id = NULL, fencing_token = fencing_token + 1, expires_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND owner_id IS NOT NULL`, tenantID); err != nil {
				return TenantOperationalStatus{}, errors.New("fence tenant connections")
			}
			if _, err = tx.ExecContext(ctx, `UPDATE tenants SET status = 'suspended', suspended_at = clock_timestamp(), updated_at = clock_timestamp() WHERE tenant_id = $1`, tenantID); err != nil {
				return TenantOperationalStatus{}, errors.New("suspend tenant")
			}
		} else {
			if _, err = tx.ExecContext(ctx, `UPDATE connections
SET state = tenant_suspend_prior_state, tenant_suspend_prior_state = NULL, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND state = 'suspended' AND tenant_suspend_prior_state IS NOT NULL`, tenantID); err != nil {
				return TenantOperationalStatus{}, errors.New("resume tenant connections")
			}
			if _, err = tx.ExecContext(ctx, `UPDATE tenants SET status = 'active', suspended_at = NULL, updated_at = clock_timestamp() WHERE tenant_id = $1`, tenantID); err != nil {
				return TenantOperationalStatus{}, errors.New("resume tenant")
			}
		}
		if err = admin.audit(ctx, tx, tenantID, wantStatusAction(suspend), name, quota); err != nil {
			return TenantOperationalStatus{}, err
		}
	}
	result, err := readTenantOperationalStatus(ctx, tx, tenantID)
	if err != nil {
		return TenantOperationalStatus{}, err
	}
	if err = tx.Commit(); err != nil {
		return TenantOperationalStatus{}, errors.New("commit tenant status change")
	}
	return result, nil
}

func wantStatusAction(suspend bool) string {
	if suspend {
		return "suspend"
	}
	return "resume"
}

func (admin *TenantAdministrator) beginTenant(ctx context.Context, tenantID string) (*sql.Tx, error) {
	if ctx == nil {
		return nil, ErrInvalidTenantAdminInput
	}
	tx, err := admin.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.New("begin tenant administration")
	}
	if _, err = tx.ExecContext(ctx, tenantContextSQL, tenantID); err != nil {
		_ = tx.Rollback()
		return nil, errors.New("set tenant administration context")
	}
	if _, err = tx.ExecContext(ctx, `SELECT set_config('lock_timeout', '10s', true), set_config('statement_timeout', '30s', true)`); err != nil {
		_ = tx.Rollback()
		return nil, errors.New("set tenant administration timeout")
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1, 0))`, tenantID); err != nil {
		_ = tx.Rollback()
		return nil, errors.New("lock tenant administration")
	}
	return tx, nil
}

func (admin *TenantAdministrator) audit(ctx context.Context, tx *sql.Tx, tenantID, action, name string, quota int) error {
	eventID := admin.newID()
	if eventID == "" {
		return errors.New("create tenant audit identity")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_admin_events
    (tenant_id, event_id, action, actor, tenant_name, max_connections)
VALUES ($1, $2, $3, $4, $5, $6)`, tenantID, eventID, action, admin.actor, name, quota); err != nil {
		return errors.New("record tenant administration audit")
	}
	return nil
}

func readTenantOperationalStatus(ctx context.Context, tx *sql.Tx, tenantID string) (TenantOperationalStatus, error) {
	var status TenantOperationalStatus
	err := tx.QueryRowContext(ctx, `SELECT tenant_id, name, status, max_connections,
    (SELECT count(*) FROM connections WHERE tenant_id = $1)
FROM tenants WHERE tenant_id = $1`, tenantID).Scan(
		&status.TenantID, &status.Name, &status.Status, &status.MaxConnections, &status.ConnectionCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TenantOperationalStatus{}, ErrTenantNotFound
	}
	if err != nil {
		return TenantOperationalStatus{}, fmt.Errorf("read tenant operational status: %w", errors.New("database operation failed"))
	}
	return status, nil
}
