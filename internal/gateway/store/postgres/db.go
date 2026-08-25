package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type rowScanner interface {
	Scan(...any) error
}

type rowIterator interface {
	Next() bool
	Scan(...any) error
	Close() error
	Err() error
}

type transaction interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) rowScanner
	QueryContext(context.Context, string, ...any) (rowIterator, error)
	Commit() error
	Rollback() error
}

type beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (transaction, error)
}

type sqlBeginner struct{ db *sql.DB }

func (db sqlBeginner) BeginTx(ctx context.Context, options *sql.TxOptions) (transaction, error) {
	tx, err := db.db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return sqlTransaction{tx: tx}, nil
}

type sqlTransaction struct{ tx *sql.Tx }

func (tx sqlTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, query, args...)
}

func (tx sqlTransaction) QueryRowContext(ctx context.Context, query string, args ...any) rowScanner {
	return tx.tx.QueryRowContext(ctx, query, args...)
}

func (tx sqlTransaction) QueryContext(ctx context.Context, query string, args ...any) (rowIterator, error) {
	return tx.tx.QueryContext(ctx, query, args...)
}

func (tx sqlTransaction) Commit() error   { return tx.tx.Commit() }
func (tx sqlTransaction) Rollback() error { return tx.tx.Rollback() }

type Repository struct {
	db    beginner
	newID func() string
}

func New(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("postgres repository requires a database")
	}
	return newRepository(sqlBeginner{db: db}), nil
}

func newRepository(db beginner) *Repository {
	return &Repository{db: db, newID: uuid.NewString}
}

const tenantContextSQL = `/* op:tenant_context */ SELECT set_config('sirenaix.tenant_id', $1, true)`

func inTenant[T any](ctx context.Context, repository *Repository, tenantID domain.TenantID, operation func(transaction) (T, error)) (T, error) {
	var zero T
	if tenantID == "" {
		return zero, domain.ErrInvalidTenantID
	}
	tx, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("begin tenant transaction: %w", err)
	}
	if _, err = tx.ExecContext(ctx, tenantContextSQL, string(tenantID)); err != nil {
		_ = tx.Rollback()
		return zero, fmt.Errorf("set tenant context: %w", err)
	}
	value, err := operation(tx)
	if err != nil {
		_ = tx.Rollback()
		return zero, err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return zero, fmt.Errorf("commit tenant transaction: %w", err)
	}
	return value, nil
}

func inTenantExec(ctx context.Context, repository *Repository, tenantID domain.TenantID, operation func(transaction) error) error {
	_, err := inTenant(ctx, repository, tenantID, func(tx transaction) (struct{}, error) {
		return struct{}{}, operation(tx)
	})
	return err
}

func requireAffected(result sql.Result, notFound error) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected row count: %w", err)
	}
	if count == 0 {
		return notFound
	}
	return nil
}
