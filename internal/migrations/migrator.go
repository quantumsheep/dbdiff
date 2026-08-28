package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/quantumsheep/dbdiff/drivers"
)

type AppliedMigration struct {
	Version   string
	Name      string
	Checksum  string
	AppliedAt time.Time
}

type Migrator interface {
	Close() error
	EnsureHistoryTable(ctx context.Context) error
	AppliedMigrations(ctx context.Context) ([]AppliedMigration, error)
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
	Begin(ctx context.Context, useTransaction bool) (MigrationTransaction, error)
}

type migrationExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type MigrationTransaction interface {
	Apply(ctx context.Context, statement string) error
	Record(ctx context.Context, migration *Migration) error
	Commit() error
	Rollback() error
}

func NewMigrator(ctx context.Context, driverName string, target string,
	schema string) (Migrator, error) {
	if driverName == "" {
		detected, err := drivers.DetectDriver(target, target)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	switch driverName {
	case drivers.SQLiteDriverName:
		return NewSQLiteMigrator(target)
	case drivers.PostgresDriverName:
		return NewPostgresMigrator(ctx, target, schema)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}
