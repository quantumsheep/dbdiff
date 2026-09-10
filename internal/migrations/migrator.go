package migrations

import (
	"context"
	"database/sql"
	"time"
)

type AppliedMigration struct {
	Version   string
	Name      string
	Checksum  string
	AppliedAt time.Time
	Dirty     bool
}

type Migrator interface {
	Close() error

	SupportsTransactionalDDL() bool
	EnsureHistoryTable(ctx context.Context) error
	AppliedMigrations(ctx context.Context) ([]AppliedMigration, error)

	Lock(ctx context.Context) error
	TryLock(ctx context.Context) (bool, error)
	Unlock(ctx context.Context) error

	Begin(ctx context.Context, useTransaction bool) (MigrationTransaction, error)

	RecordDirty(ctx context.Context, migration *Migration) error
	ClearDirty(ctx context.Context, migration *Migration) error

	UpdateChecksum(ctx context.Context, migration *Migration) error

	DeleteRecord(ctx context.Context, version string) error
}

type MigrationExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type MigrationTransaction interface {
	Apply(ctx context.Context, statement string) error
	Record(ctx context.Context, migration *Migration) error
	Commit() error
	Rollback() error
}
