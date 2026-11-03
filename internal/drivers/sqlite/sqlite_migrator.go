package driverssqlite

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/migrations"
)

var sqliteHistoryDialect = migrations.SQLDialect{
	QuoteIdentifier: driversshared.QuoteIdentifier,
	Placeholder: func(int) string {
		return "?"
	},
	CreateHistoryTableStatement: fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"version"    TEXT NOT NULL PRIMARY KEY,
			"name"       TEXT NOT NULL,
			"checksum"   TEXT NOT NULL,
			"applied_at" TEXT NOT NULL,
			"dirty"      INTEGER NOT NULL DEFAULT 0
		);
	`, driversshared.QuoteIdentifier(driversshared.MigrationHistoryTableName)),
	HistoryTableExistsQuery: `SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?);`,
	DirtyColumnExistsQuery:  `SELECT EXISTS (SELECT 1 FROM pragma_table_info(?) WHERE name = 'dirty');`,
	DirtyColumnType:         "INTEGER",
	TrueLiteral:             "1",
	FalseLiteral:            "0",
	StoresTimeAsText:        true,
}

type SQLiteMigrator struct {
	migrations.SQLMigrator
}

func NewSQLiteMigrator(path string) (*SQLiteMigrator, error) {
	// The name holds no _foreign_keys parameter. A recreation of a table drops one table
	// and renames another table, and PRAGMA foreign_keys does nothing inside a transaction.
	connection, err := sql.Open("sqlite3", TrimSQLitePrefix(path))
	if err != nil {
		return nil, err
	}

	connection.SetMaxOpenConns(1)

	migrator := &SQLiteMigrator{
		SQLMigrator: migrations.SQLMigrator{
			Connection: connection,
			Dialect:    sqliteHistoryDialect,
		},
	}

	return migrator, nil
}

func (m *SQLiteMigrator) Close() error {
	return m.Connection.Close()
}

func (m *SQLiteMigrator) SupportsTransactionalDDL() bool {
	return true
}

// A write transaction of SQLite takes the lock of the database already, and a second
// process then reads the error SQLITE_BUSY. A lock table adds a failure mode and no safety.
func (m *SQLiteMigrator) Lock(ctx context.Context) error {
	return nil
}

func (m *SQLiteMigrator) TryLock(ctx context.Context) (bool, error) {
	return true, nil
}

func (m *SQLiteMigrator) Unlock(ctx context.Context) error {
	return nil
}
