package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/quantumsheep/dbdiff/drivers"
)

type SQLiteMigrator struct {
	Connection *sql.DB
}

func NewSQLiteMigrator(path string) (*SQLiteMigrator, error) {
	// The name holds no _foreign_keys parameter. A recreation of a table drops one table
	// and renames another table, and PRAGMA foreign_keys does nothing inside a transaction.
	connection, err := sql.Open("sqlite3", drivers.TrimSQLitePrefix(path))
	if err != nil {
		return nil, err
	}

	connection.SetMaxOpenConns(1)

	migrator := &SQLiteMigrator{
		Connection: connection,
	}

	return migrator, nil
}

func (m *SQLiteMigrator) Close() error {
	return m.Connection.Close()
}

func (m *SQLiteMigrator) EnsureHistoryTable(ctx context.Context) error {
	statement := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"version"    TEXT NOT NULL PRIMARY KEY,
			"name"       TEXT NOT NULL,
			"checksum"   TEXT NOT NULL,
			"applied_at" TEXT NOT NULL,
			"dirty"      INTEGER NOT NULL DEFAULT 0
		);
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement)
	if err != nil {
		return err
	}

	return m.ensureDirtyColumn(ctx)
}

// An older dbdiff created the table without the dirty column.
func (m *SQLiteMigrator) ensureDirtyColumn(ctx context.Context) error {
	found, err := m.dirtyColumnExists(ctx)
	if err != nil {
		return err
	}

	if found {
		return nil
	}

	statement := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "dirty" INTEGER NOT NULL DEFAULT 0;`,
		drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err = m.Connection.ExecContext(ctx, statement)

	return err
}

func (m *SQLiteMigrator) dirtyColumnExists(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pragma_table_info(?) WHERE name = 'dirty');`,
		drivers.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (m *SQLiteMigrator) AppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	found, err := m.historyTableExists(ctx)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	// A read must stay a read, so an old table keeps its columns and the query selects
	// a constant in the place of the absent one.
	dirtyColumn := `"dirty"`

	dirtyFound, err := m.dirtyColumnExists(ctx)
	if err != nil {
		return nil, err
	}

	if !dirtyFound {
		dirtyColumn = "0"
	}

	statement := fmt.Sprintf(`
		SELECT "version", "name", "checksum", "applied_at", %s
		FROM %s
		ORDER BY "version";
	`, dirtyColumn, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	rows, err := m.Connection.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var applied []AppliedMigration

	for rows.Next() {
		var row AppliedMigration
		var appliedAt string

		err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &appliedAt, &row.Dirty)
		if err != nil {
			return nil, err
		}

		row.AppliedAt, err = time.Parse(time.RFC3339, appliedAt)
		if err != nil {
			return nil, err
		}

		applied = append(applied, row)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return applied, nil
}

func (m *SQLiteMigrator) historyTableExists(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?);`,
		drivers.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
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

func (m *SQLiteMigrator) Begin(ctx context.Context, useTransaction bool) (MigrationTransaction, error) {
	if !useTransaction {
		return &SQLiteMigrationTransaction{
			Executor: m.Connection,
		}, nil
	}

	transaction, err := m.Connection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &SQLiteMigrationTransaction{
		Executor:    transaction,
		Transaction: transaction,
	}, nil
}

type SQLiteMigrationTransaction struct {
	Executor migrationExecutor

	Transaction *sql.Tx
}

func (t *SQLiteMigrationTransaction) Apply(ctx context.Context, statement string) error {
	_, err := t.Executor.ExecContext(ctx, statement)

	return err
}

func (t *SQLiteMigrationTransaction) Record(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf(`
		INSERT INTO %s ("version", "name", "checksum", "applied_at")
		VALUES (?, ?, ?, ?);
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := t.Executor.ExecContext(ctx, statement,
		migration.Version, migration.Name, migration.Checksum, time.Now().UTC().Format(time.RFC3339))

	return err
}

func (m *SQLiteMigrator) RecordDirty(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf(`
		INSERT INTO %s ("version", "name", "checksum", "applied_at", "dirty")
		VALUES (?, ?, ?, ?, 1);
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement,
		migration.Version, migration.Name, migration.Checksum, time.Now().UTC().Format(time.RFC3339))

	return err
}

func (m *SQLiteMigrator) ClearDirty(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf(`UPDATE %s SET "dirty" = 0 WHERE "version" = ?;`,
		drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement, migration.Version)

	return err
}

func (t *SQLiteMigrationTransaction) Commit() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Commit()
}

func (t *SQLiteMigrationTransaction) Rollback() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Rollback()
}
