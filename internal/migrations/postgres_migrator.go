package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/quantumsheep/dbdiff/drivers"
)

// This value is a constant of dbdiff. Every dbdiff process takes the same lock, and no
// other application takes it.
const postgresMigrationLockKey = 7264196283401

type PostgresMigrator struct {
	Database   *sql.DB
	Connection *sql.Conn
}

// pg_advisory_lock holds a lock of the session, so every statement runs on one connection.
// A lock that one pooled connection takes, and that another releases, does nothing.
func NewPostgresMigrator(ctx context.Context, connectionString string, schema string) (*PostgresMigrator, error) {
	database, err := drivers.OpenPostgresConnection(connectionString, schema)
	if err != nil {
		return nil, err
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		_ = database.Close()

		return nil, err
	}

	migrator := &PostgresMigrator{
		Database:   database,
		Connection: connection,
	}

	return migrator, nil
}

func (m *PostgresMigrator) Close() error {
	connectionError := m.Connection.Close()
	databaseError := m.Database.Close()

	return drivers.FirstError(connectionError, databaseError)
}

func (m *PostgresMigrator) EnsureHistoryTable(ctx context.Context) error {
	statement := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"version"    text        NOT NULL PRIMARY KEY,
			"name"       text        NOT NULL,
			"checksum"   text        NOT NULL,
			"applied_at" timestamptz NOT NULL DEFAULT now()
		);
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement)

	return err
}

// A read must stay a read, so this method creates no table. The command status calls it
// against a database that never ran a migration.
func (m *PostgresMigrator) AppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	found, err := m.historyTableExists(ctx)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	statement := fmt.Sprintf(`
		SELECT "version", "name", "checksum", "applied_at"
		FROM %s
		ORDER BY "version";
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	rows, err := m.Connection.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var applied []AppliedMigration

	for rows.Next() {
		var row AppliedMigration

		err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.AppliedAt)
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

func (m *PostgresMigrator) historyTableExists(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class
			WHERE relnamespace = current_schema()::regnamespace
			AND relkind = 'r' AND relname = $1
		);
	`, drivers.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (m *PostgresMigrator) Lock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT pg_advisory_lock($1);", postgresMigrationLockKey)

	return err
}

func (m *PostgresMigrator) Unlock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT pg_advisory_unlock($1);", postgresMigrationLockKey)

	return err
}

func (m *PostgresMigrator) Begin(ctx context.Context, useTransaction bool) (MigrationTransaction, error) {
	if !useTransaction {
		return &PostgresMigrationTransaction{
			Executor: m.Connection,
		}, nil
	}

	transaction, err := m.Connection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &PostgresMigrationTransaction{
		Executor:    transaction,
		Transaction: transaction,
	}, nil
}

type PostgresMigrationTransaction struct {
	Executor migrationExecutor

	// This field is nil when the file holds the no-transaction directive. Commit and
	// Rollback then do nothing, so the apply loop holds one path.
	Transaction *sql.Tx
}

func (t *PostgresMigrationTransaction) Apply(ctx context.Context, statement string) error {
	_, err := t.Executor.ExecContext(ctx, statement)

	return err
}

func (t *PostgresMigrationTransaction) Record(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf(`
		INSERT INTO %s ("version", "name", "checksum", "applied_at")
		VALUES ($1, $2, $3, $4);
	`, drivers.QuoteIdentifier(drivers.MigrationHistoryTableName))

	_, err := t.Executor.ExecContext(ctx, statement,
		migration.Version, migration.Name, migration.Checksum, time.Now().UTC())

	return err
}

func (t *PostgresMigrationTransaction) Commit() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Commit()
}

func (t *PostgresMigrationTransaction) Rollback() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Rollback()
}
