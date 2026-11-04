package driverspostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

// This value is a constant of dbdiff. Every dbdiff process takes the same lock, and no
// other application takes it.
const postgresMigrationLockKey = 7264196283401

var postgresHistoryDialect = internalmigrations.SQLDialect{
	QuoteIdentifier: driversshared.QuoteIdentifier,
	Placeholder: func(position int) string {
		return "$" + strconv.Itoa(position)
	},
	CreateHistoryTableStatement: fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"version"    text        NOT NULL PRIMARY KEY,
			"name"       text        NOT NULL,
			"checksum"   text        NOT NULL,
			"applied_at" timestamptz NOT NULL DEFAULT now(),
			"dirty"      boolean     NOT NULL DEFAULT false
		);
	`, driversshared.QuoteIdentifier(driversshared.MigrationHistoryTableName)),
	HistoryTableExistsQuery: `
		SELECT EXISTS (
			SELECT 1 FROM pg_class
			WHERE relnamespace = current_schema()::regnamespace
			AND relkind = 'r' AND relname = $1
		);
	`,
	DirtyColumnExistsQuery: `
		SELECT EXISTS (
			SELECT 1 FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			WHERE c.relnamespace = current_schema()::regnamespace
			AND c.relname = $1
			AND a.attname = 'dirty' AND NOT a.attisdropped
		);
	`,
	DirtyColumnType: "boolean",
	TrueLiteral:     "true",
	FalseLiteral:    "false",
}

type PostgresMigrator struct {
	internalmigrations.SQLMigrator

	Database *sql.DB
}

// pg_advisory_lock holds a lock of the session, so every statement runs on one connection.
// A lock that one pooled connection takes, and that another releases, does nothing.
func NewPostgresMigrator(ctx context.Context, connectionString string, schema string) (*PostgresMigrator, error) {
	database, err := OpenPostgresConnection(connectionString, schema)
	if err != nil {
		return nil, err
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		_ = database.Close()

		return nil, err
	}

	migrator := &PostgresMigrator{
		SQLMigrator: internalmigrations.SQLMigrator{
			Connection: connection,
			Dialect:    postgresHistoryDialect,
		},
		Database: database,
	}

	return migrator, nil
}

func (m *PostgresMigrator) Close() error {
	connectionError := m.Connection.Close()
	databaseError := m.Database.Close()

	return errors.Join(connectionError, databaseError)
}

func (m *PostgresMigrator) SupportsTransactionalDDL() bool {
	return true
}

func (m *PostgresMigrator) Lock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT pg_advisory_lock($1);", postgresMigrationLockKey)

	return err
}

func (m *PostgresMigrator) TryLock(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1);", postgresMigrationLockKey)

	locked := false

	err := row.Scan(&locked)
	if err != nil {
		return false, err
	}

	return locked, nil
}

func (m *PostgresMigrator) Unlock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT pg_advisory_unlock($1);", postgresMigrationLockKey)

	return err
}
