package driversmysql

import (
	"context"
	"database/sql"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/migrations"
)

// This name is a constant of dbdiff. Every dbdiff process takes the same lock, and no
// other application takes it.
const mysqlMigrationLockName = "dbdiff_migrations"

var mysqlHistoryDialect = migrations.SQLDialect{
	QuoteIdentifier: QuoteIdentifier,
	Placeholder: func(int) string {
		return "?"
	},
	CreateHistoryTableStatement: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"\t`version`    varchar(255) NOT NULL PRIMARY KEY,\n"+
		"\t`name`       text NOT NULL,\n"+
		"\t`checksum`   text NOT NULL,\n"+
		"\t`applied_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
		"\t`dirty`      tinyint(1) NOT NULL DEFAULT 0\n"+
		");", QuoteIdentifier(driversshared.MigrationHistoryTableName)),
	HistoryTableExistsQuery: `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		);
	`,
	DirtyColumnExistsQuery: `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'dirty'
		);
	`,
	DirtyColumnType: "tinyint(1)",
	TrueLiteral:     "1",
	FalseLiteral:    "0",
}

type MySQLMigrator struct {
	migrations.SQLMigrator

	Database *sql.DB
}

// GET_LOCK holds a lock of the session, so every statement runs on one connection. A lock
// that one pooled connection takes, and that another releases, does nothing.
func NewMySQLMigrator(ctx context.Context, connectionString string) (*MySQLMigrator, error) {
	database, err := OpenMySQLConnection(connectionString)
	if err != nil {
		return nil, err
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		_ = database.Close()

		return nil, err
	}

	migrator := &MySQLMigrator{
		SQLMigrator: migrations.SQLMigrator{
			Connection: connection,
			Dialect:    mysqlHistoryDialect,
		},
		Database: database,
	}

	return migrator, nil
}

func (m *MySQLMigrator) Close() error {
	connectionError := m.Connection.Close()
	databaseError := m.Database.Close()

	return driversshared.FirstError(connectionError, databaseError)
}

// MySQL commits every DDL statement at once, so a transaction rolls no schema change back.
func (m *MySQLMigrator) SupportsTransactionalDDL() bool {
	return false
}

func (m *MySQLMigrator) Lock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT GET_LOCK(?, -1);", mysqlMigrationLockName)

	return err
}

func (m *MySQLMigrator) TryLock(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0);", mysqlMigrationLockName)

	locked := false

	err := row.Scan(&locked)
	if err != nil {
		return false, err
	}

	return locked, nil
}

func (m *MySQLMigrator) Unlock(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, "SELECT RELEASE_LOCK(?);", mysqlMigrationLockName)

	return err
}
