package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	driversmysql "github.com/quantumsheep/dbdiff/internal/drivers/mysql"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// This name is a constant of dbdiff. Every dbdiff process takes the same lock, and no
// other application takes it.
const mysqlMigrationLockName = "dbdiff_migrations"

type MySQLMigrator struct {
	Database   *sql.DB
	Connection *sql.Conn
}

// GET_LOCK holds a lock of the session, so every statement runs on one connection. A lock
// that one pooled connection takes, and that another releases, does nothing.
func NewMySQLMigrator(ctx context.Context, connectionString string) (*MySQLMigrator, error) {
	database, err := driversmysql.OpenMySQLConnection(connectionString)
	if err != nil {
		return nil, err
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		_ = database.Close()

		return nil, err
	}

	migrator := &MySQLMigrator{
		Database:   database,
		Connection: connection,
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

func (m *MySQLMigrator) EnsureHistoryTable(ctx context.Context) error {
	statement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"\t`version`    varchar(255) NOT NULL PRIMARY KEY,\n"+
		"\t`name`       text NOT NULL,\n"+
		"\t`checksum`   text NOT NULL,\n"+
		"\t`applied_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
		"\t`dirty`      tinyint(1) NOT NULL DEFAULT 0\n"+
		");", driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement)
	if err != nil {
		return err
	}

	return m.ensureDirtyColumn(ctx)
}

// An older dbdiff created the table without the dirty column.
func (m *MySQLMigrator) ensureDirtyColumn(ctx context.Context) error {
	found, err := m.dirtyColumnExists(ctx)
	if err != nil {
		return err
	}

	if found {
		return nil
	}

	statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN `dirty` tinyint(1) NOT NULL DEFAULT 0;",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err = m.Connection.ExecContext(ctx, statement)

	return err
}

func (m *MySQLMigrator) dirtyColumnExists(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'dirty'
		);
	`, driversshared.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (m *MySQLMigrator) historyTableExists(ctx context.Context) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		);
	`, driversshared.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
}

// A read must stay a read, so this method creates no table. The command status calls it
// against a database that never ran a migration.
func (m *MySQLMigrator) AppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	found, err := m.historyTableExists(ctx)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	// A read must stay a read, so an old table keeps its columns and the query selects
	// a constant in the place of the absent one.
	dirtyColumn := "`dirty`"

	dirtyFound, err := m.dirtyColumnExists(ctx)
	if err != nil {
		return nil, err
	}

	if !dirtyFound {
		dirtyColumn = "0"
	}

	statement := fmt.Sprintf("SELECT `version`, `name`, `checksum`, `applied_at`, %s FROM %s ORDER BY `version`;",
		dirtyColumn, driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	rows, err := m.Connection.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var applied []AppliedMigration

	for rows.Next() {
		var row AppliedMigration

		err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.AppliedAt, &row.Dirty)
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

func (m *MySQLMigrator) RecordDirty(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("INSERT INTO %s (`version`, `name`, `checksum`, `applied_at`, `dirty`) VALUES (?, ?, ?, ?, 1);",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement,
		migration.Version, migration.Name, migration.Checksum, time.Now().UTC())

	return err
}

func (m *MySQLMigrator) ClearDirty(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("UPDATE %s SET `dirty` = 0 WHERE `version` = ?;",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement, migration.Version)

	return err
}

func (m *MySQLMigrator) UpdateChecksum(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("UPDATE %s SET `checksum` = ? WHERE `version` = ?;",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement, migration.Checksum, migration.Version)

	return err
}

func (m *MySQLMigrator) DeleteRecord(ctx context.Context, version string) error {
	statement := fmt.Sprintf("DELETE FROM %s WHERE `version` = ?;",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := m.Connection.ExecContext(ctx, statement, version)

	return err
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

func (m *MySQLMigrator) Begin(ctx context.Context, useTransaction bool) (MigrationTransaction, error) {
	if !useTransaction {
		return &MySQLMigrationTransaction{
			Executor: m.Connection,
		}, nil
	}

	transaction, err := m.Connection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &MySQLMigrationTransaction{
		Executor:    transaction,
		Transaction: transaction,
	}, nil
}

type MySQLMigrationTransaction struct {
	Executor migrationExecutor

	// This field is nil when the file holds the no-transaction directive. Commit and
	// Rollback then do nothing, so the apply loop holds one path.
	Transaction *sql.Tx
}

func (t *MySQLMigrationTransaction) Apply(ctx context.Context, statement string) error {
	_, err := t.Executor.ExecContext(ctx, statement)

	return err
}

func (t *MySQLMigrationTransaction) Record(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("INSERT INTO %s (`version`, `name`, `checksum`, `applied_at`) VALUES (?, ?, ?, ?);",
		driversmysql.QuoteIdentifier(driversshared.MigrationHistoryTableName))

	_, err := t.Executor.ExecContext(ctx, statement,
		migration.Version, migration.Name, migration.Checksum, time.Now().UTC())

	return err
}

func (t *MySQLMigrationTransaction) Commit() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Commit()
}

func (t *MySQLMigrationTransaction) Rollback() error {
	if t.Transaction == nil {
		return nil
	}

	return t.Transaction.Rollback()
}
