package driversmysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

// A lock name is unique in the server, not in the database, so the name holds the
// database. MySQL refuses a lock name longer than 64 characters, so the name holds a hash
// of the database name.
const mysqlMigrationLockNameExpression = "CONCAT('dbdiff_migrations_', MD5(DATABASE()))"

var errMySQLMigrationLockNotTaken = errors.New("the server did not give the migration lock")

var mysqlHistoryDialect = internalmigrations.SQLDialect{
	BackslashEscapes: true,
	QuoteIdentifier:  QuoteIdentifier,
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
	internalmigrations.SQLMigrator

	Database *sql.DB
}

// GET_LOCK holds a lock of the session, so every statement runs on one connection. A lock
// that one pooled connection takes, and that another releases, does nothing.
func NewMySQLMigrator(ctx context.Context, connectionString string) (*MySQLMigrator, error) {
	config, err := ParseMySQLConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	// The history table holds DATETIME values, and the scan of applied_at needs time.Time.
	// The diff connections read text instead, so a zero date keeps its value.
	config.ParseTime = true

	database, err := openMySQLConnectionWithConfig(config)
	if err != nil {
		return nil, err
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		_ = database.Close()

		return nil, err
	}

	migrator := &MySQLMigrator{
		SQLMigrator: internalmigrations.SQLMigrator{
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

	return errors.Join(connectionError, databaseError)
}

// MySQL commits every DDL statement at once, so a transaction rolls no schema change back.
func (m *MySQLMigrator) SupportsTransactionalDDL() bool {
	return false
}

// GET_LOCK gives 1 when the session takes the lock, 0 when the time limit ends, and NULL
// when an error occurs.
func (m *MySQLMigrator) Lock(ctx context.Context) error {
	result, err := m.lockFunctionResult(ctx, "SELECT GET_LOCK("+mysqlMigrationLockNameExpression+", -1);")
	if err != nil {
		return err
	}

	if result.Int64 != 1 {
		return errMySQLMigrationLockNotTaken
	}

	return nil
}

func (m *MySQLMigrator) TryLock(ctx context.Context) (bool, error) {
	result, err := m.lockFunctionResult(ctx, "SELECT GET_LOCK("+mysqlMigrationLockNameExpression+", 0);")
	if err != nil {
		return false, err
	}

	if !result.Valid {
		return false, errMySQLMigrationLockNotTaken
	}

	return result.Int64 == 1, nil
}

// RELEASE_LOCK gives 0 when another session holds the lock, and NULL when no session holds it.
func (m *MySQLMigrator) Unlock(ctx context.Context) error {
	result, err := m.lockFunctionResult(ctx, "SELECT RELEASE_LOCK("+mysqlMigrationLockNameExpression+");")
	if err != nil {
		return err
	}

	if result.Int64 != 1 {
		return errors.New("this session does not hold the migration lock")
	}

	return nil
}

func (m *MySQLMigrator) lockFunctionResult(ctx context.Context, query string) (sql.NullInt64, error) {
	var result sql.NullInt64

	err := m.Connection.QueryRowContext(ctx, query).Scan(&result)

	return result, err
}
