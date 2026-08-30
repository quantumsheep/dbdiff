package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	driversmysql "github.com/quantumsheep/dbdiff/internal/drivers/mysql"
	driverssqlite "github.com/quantumsheep/dbdiff/internal/drivers/sqlite"
	"github.com/stretchr/testify/require"
)

type TestingSQLiteMigrator struct {
	*SQLiteMigrator

	tb   testing.TB
	Path string
}

func NewTestSQLiteMigrator(tb testing.TB) *TestingSQLiteMigrator {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "source.sqlite")

	migrator, err := NewSQLiteMigrator(path)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, migrator.Close())
	})

	return &TestingSQLiteMigrator{
		SQLiteMigrator: migrator,
		tb:             tb,
		Path:           path,
	}
}

func (m *TestingSQLiteMigrator) TableNames() []string {
	m.tb.Helper()

	rows, err := m.Connection.QueryContext(m.tb.Context(),
		`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;`)
	require.NoError(m.tb, err)

	defer func() { require.NoError(m.tb, rows.Close()) }()

	var names []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		require.NoError(m.tb, err)

		names = append(names, name)
	}

	require.NoError(m.tb, rows.Err())

	return names
}

const PostgresTestConnectionString = "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"

const MySQLTestConnectionString = "root:password@tcp(localhost:3306)/dbdiff"

type TestingMySQLMigrator struct {
	*MySQLMigrator

	tb       testing.TB
	database string
}

func NewTestMySQLMigrator(tb testing.TB) *TestingMySQLMigrator {
	tb.Helper()

	if os.Getenv("DBDIFF_TEST_SKIP_MYSQL") != "" {
		tb.Skip("DBDIFF_TEST_SKIP_MYSQL names no server")
	}

	adminConnection, err := driversmysql.OpenMySQLConnection(MySQLTestConnectionString)
	require.NoError(tb, err)

	err = adminConnection.PingContext(tb.Context())
	require.NoError(tb, err)

	database := fmt.Sprintf("migrator_%d", time.Now().UnixNano())

	_, err = adminConnection.ExecContext(tb.Context(),
		"CREATE DATABASE "+driversmysql.QuoteIdentifier(database))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := adminConnection.ExecContext(context.Background(),
			"DROP DATABASE "+driversmysql.QuoteIdentifier(database))
		require.NoError(tb, err)

		require.NoError(tb, adminConnection.Close())
	})

	config, err := driversmysql.ParseMySQLConnectionString(MySQLTestConnectionString)
	require.NoError(tb, err)

	config.DBName = database

	migrator, err := NewMySQLMigrator(tb.Context(), config.FormatDSN())
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, migrator.Close())
	})

	return &TestingMySQLMigrator{
		MySQLMigrator: migrator,
		tb:            tb,
		database:      database,
	}
}

func (m *TestingMySQLMigrator) TableNames() []string {
	m.tb.Helper()

	rows, err := m.Connection.QueryContext(m.tb.Context(), `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME;
	`)
	require.NoError(m.tb, err)

	defer func() { require.NoError(m.tb, rows.Close()) }()

	var names []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		require.NoError(m.tb, err)

		names = append(names, name)
	}

	require.NoError(m.tb, rows.Err())

	return names
}

type TestingPostgresMigrator struct {
	*PostgresMigrator

	tb     testing.TB
	schema string
}

func NewTestPostgresMigrator(tb testing.TB) *TestingPostgresMigrator {
	tb.Helper()

	if os.Getenv("DBDIFF_TEST_SKIP_POSTGRES") != "" {
		tb.Skip("DBDIFF_TEST_SKIP_POSTGRES names no server")
	}

	adminConnection, err := sql.Open("pgx", PostgresTestConnectionString)
	require.NoError(tb, err)

	err = adminConnection.PingContext(tb.Context())
	require.NoError(tb, err)

	schema := fmt.Sprintf("migrator_%d", time.Now().UnixNano())

	_, err = adminConnection.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", schema))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := adminConnection.ExecContext(context.Background(),
			fmt.Sprintf("DROP SCHEMA %s CASCADE", schema))
		require.NoError(tb, err)

		require.NoError(tb, adminConnection.Close())
	})

	migrator, err := NewPostgresMigrator(tb.Context(), PostgresTestConnectionString, schema)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, migrator.Close())
	})

	return &TestingPostgresMigrator{
		PostgresMigrator: migrator,
		tb:               tb,
		schema:           schema,
	}
}

func (m *TestingPostgresMigrator) Schema() string {
	return m.schema
}

func (m *TestingPostgresMigrator) TableNames() []string {
	m.tb.Helper()

	rows, err := m.Connection.QueryContext(m.tb.Context(), `
		SELECT relname
		FROM pg_class
		WHERE relnamespace = current_schema()::regnamespace AND relkind = 'r'
		ORDER BY relname;
	`)
	require.NoError(m.tb, err)

	defer func() { require.NoError(m.tb, rows.Close()) }()

	var names []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		require.NoError(m.tb, err)

		names = append(names, name)
	}

	require.NoError(m.tb, rows.Err())

	return names
}

func GenerateSQLiteMigration(tb testing.TB, target string, migrations string) []string {
	tb.Helper()

	require.NoError(tb, os.MkdirAll(migrations, 0o750))

	driver, err := driverssqlite.NewSQLiteDriver(tb.Context(), &driverssqlite.SQLiteDriverConfig{
		TargetDatabasePath: target,
		SourceDatabasePath: migrations,
	})
	require.NoError(tb, err)

	defer func() { require.NoError(tb, driver.Close()) }()

	instructions, err := driver.Diff(tb.Context())
	require.NoError(tb, err)

	moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)

	paths, err := WriteMigrationFiles(migrations, "add_users", moment, "1.4.0", instructions)
	require.NoError(tb, err)

	return paths
}
