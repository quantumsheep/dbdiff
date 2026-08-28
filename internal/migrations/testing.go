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
	"github.com/quantumsheep/dbdiff/drivers"
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

func WriteSQLFile(tb testing.TB, directory string, name string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, name)

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

func GenerateSQLiteMigration(tb testing.TB, target string, migrations string) []string {
	tb.Helper()

	require.NoError(tb, os.MkdirAll(migrations, 0o750))

	driver, err := drivers.NewSQLiteDriver(tb.Context(), &drivers.SQLLiteDriverConfig{
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
