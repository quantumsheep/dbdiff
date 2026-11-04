package driverssqlite

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/stretchr/testify/require"
)

type TestingSQLiteMigrator struct {
	*SQLiteMigrator

	tb   testing.TB
	Path string
}

func NewTestSQLiteMigrator(tb testing.TB) *TestingSQLiteMigrator {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "current.sqlite")

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

func GenerateSQLiteMigration(tb testing.TB, desired string, directory string) []string {
	tb.Helper()

	require.NoError(tb, os.MkdirAll(directory, 0o750))

	driver := NewSQLiteDriver(&SQLiteDriverConfig{})

	instructions, err := driver.Diff(tb.Context(), driversshared.ParseDataSource(directory),
		driversshared.ParseDataSource(desired), driversshared.DiffOptions{})
	require.NoError(tb, err)

	moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)

	paths, err := internalmigrations.WriteFiles(directory, "add_users", moment, "1.4.0", instructions)
	require.NoError(tb, err)

	return paths
}
