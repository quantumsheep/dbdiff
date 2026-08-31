package migrations_test

import (
	"os"
	"path/filepath"
	"testing"

	dbdiffdrivers "github.com/quantumsheep/dbdiff/drivers"
	"github.com/quantumsheep/dbdiff/migrations"
	"github.com/stretchr/testify/require"
)

func TestMigrations(t *testing.T) {
	writeMigration := func(t *testing.T, directory string, name string, content string) {
		t.Helper()

		err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o600)
		require.NoError(t, err)
	}

	newSQLiteMigrator := func(t *testing.T) *migrations.Migrator {
		t.Helper()

		driver, err := dbdiffdrivers.NewDriver(dbdiffdrivers.SQLiteDriverName)
		require.NoError(t, err)

		return driver.Migrator()
	}

	t.Run("UpAppliesThePendingFiles", func(t *testing.T) {
		directory := t.TempDir()
		writeMigration(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		writeMigration(t, directory, "20260822143000_add_created_at.sql",
			"ALTER TABLE users ADD COLUMN created_at TEXT;\n")

		migrator := newSQLiteMigrator(t)
		target := migrations.WithTargetDataSource(
			dbdiffdrivers.NewConnectionStringDataSource(filepath.Join(t.TempDir(), "app.sqlite")),
		)

		err := migrator.Up(t.Context(), target, migrations.WithMigrationDirectory(directory))
		require.NoError(t, err)

		entries, err := migrator.Status(t.Context(), target, migrations.WithMigrationDirectory(directory))
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, "20260814101500", entries[0].Version)
		require.Equal(t, "init", entries[0].Name)
		require.Equal(t, migrations.StateApplied, entries[0].State)
		require.False(t, entries[0].AppliedAt.IsZero())
		require.Equal(t, migrations.StateApplied, entries[1].State)

		err = migrator.Up(t.Context(), target, migrations.WithMigrationDirectory(directory))
		require.NoError(t, err)
	})

	t.Run("UpStopsAtToVersion", func(t *testing.T) {
		directory := t.TempDir()
		writeMigration(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		writeMigration(t, directory, "20260822143000_add_created_at.sql",
			"ALTER TABLE users ADD COLUMN created_at TEXT;\n")

		migrator := newSQLiteMigrator(t)
		target := migrations.WithTargetDataSource(
			dbdiffdrivers.NewConnectionStringDataSource(filepath.Join(t.TempDir(), "app.sqlite")),
		)

		err := migrator.Up(t.Context(), target, migrations.WithMigrationDirectory(directory),
			migrations.WithToVersion("20260814101500"))
		require.NoError(t, err)

		entries, err := migrator.Status(t.Context(), target, migrations.WithMigrationDirectory(directory))
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, migrations.StateApplied, entries[0].State)
		require.Equal(t, migrations.StatePending, entries[1].State)
	})

	t.Run("UpRefusesAMissingOption", func(t *testing.T) {
		migrator := newSQLiteMigrator(t)

		err := migrator.Up(t.Context(), migrations.WithMigrationDirectory(t.TempDir()))
		require.ErrorContains(t, err, "WithTargetDataSource")

		err = migrator.Up(t.Context(), migrations.WithTargetDataSource(
			dbdiffdrivers.NewConnectionStringDataSource(filepath.Join(t.TempDir(), "app.sqlite")),
		))
		require.ErrorContains(t, err, "WithMigrationDirectory")
	})
}
