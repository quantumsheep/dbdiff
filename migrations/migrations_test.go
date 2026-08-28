package migrations

import (
	"os"
	"path/filepath"
	"testing"

	dbdiffdrivers "github.com/quantumsheep/dbdiff/drivers"
	"github.com/stretchr/testify/require"
)

func TestMigrations(t *testing.T) {
	writeMigration := func(t *testing.T, directory string, name string, content string) {
		t.Helper()

		err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o600)
		require.NoError(t, err)
	}

	t.Run("UpAppliesThePendingFiles", func(t *testing.T) {
		directory := t.TempDir()
		writeMigration(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		writeMigration(t, directory, "20260822143000_add_created_at.sql",
			"ALTER TABLE users ADD COLUMN created_at TEXT;\n")

		database := WithDatabase(dbdiffdrivers.SQLiteDriverName, filepath.Join(t.TempDir(), "app.sqlite"))

		err := Up(t.Context(), database, WithDirectory(directory))
		require.NoError(t, err)

		entries, err := Status(t.Context(), database, WithDirectory(directory))
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, "20260814101500", entries[0].Version)
		require.Equal(t, "init", entries[0].Name)
		require.Equal(t, StateApplied, entries[0].State)
		require.False(t, entries[0].AppliedAt.IsZero())
		require.Equal(t, StateApplied, entries[1].State)

		err = Up(t.Context(), database, WithDirectory(directory))
		require.NoError(t, err)
	})

	t.Run("UpStopsAtToVersion", func(t *testing.T) {
		directory := t.TempDir()
		writeMigration(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		writeMigration(t, directory, "20260822143000_add_created_at.sql",
			"ALTER TABLE users ADD COLUMN created_at TEXT;\n")

		database := WithDatabase("", filepath.Join(t.TempDir(), "app.sqlite"))

		err := Up(t.Context(), database, WithDirectory(directory), WithToVersion("20260814101500"))
		require.NoError(t, err)

		entries, err := Status(t.Context(), database, WithDirectory(directory))
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, StateApplied, entries[0].State)
		require.Equal(t, StatePending, entries[1].State)
	})

	t.Run("UpRefusesAMissingOption", func(t *testing.T) {
		err := Up(t.Context(), WithDirectory(t.TempDir()))
		require.ErrorContains(t, err, "WithDatabase")

		err = Up(t.Context(), WithDatabase("", filepath.Join(t.TempDir(), "app.sqlite")))
		require.ErrorContains(t, err, "WithDirectory")
	})
}
