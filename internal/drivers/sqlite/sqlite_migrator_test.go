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

func TestSQLiteMigrator(t *testing.T) {
	t.Run("AppliedMigrationsWithNoHistoryTable", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
		require.Empty(t, migrator.TableNames())
	})

	t.Run("EnsureHistoryTableRunsTwice", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("LockAndUnlock", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		require.NoError(t, migrator.Lock(t.Context()))

		locked, err := migrator.TryLock(t.Context())
		require.NoError(t, err)
		require.True(t, locked)

		require.NoError(t, migrator.Unlock(t.Context()))
	})

	t.Run("RecordDirtyAndClearDirty", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		migration := &internalmigrations.Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}

		require.NoError(t, migrator.RecordDirty(t.Context(), migration))

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.True(t, applied[0].Dirty)

		require.NoError(t, migrator.ClearDirty(t.Context(), migration))

		applied, err = migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.False(t, applied[0].Dirty)
	})

	t.Run("EnsureHistoryTableAddsTheDirtyColumnToAnOldTable", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		_, err := migrator.Connection.ExecContext(t.Context(), `
			CREATE TABLE "dbdiff_migrations" (
				"version"    TEXT NOT NULL PRIMARY KEY,
				"name"       TEXT NOT NULL,
				"checksum"   TEXT NOT NULL,
				"applied_at" TEXT NOT NULL
			);
			INSERT INTO "dbdiff_migrations" VALUES ('20260814101500', 'init', 'aaa', '2026-08-14T10:15:00Z');
		`)
		require.NoError(t, err)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.False(t, applied[0].Dirty)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		applied, err = migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.False(t, applied[0].Dirty)
	})

	t.Run("CommitWritesTheStatementAndTheRow", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" INTEGER PRIMARY KEY);`))
		require.NoError(t, transaction.Record(t.Context(), &internalmigrations.Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}))
		require.NoError(t, transaction.Commit())

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.Equal(t, "20260814101500", applied[0].Version)
		require.Equal(t, "init", applied[0].Name)
		require.Equal(t, "aaa", applied[0].Checksum)
		require.False(t, applied[0].AppliedAt.IsZero())
	})

	t.Run("RollbackWritesNothing", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" INTEGER PRIMARY KEY);`))
		require.NoError(t, transaction.Record(t.Context(), &internalmigrations.Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}))
		require.NoError(t, transaction.Rollback())

		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
	})

	t.Run("ApplyReportsABadStatement", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		defer func() { require.NoError(t, transaction.Rollback()) }()

		require.Error(t, transaction.Apply(t.Context(), "NOT SQL AT ALL;"))
	})

	t.Run("SatisfiesTheMigratorInterface", func(t *testing.T) {
		var migrator internalmigrations.Migrator = &SQLiteMigrator{}
		require.NotNil(t, migrator)
	})

	t.Run("ExecRunsTheStatementOutsideATransaction", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		require.NoError(t, migrator.Exec(t.Context(), `CREATE TABLE "users" ("id" INTEGER PRIMARY KEY);`))

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())
	})

	t.Run("ApplyRecreatesAReferencedTableWhenTheNameTurnsTheForeignKeysOn", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		desiredPath := filepath.Join(directory, "desired.sql")

		migrator, err := NewSQLiteMigrator(currentPath + "?_pragma=foreign_keys(1)")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, migrator.Close())
		})

		_, err = migrator.Connection.ExecContext(t.Context(), `
			CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents (id) ON DELETE CASCADE);

			INSERT INTO parents (id, name) VALUES (1, 'Ada');
			INSERT INTO children (id, parent_id) VALUES (1, 1);
		`)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(desiredPath, []byte(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT '');
			CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents (id) ON DELETE CASCADE);
		`), 0o600))

		instructions, err := NewSQLiteDriver(&SQLiteDriverConfig{}).Diff(t.Context(),
			driversshared.ParseDataSource(currentPath), driversshared.ParseDataSource(desiredPath),
			driversshared.DiffOptions{})
		require.NoError(t, err)

		content := internalmigrations.RenderFile("1.4.0", time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC), instructions)

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)
		require.NoError(t, transaction.Apply(t.Context(), content))
		require.NoError(t, transaction.Commit())

		var childCount int

		err = migrator.Connection.QueryRowContext(t.Context(), `SELECT count(*) FROM children;`).Scan(&childCount)
		require.NoError(t, err)
		require.Equal(t, 1, childCount)
	})
}
