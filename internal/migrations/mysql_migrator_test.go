package migrations

import (
	"io"
	"testing"

	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMySQLMigrator(t *testing.T) {
	t.Run("AppliedMigrationsWithNoHistoryTable", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
		require.Empty(t, migrator.TableNames())
	})

	t.Run("EnsureHistoryTableRunsTwice", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("TryLockReportsALockOfAnotherSession", func(t *testing.T) {
		first := NewTestMySQLMigrator(t)
		second := NewTestMySQLMigrator(t)

		require.NoError(t, first.Lock(t.Context()))

		locked, err := second.TryLock(t.Context())
		require.NoError(t, err)
		require.False(t, locked)

		require.NoError(t, first.Unlock(t.Context()))

		locked, err = second.TryLock(t.Context())
		require.NoError(t, err)
		require.True(t, locked)

		require.NoError(t, second.Unlock(t.Context()))
	})

	t.Run("RecordDirtyAndClearDirty", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		migration := &Migration{
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

	t.Run("CommitWritesTheStatementAndTheRow", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context(), true)
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), "CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));"))
		require.NoError(t, transaction.Record(t.Context(), &Migration{
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
		require.False(t, applied[0].AppliedAt.IsZero())
	})

	t.Run("RollbackWritesNoRow", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context(), true)
		require.NoError(t, err)

		require.NoError(t, transaction.Record(t.Context(), &Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}))
		require.NoError(t, transaction.Rollback())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
	})

	t.Run("PreviewRefusesTheEngine", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		set := NewMigrationSet([]*Migration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		}, nil)

		err := RunMigrationPreview(t.Context(), migrator, set, io.Discard)
		require.EqualError(t, err,
			"the target engine commits every DDL statement at once, so preview cannot roll the pending files back")
	})

	t.Run("PreviewWithNoPendingFileReportsNoError", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		err := RunMigrationPreview(t.Context(), migrator, NewMigrationSet(nil, nil), io.Discard)
		require.NoError(t, err)
	})

	t.Run("UpClearsTheDirtyRowOfAWholeApply", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))
		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.False(t, applied[0].Dirty)
	})

	t.Run("UpMarksAHalfApplyWithTheDirtyRow", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));\n"+
				"CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.EqualError(t, err, "20260814101500_init failed, and the statements before the failure stay applied. "+
			"Repair the database, and run migrate repair: Error 1050 (42S01): Table 'users' already exists")
		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.True(t, applied[0].Dirty)

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationDirty, set.Entries[0].State)
	})

	t.Run("SatisfiesTheMigratorInterface", func(t *testing.T) {
		var migrator Migrator = &MySQLMigrator{}
		require.NotNil(t, migrator)
		require.False(t, migrator.SupportsTransactionalDDL())
	})
}
