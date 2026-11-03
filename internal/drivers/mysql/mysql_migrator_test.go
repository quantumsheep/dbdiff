package driversmysql

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

type TestingMySQLMigrator struct {
	*MySQLMigrator

	tb testing.TB
}

func NewTestMySQLMigrator(tb testing.TB) *TestingMySQLMigrator {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	adminConnection, err := OpenMySQLConnection(mysqlTestConnectionString)
	require.NoError(tb, err)

	err = adminConnection.PingContext(tb.Context())
	require.NoError(tb, err)

	database := fmt.Sprintf("migrator_%d", time.Now().UnixNano())

	_, err = adminConnection.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(database))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := adminConnection.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(database))
		require.NoError(tb, err)

		require.NoError(tb, adminConnection.Close())
	})

	migrator, err := NewMySQLMigrator(tb.Context(),
		testConnectionStringWithDatabase(tb, mysqlTestConnectionString, database))
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, migrator.Close())
	})

	return &TestingMySQLMigrator{
		MySQLMigrator: migrator,
		tb:            tb,
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

	t.Run("EnsureHistoryTableAddsTheDirtyColumnToAnOldTable", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		_, err := migrator.Connection.ExecContext(t.Context(), "CREATE TABLE `dbdiff_migrations` (\n"+
			"\t`version`    varchar(255) NOT NULL PRIMARY KEY,\n"+
			"\t`name`       text NOT NULL,\n"+
			"\t`checksum`   text NOT NULL,\n"+
			"\t`applied_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP\n"+
			");")
		require.NoError(t, err)

		_, err = migrator.Connection.ExecContext(t.Context(), "INSERT INTO `dbdiff_migrations` VALUES ('20260814101500', 'init', 'aaa', '2026-08-14 10:15:00');")
		require.NoError(t, err)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Equal(t, []migrations.AppliedMigration{
			{
				Version:   "20260814101500",
				Name:      "init",
				Checksum:  "aaa",
				AppliedAt: time.Date(2026, 8, 14, 10, 15, 0, 0, time.UTC),
				Dirty:     false,
			},
		}, appliedMigrationsInUTC(applied))

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		applied, err = migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Equal(t, []migrations.AppliedMigration{
			{
				Version:   "20260814101500",
				Name:      "init",
				Checksum:  "aaa",
				AppliedAt: time.Date(2026, 8, 14, 10, 15, 0, 0, time.UTC),
				Dirty:     false,
			},
		}, appliedMigrationsInUTC(applied))

		require.NoError(t, migrator.RecordDirty(t.Context(), &migrations.Migration{
			Version:  "20260815101500",
			Name:     "users",
			Checksum: "bbb",
		}))

		applied, err = migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 2)
		require.Equal(t, migrations.AppliedMigration{
			Version:   "20260815101500",
			Name:      "users",
			Checksum:  "bbb",
			AppliedAt: applied[1].AppliedAt,
			Dirty:     true,
		}, applied[1])
	})

	t.Run("RecordDirtyAndClearDirty", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		migration := &migrations.Migration{
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

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), "CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));"))
		require.NoError(t, transaction.Record(t.Context(), &migrations.Migration{
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

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Record(t.Context(), &migrations.Migration{
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

		set := migrations.NewMigrationSet([]*migrations.Migration{
			{
				Version:  "20260814101500",
				Name:     "init",
				Checksum: "aaa",
			},
		}, nil)

		err := migrations.RunMigrationPreview(t.Context(), migrator, set, io.Discard)
		require.EqualError(t, err,
			"the target engine commits every DDL statement at once, so preview cannot roll the pending files back")
	})

	t.Run("PreviewWithNoPendingFileReportsNoError", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)

		err := migrations.RunMigrationPreview(t.Context(), migrator, migrations.NewMigrationSet(nil, nil), io.Discard)
		require.NoError(t, err)
	})

	t.Run("UpClearsTheDirtyRowOfAWholeApply", func(t *testing.T) {
		migrator := NewTestMySQLMigrator(t)
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`));\n")

		set, err := migrations.LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, migrations.ApplyMigrations(t.Context(), migrator, set, "", io.Discard))
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

		set, err := migrations.LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = migrations.ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.EqualError(t, err, "20260814101500_init failed, and the statements before the failure stay applied. "+
			"Repair the database, and run migrate repair: Error 1050 (42S01): Table 'users' already exists")
		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.True(t, applied[0].Dirty)

		set, err = migrations.LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, migrations.MigrationDirty, set.Entries[0].State)
	})

	t.Run("SatisfiesTheMigratorInterface", func(t *testing.T) {
		var migrator migrations.Migrator = &MySQLMigrator{}
		require.NotNil(t, migrator)
		require.False(t, migrator.SupportsTransactionalDDL())
	})
}

// The driver can return the time in the local zone, so the test compares the times in UTC.
func appliedMigrationsInUTC(applied []migrations.AppliedMigration) []migrations.AppliedMigration {
	for index := range applied {
		applied[index].AppliedAt = applied[index].AppliedAt.UTC()
	}

	return applied
}
