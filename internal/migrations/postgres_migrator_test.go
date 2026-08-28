package migrations

import (
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func TestPostgresMigrator(t *testing.T) {
	t.Run("AppliedMigrationsWithNoHistoryTable", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
		require.Empty(t, migrator.TableNames())
	})

	t.Run("EnsureHistoryTableRunsTwice", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))
		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("LockAndUnlock", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

		require.NoError(t, migrator.Lock(t.Context()))
		require.NoError(t, migrator.Unlock(t.Context()))
	})

	t.Run("CommitWritesTheStatementAndTheRow", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context(), true)
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))
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

	t.Run("RollbackWritesNothing", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context(), true)
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))
		require.NoError(t, transaction.Record(t.Context(), &Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}))
		require.NoError(t, transaction.Rollback())

		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("SatisfiesTheMigratorInterface", func(t *testing.T) {
		var migrator Migrator = &PostgresMigrator{}
		require.NotNil(t, migrator)
	})

	t.Run("BeginWithNoTransactionCommitsEachStatement", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context(), false)
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))
		require.NoError(t, transaction.Rollback())

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())
	})
}
