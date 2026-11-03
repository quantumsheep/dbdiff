package driverspostgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

type TestingPostgresMigrator struct {
	*PostgresMigrator

	tb     testing.TB
	schema string
}

func NewTestPostgresMigrator(tb testing.TB) *TestingPostgresMigrator {
	tb.Helper()

	skipWithoutPostgresServer(tb)

	adminConnection, err := sql.Open("pgx", postgresTestConnectionString)
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

	migrator, err := NewPostgresMigrator(tb.Context(), postgresTestConnectionString, schema)
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

	t.Run("TryLockReportsALockOfAnotherSession", func(t *testing.T) {
		first := NewTestPostgresMigrator(t)
		second := NewTestPostgresMigrator(t)

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
		migrator := NewTestPostgresMigrator(t)

		_, err := migrator.Connection.ExecContext(t.Context(), `
			CREATE TABLE "dbdiff_migrations" (
				"version"    text        NOT NULL PRIMARY KEY,
				"name"       text        NOT NULL,
				"checksum"   text        NOT NULL,
				"applied_at" timestamptz NOT NULL DEFAULT now()
			);
		`)
		require.NoError(t, err)

		_, err = migrator.Connection.ExecContext(t.Context(), `
			INSERT INTO "dbdiff_migrations" VALUES ('20260814101500', 'init', 'aaa', '2026-08-14T10:15:00Z');
		`)
		require.NoError(t, err)

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Equal(t, []internalmigrations.AppliedMigration{
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
		require.Equal(t, []internalmigrations.AppliedMigration{
			{
				Version:   "20260814101500",
				Name:      "init",
				Checksum:  "aaa",
				AppliedAt: time.Date(2026, 8, 14, 10, 15, 0, 0, time.UTC),
				Dirty:     false,
			},
		}, appliedMigrationsInUTC(applied))

		require.NoError(t, migrator.RecordDirty(t.Context(), &internalmigrations.Migration{
			Version:  "20260815101500",
			Name:     "users",
			Checksum: "bbb",
		}))

		applied, err = migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 2)
		require.Equal(t, internalmigrations.AppliedMigration{
			Version:   "20260815101500",
			Name:      "users",
			Checksum:  "bbb",
			AppliedAt: applied[1].AppliedAt,
			Dirty:     true,
		}, applied[1])
	})

	t.Run("RecordDirtyAndClearDirty", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

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

	t.Run("LockAndUnlock", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

		require.NoError(t, migrator.Lock(t.Context()))
		require.NoError(t, migrator.Unlock(t.Context()))
	})

	t.Run("CommitWritesTheStatementAndTheRow", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))
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
		require.False(t, applied[0].AppliedAt.IsZero())
	})

	t.Run("RollbackWritesNothing", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		transaction, err := migrator.Begin(t.Context())
		require.NoError(t, err)

		require.NoError(t, transaction.Apply(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))
		require.NoError(t, transaction.Record(t.Context(), &internalmigrations.Migration{
			Version:  "20260814101500",
			Name:     "init",
			Checksum: "aaa",
		}))
		require.NoError(t, transaction.Rollback())

		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("SatisfiesTheMigratorInterface", func(t *testing.T) {
		var migrator internalmigrations.Migrator = &PostgresMigrator{}
		require.NotNil(t, migrator)
	})

	t.Run("ExecRunsTheStatementOutsideATransaction", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)
		require.NoError(t, migrator.EnsureHistoryTable(t.Context()))

		require.NoError(t, migrator.Exec(t.Context(), `CREATE TABLE "users" ("id" integer PRIMARY KEY);`))

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())
	})

	t.Run("NoTransactionFile", func(t *testing.T) {
		migrator := NewTestPostgresMigrator(t)

		_, err := migrator.Connection.ExecContext(t.Context(),
			`CREATE TABLE users (id INT, email TEXT, username TEXT);`)
		require.NoError(t, err)

		directory := t.TempDir()
		sqltest.WriteSQLFile(t, directory, "20260101000000_concurrent_index.sql",
			`-- dbdiff:no-transaction
	DROP INDEX CONCURRENTLY IF EXISTS ix_users_email;
	ALTER TABLE users ALTER COLUMN username SET NOT NULL;
	CREATE INDEX CONCURRENTLY ix_users_email ON users (email);
	`)

		set, err := internalmigrations.LoadSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}

		runner := &internalmigrations.Runner{
			Migrator: migrator,
			Set:      set,
			Output:   output,
		}

		err = runner.Up(t.Context(), "")
		require.NoError(t, err)
		require.Contains(t, output.String(), "Applied 20260101000000_concurrent_index.")
		require.Contains(t, migrator.TableNames(), "users")

		set, err = internalmigrations.LoadSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, internalmigrations.StateApplied, set.Entries[0].State)
	})

	t.Run("GenerateThenApplyLeavesNoDiff", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrator := NewTestPostgresMigrator(t)

		scratchVersion := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.NotEmpty(t, scratchVersion)

		directory := t.TempDir()

		schemaFile := sqltest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL);\n"+
				"CREATE INDEX users_email ON users (email);\n")

		migrationsDirectory := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrationsDirectory, 0o750))

		generateDriver := NewPostgresDriver(&PostgresDriverConfig{
			ScratchServerVersion: string(scratchVersion),
		})

		generatedInstructions, err := generateDriver.Diff(t.Context(), driversshared.ParseDataSource(migrationsDirectory),
			driversshared.ParseDataSource(schemaFile), driversshared.DiffOptions{})
		require.NoError(t, err)

		paths, err := internalmigrations.WriteFiles(migrationsDirectory, "add_users", time.Now(), "test", generatedInstructions)
		require.NoError(t, err)
		require.NotEmpty(t, paths)

		set, err := internalmigrations.LoadSet(t.Context(), migrator, migrationsDirectory)
		require.NoError(t, err)
		runner := &internalmigrations.Runner{
			Migrator: migrator,
			Set:      set,
			Output:   io.Discard,
		}

		require.NoError(t, runner.Up(t.Context(), ""))

		sourceConnectionString := fmt.Sprintf("%s&search_path=%s", postgresTestConnectionString, migrator.schema)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		instructions, err := driver.Diff(t.Context(), driversshared.ParseDataSource(sourceConnectionString),
			driversshared.ParseDataSource(schemaFile), driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)
	})
}

// The driver can return the time in the local zone, so the test compares the times in UTC.
func appliedMigrationsInUTC(applied []internalmigrations.AppliedMigration) []internalmigrations.AppliedMigration {
	for index := range applied {
		applied[index].AppliedAt = applied[index].AppliedAt.UTC()
	}

	return applied
}
