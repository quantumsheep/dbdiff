package migrations_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	driverspostgres "github.com/quantumsheep/dbdiff/internal/drivers/postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	driverssqlite "github.com/quantumsheep/dbdiff/internal/drivers/sqlite"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestGenerateMigration(t *testing.T) {
	t.Run("WriteTheFirstMigration", func(t *testing.T) {
		directory := t.TempDir()

		target := sqltest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		paths := driverssqlite.GenerateSQLiteMigration(t, target, migrations)
		require.Len(t, paths, 1)
		require.Equal(t, filepath.Join(migrations, "20260822143000_add_users.sql"), paths[0])

		content, err := os.ReadFile(paths[0])
		require.NoError(t, err)
		require.Contains(t, string(content), "-- dbdiff 1.4.0")
		require.Contains(t, string(content), `CREATE TABLE "users"`)
	})

	t.Run("TheSecondRunOfOneSecondTakesTheNextVersion", func(t *testing.T) {
		directory := t.TempDir()
		moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)

		first, err := coremigrations.WriteMigrationFiles(directory, "one", moment, "test",
			[]driversshared.Instruction{&driversshared.SQLDropTableInstruction{Name: "users"}})
		require.NoError(t, err)

		second, err := coremigrations.WriteMigrationFiles(directory, "two", moment, "test",
			[]driversshared.Instruction{&driversshared.SQLDropTableInstruction{Name: "notes"}})
		require.NoError(t, err)

		require.Equal(t, filepath.Join(directory, "20260822143000_one.sql"), first[0])
		require.Equal(t, filepath.Join(directory, "20260822143001_two.sql"), second[0])
	})

	t.Run("WriteNoFileWhenTheSchemaMatches", func(t *testing.T) {
		directory := t.TempDir()

		target := sqltest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		sqltest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		paths := driverssqlite.GenerateSQLiteMigration(t, target, migrations)
		require.Empty(t, paths)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("TheSecondMigrationHoldsTheChangeAlone", func(t *testing.T) {
		directory := t.TempDir()

		target := sqltest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		sqltest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		paths := driverssqlite.GenerateSQLiteMigration(t, target, migrations)
		require.Len(t, paths, 1)

		content, err := os.ReadFile(paths[0])
		require.NoError(t, err)
		require.Contains(t, string(content), `ADD COLUMN "email"`)
		require.NotContains(t, string(content), "CREATE TABLE")
	})
}

func TestSplitMigrationInstructions(t *testing.T) {
	moment := time.Date(2026, 8, 22, 14, 30, 0, 0, time.UTC)

	t.Run("OneFileWithNoSeparateInstruction", func(t *testing.T) {
		directory := t.TempDir()

		paths, err := coremigrations.WriteMigrationFiles(directory, "add_users", moment, "test",
			[]driversshared.Instruction{&driversshared.SQLDropTableInstruction{Name: "users"}})
		require.NoError(t, err)
		require.Equal(t, []string{filepath.Join(directory, "20260822143000_add_users.sql")}, paths)
	})

	t.Run("TheSeparateInstructionsTakeTheFirstFile", func(t *testing.T) {
		directory := t.TempDir()

		enumValue := &driverspostgres.PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "happy",
		}

		other := &driversshared.SQLDropTableInstruction{Name: "users"}

		paths, err := coremigrations.WriteMigrationFiles(directory, "add_mood", moment, "test",
			[]driversshared.Instruction{other, enumValue})
		require.NoError(t, err)
		require.Equal(t, []string{
			filepath.Join(directory, "20260822143000_add_mood_types.sql"),
			filepath.Join(directory, "20260822143001_add_mood.sql"),
		}, paths)

		first, err := os.ReadFile(paths[0])
		require.NoError(t, err)
		require.Equal(t, "-- dbdiff test\n-- generated 2026-08-22T14:30:00Z\n\n"+
			"-- Modify the type \"mood\"\nALTER TYPE \"mood\" ADD VALUE 'happy';\n", string(first))

		second, err := os.ReadFile(paths[1])
		require.NoError(t, err)
		require.Equal(t, "-- dbdiff test\n-- generated 2026-08-22T14:30:01Z\n\n"+
			"-- Drop the table \"users\"\nDROP TABLE \"users\";\n", string(second))
	})
}
