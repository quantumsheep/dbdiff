package migrations

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/quantumsheep/dbdiff/internal/drivers"
	"github.com/stretchr/testify/require"
)

func TestGenerateMigration(t *testing.T) {
	t.Run("WriteTheFirstMigration", func(t *testing.T) {
		directory := t.TempDir()

		target := WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		paths := GenerateSQLiteMigration(t, target, migrations)
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

		first, err := WriteMigrationFiles(directory, "one", moment, "test",
			[]drivers.Instruction{&drivers.SQLDropTableInstruction{Name: "users"}})
		require.NoError(t, err)

		second, err := WriteMigrationFiles(directory, "two", moment, "test",
			[]drivers.Instruction{&drivers.SQLDropTableInstruction{Name: "notes"}})
		require.NoError(t, err)

		require.Equal(t, filepath.Join(directory, "20260822143000_one.sql"), first[0])
		require.Equal(t, filepath.Join(directory, "20260822143001_two.sql"), second[0])
	})

	t.Run("WriteNoFileWhenTheSchemaMatches", func(t *testing.T) {
		directory := t.TempDir()

		target := WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		paths := GenerateSQLiteMigration(t, target, migrations)
		require.Empty(t, paths)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("TheSecondMigrationHoldsTheChangeAlone", func(t *testing.T) {
		directory := t.TempDir()

		target := WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);")

		paths := GenerateSQLiteMigration(t, target, migrations)
		require.Len(t, paths, 1)

		content, err := os.ReadFile(paths[0])
		require.NoError(t, err)
		require.Contains(t, string(content), `ADD COLUMN "email"`)
		require.NotContains(t, string(content), "CREATE TABLE")
	})
}

func TestSplitMigrationInstructions(t *testing.T) {
	t.Run("OneGroupWithNoEnumValue", func(t *testing.T) {
		instructions := []drivers.Instruction{
			&drivers.SQLCommentInstruction{
				Text: "a",
			},
		}

		groups := splitMigrationInstructions(instructions)
		require.Len(t, groups, 1)
	})

	t.Run("TheEnumValuesTakeTheFirstGroup", func(t *testing.T) {
		enumValue := &drivers.PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "'happy'",
		}

		other := &drivers.SQLCommentInstruction{
			Text: "a",
		}

		groups := splitMigrationInstructions([]drivers.Instruction{other, enumValue})
		require.Len(t, groups, 2)
		require.Equal(t, []drivers.Instruction{enumValue}, groups[0])
		require.Equal(t, []drivers.Instruction{other}, groups[1])
	})
}
