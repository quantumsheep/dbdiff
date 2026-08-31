package cmdmigrategenerate_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateGenerateCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("WithNoDriver", func(t *testing.T) {
		directory := t.TempDir()

		clitest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		schemaPath := filepath.Join(directory, "schema.sql")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"target: "+schemaPath+"\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: cannot detect the driver of the source %q and the target %q. Name the driver with the --driver flag\n",
			migrations, schemaPath), result.Stderr)
	})

	t.Run("WithNoTarget", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the target with the --target flag, with the key target of dbdiff.yaml, or with the DBDIFF_TARGET variable\n", result.Stderr)
	})

	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		clitest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the source with the --source flag, or with the key source of dbdiff.yaml\n", result.Stderr)
	})

	t.Run("RefusesAnEmptyMigration", func(t *testing.T) {
		directory := t.TempDir()

		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);"

		clitest.WriteSQLFile(t, directory, "schema.sql", schema)

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql", schema)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+
				"\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the migrations hold the schema of the source already, so dbdiff wrote no file\n", result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("IgnoresATableOfTheConfiguration", func(t *testing.T) {
		directory := t.TempDir()

		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);"

		clitest.WriteSQLFile(t, directory, "schema.sql", schema+"\nCREATE TABLE example (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql", schema)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+
				"\nsource: "+migrations+"\nignore:\n  tables:\n    - example\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_example")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the migrations hold the schema of the source already, so dbdiff wrote no file\n", result.Stderr)
	})

	t.Run("BuildsTheMigrationsDirectory", func(t *testing.T) {
		directory := t.TempDir()

		clitest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "migrations")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+
				"\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})
}
