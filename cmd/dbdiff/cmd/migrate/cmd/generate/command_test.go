package cmdmigrategenerate_test

import (
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

		configPath := clitest.WriteMigrationConfig(t, directory,
			"target: "+filepath.Join(directory, "schema.sql")+"\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "--driver")
	})

	t.Run("WithNoTarget", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "target")
	})

	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		clitest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "--source")
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
		require.Contains(t, result.Stderr, "wrote no file")

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
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
