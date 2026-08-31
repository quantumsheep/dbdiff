package cmdmigrateup_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateUpCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the source with the --source flag, or with the key source of dbdiff.yaml\n", result.Stderr)
	})

	t.Run("WithNoTarget", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the target with the --target flag, with the key target of dbdiff.yaml, or with the DBDIFF_TARGET variable\n", result.Stderr)
	})

	t.Run("WithASQLSourceAsTheTarget", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\ntarget: ./schema.sql\n")

		result := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the target \"./schema.sql\" names SQL text, and this command needs a database. Give a connection URL with the --target flag, or with the DBDIFF_TARGET variable\n", result.Stderr)
	})

	t.Run("WithTheToFlag", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)
		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		clitest.WriteSQLFile(t, migrations, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "up",
			"--config", configPath, "--target", databasePath, "--to", "20260814101500")
		require.Equal(t, 0, result.ExitCode)
		require.Contains(t, result.Stdout, "20260814101500_init")
		require.NotContains(t, result.Stdout, "20260822143000_notes")
	})

	t.Run("WithTheToFlagAndAVersionThatNoFileHolds", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)
		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "up",
			"--config", configPath, "--target", databasePath, "--to", "20990101000000")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the directory holds no migration with the version 20990101000000\n", result.Stderr)
	})
}
