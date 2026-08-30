package cmdmigratebaseline_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateBaselineCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("RecordsTheFilesAndRunsNoFile", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		baseline := clitest.Run(t, binaryPath, "migrate", "baseline", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, baseline.ExitCode, baseline.Stderr)
		require.Contains(t, baseline.Stdout, "Recorded 20260814101500_init.")

		status := clitest.Run(t, binaryPath, "migrate", "status", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, status.ExitCode, status.Stderr)
		require.Contains(t, status.Stdout, "applied")

		up := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)
		require.Contains(t, up.Stdout, "The database is up to date.")

		verify := clitest.Run(t, binaryPath, "migrate", "verify", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, verify.ExitCode, verify.Stderr)
	})

	t.Run("WithNoUnrecordedFile", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		up := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)

		baseline := clitest.Run(t, binaryPath, "migrate", "baseline", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, baseline.ExitCode, baseline.Stderr)
		require.Contains(t, baseline.Stdout, "The record holds every file.")
	})
}
