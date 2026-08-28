package cmdmigraterepair_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateRepairCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, binaryPath, "migrate", "repair", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "--source")
	})

	t.Run("ACleanRecordNeedsNoRepair", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		migrations := clitest.MakeMigrationsDirectory(t, directory)
		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		result = clitest.Run(t, binaryPath, "migrate", "repair", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, "The record needs no repair.")
	})
}
