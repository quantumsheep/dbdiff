package cmdmigratestep_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateStepCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, binaryPath, "migrate", "step", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the source with the --source flag, or with the key source of dbdiff.yaml\n", result.Stderr)
	})

	t.Run("WithoutATerminal", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		migrations := clitest.MakeMigrationsDirectory(t, directory)
		clitest.WriteSQLFile(t, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nsource: "+migrations+"\n")

		result := clitest.Run(t, binaryPath, "migrate", "step", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: step needs a terminal. Use the command up to apply a migration with no prompt\n", result.Stderr)
	})
}
