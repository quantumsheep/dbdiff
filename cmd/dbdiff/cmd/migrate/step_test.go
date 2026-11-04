package cmdmigrate_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateStepCommand(t *testing.T) {
	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, "migrate", "step", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: name the source with the --source flag, or with the key source of dbdiff.yaml\n", result.Stderr)
	})

	t.Run("WithoutATerminal", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		result := clitest.Run(t, "migrate", "step", "--config", project.ConfigPath, "--target", project.DatabasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: step needs a terminal. Use the command up to apply a migration with no prompt\n", result.Stderr)
	})
}
