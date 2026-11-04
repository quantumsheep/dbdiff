package cmdmigrate_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigratePreviewCommand(t *testing.T) {
	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, "migrate", "preview", "--config", configPath, "--database", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})

	t.Run("PrintsThePendingFile", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		result := clitest.Run(t, "migrate", "preview", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, "20260814101500_init")
	})
}
