package cmdmigrate_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateCommand(t *testing.T) {
	t.Run("GenerateAndStatusAndUp", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ndesired: "+filepath.Join(directory, "schema.sql")+
				"\nmigrations: "+migrations+"\n")

		generate := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 0, generate.ExitCode, generate.Stderr)
		require.Contains(t, generate.Stdout, "_add_users.sql")

		status := clitest.Run(t, "migrate", "status", "--config", configPath, "--database", databasePath)
		require.Equal(t, 0, status.ExitCode, status.Stderr)
		require.Contains(t, status.Stdout, "pending")

		preview := clitest.Run(t, "migrate", "preview", "--config", configPath, "--database", databasePath)
		require.Equal(t, 0, preview.ExitCode, preview.Stderr)
		require.Contains(t, preview.Stdout, "_add_users")
		require.Contains(t, preview.Stdout, "CREATE TABLE")

		up := clitest.Run(t, "migrate", "up", "--config", configPath, "--database", databasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)
		require.Contains(t, up.Stdout, "Applied")

		statusAgain := clitest.Run(t, "migrate", "status", "--config", configPath, "--database", databasePath)
		require.Equal(t, 0, statusAgain.ExitCode, statusAgain.Stderr)
		require.Contains(t, statusAgain.Stdout, "applied")

		verify := clitest.Run(t, "migrate", "verify", "--config", configPath, "--database", databasePath)
		require.Equal(t, 0, verify.ExitCode, verify.Stderr)
	})
}
