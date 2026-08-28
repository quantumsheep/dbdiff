package cmdmigrate_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("GenerateAndStatusAndUp", func(t *testing.T) {
		directory := t.TempDir()

		clitest.WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ntarget: "+filepath.Join(directory, "schema.sql")+
				"\nsource: "+migrations+"\n")

		generate := clitest.Run(t, binaryPath, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 0, generate.ExitCode, generate.Stderr)
		require.Contains(t, generate.Stdout, "_add_users.sql")

		status := clitest.Run(t, binaryPath, "migrate", "status", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, status.ExitCode, status.Stderr)
		require.Contains(t, status.Stdout, "pending")

		preview := clitest.Run(t, binaryPath, "migrate", "preview", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, preview.ExitCode, preview.Stderr)
		require.Contains(t, preview.Stdout, "_add_users")
		require.Contains(t, preview.Stdout, "CREATE TABLE")

		up := clitest.Run(t, binaryPath, "migrate", "up", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)
		require.Contains(t, up.Stdout, "Applied")

		statusAgain := clitest.Run(t, binaryPath, "migrate", "status", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, statusAgain.ExitCode, statusAgain.Stderr)
		require.Contains(t, statusAgain.Stdout, "applied")

		verify := clitest.Run(t, binaryPath, "migrate", "verify", "--config", configPath, "--target", databasePath)
		require.Equal(t, 0, verify.ExitCode, verify.Stderr)
	})
}
