package cmdmigrateverify_test

import (
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateVerifyCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("ReportsADrift", func(t *testing.T) {
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

		clitest.WriteSQLiteDatabase(t, databasePath, "CREATE TABLE audit (id INTEGER PRIMARY KEY);")

		verify := clitest.Run(t, binaryPath, "migrate", "verify", "--config", configPath, "--target", databasePath)
		require.Equal(t, 1, verify.ExitCode)
		require.Contains(t, verify.Stdout, `DROP TABLE "audit"`)
	})
}
