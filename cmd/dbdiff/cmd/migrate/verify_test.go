package cmdmigrate_test

import (
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateVerifyCommand(t *testing.T) {
	t.Run("ReportsADrift", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		up := clitest.Run(t, "migrate", "up", "--config", project.ConfigPath, "--target", project.DatabasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)

		clitest.WriteSQLiteDatabase(t, project.DatabasePath, "CREATE TABLE audit (id INTEGER PRIMARY KEY);")

		verify := clitest.Run(t, "migrate", "verify", "--config", project.ConfigPath, "--target", project.DatabasePath)
		require.Equal(t, 1, verify.ExitCode)
		require.Contains(t, verify.Stdout, `DROP TABLE "audit"`)
	})

	t.Run("NamesAnOutOfOrderFile", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		up := clitest.Run(t, "migrate", "up", "--config", project.ConfigPath, "--target", project.DatabasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260810101500_early.sql",
			"CREATE TABLE early (id INTEGER PRIMARY KEY);\n")

		verify := clitest.Run(t, "migrate", "verify", "--config", project.ConfigPath, "--target", project.DatabasePath)
		require.Equal(t, 0, verify.ExitCode, verify.Stderr)
		require.Contains(t, verify.Stdout, "20260810101500_early is out of order")
	})
}
