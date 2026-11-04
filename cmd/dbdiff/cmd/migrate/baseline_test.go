package cmdmigrate_test

import (
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateBaselineCommand(t *testing.T) {
	t.Run("RecordsTheFilesAndRunsNoFile", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		baseline := clitest.Run(t, "migrate", "baseline", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, baseline.ExitCode, baseline.Stderr)
		require.Contains(t, baseline.Stdout, "Recorded 20260814101500_init.")

		status := clitest.Run(t, "migrate", "status", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, status.ExitCode, status.Stderr)
		require.Contains(t, status.Stdout, "applied")

		up := clitest.Run(t, "migrate", "up", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)
		require.Contains(t, up.Stdout, "The database is up to date.")

		verify := clitest.Run(t, "migrate", "verify", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, verify.ExitCode, verify.Stderr)
	})

	t.Run("WithTheToFlag", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		baseline := clitest.Run(t, "migrate", "baseline",
			"--config", project.ConfigPath, "--database", project.DatabasePath, "--to", "20260814101500")
		require.Equal(t, 0, baseline.ExitCode, baseline.Stderr)
		require.Contains(t, baseline.Stdout, "Recorded 20260814101500_init.")
		require.NotContains(t, baseline.Stdout, "20260822143000_notes")

		status := clitest.Run(t, "migrate", "status", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, status.ExitCode, status.Stderr)
		require.Contains(t, status.Stdout, "pending")
	})

	t.Run("WithNoUnrecordedFile", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		up := clitest.Run(t, "migrate", "up", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, up.ExitCode, up.Stderr)

		baseline := clitest.Run(t, "migrate", "baseline", "--config", project.ConfigPath, "--database", project.DatabasePath)
		require.Equal(t, 0, baseline.ExitCode, baseline.Stderr)
		require.Contains(t, baseline.Stdout, "The record holds every file.")
	})
}
