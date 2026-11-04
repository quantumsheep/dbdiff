package cmdmigrate_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateUpCommand(t *testing.T) {
	t.Run("WithNoSource", func(t *testing.T) {
		directory := t.TempDir()

		databasePath := filepath.Join(directory, "app.sqlite")
		clitest.WriteSQLiteDatabase(t, databasePath, "")

		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath, "--database", databasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithNoDatabase", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nmigrations: "+migrations+"\ndesired: schema.sql\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the database with the --database flag, or with the key \"database\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithASQLFileAsTheDatabase", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nmigrations: "+migrations+"\ndatabase: ./schema.sql\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: the database \"./schema.sql\" names SQL text, and this command needs a connection URL. "+
			"Give the connection URL with the --database flag, or with the key \"database\" of %s\n", configPath), result.Stderr)
	})

	t.Run("TheDatabaseFlagOverridesTheKey", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		configPath := clitest.WriteMigrationConfig(t, filepath.Dir(project.ConfigPath),
			"driver: sqlite3\nmigrations: "+project.MigrationsDirectory+"\ndatabase: ./schema.sql\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath, "--database", project.DatabasePath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Equal(t, "Applied 20260814101500_init.\n", result.Stdout)
	})

	t.Run("WithAVariableOfTheDatabase", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		t.Setenv("DBDIFF_TEST_DATABASE_URL", project.DatabasePath)

		configPath := clitest.WriteMigrationConfig(t, filepath.Dir(project.ConfigPath),
			"driver: sqlite3\nmigrations: "+project.MigrationsDirectory+"\ndatabase: ${DBDIFF_TEST_DATABASE_URL}\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Equal(t, "Applied 20260814101500_init.\n", result.Stdout)
	})

	t.Run("WithAnotherConfigurationFile", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := filepath.Join(directory, "ci.yaml")
		require.NoError(t, os.WriteFile(configPath, []byte("driver: sqlite3\nmigrations: "+migrations+"\n"), 0o600))

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the database with the --database flag, or with the key \"database\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithAnEmptyConfigurationFile", func(t *testing.T) {
		configPath := clitest.WriteMigrationConfig(t, t.TempDir(), "")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithAConfigurationFileThatHoldsOnlyAComment", func(t *testing.T) {
		configPath := clitest.WriteMigrationConfig(t, t.TempDir(), "# no key\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithAConfigurationFileThatHoldsNoMapping", func(t *testing.T) {
		configPath := clitest.WriteMigrationConfig(t, t.TempDir(), "- migrations\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: failed to read the configuration file: the configuration file %s must hold a mapping of keys\n", configPath), result.Stderr)
	})

	t.Run("WithTheServerVersionKeyOnTheSQLiteDriver", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		configPath := clitest.WriteMigrationConfig(t, filepath.Dir(project.ConfigPath),
			"migrations: "+project.MigrationsDirectory+"\nserver-version: 17.6.0\n")

		result := clitest.Run(t, "migrate", "up", "--config", configPath, "--database", project.DatabasePath)
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: use the --server-version flag and the key \"server-version\" of %s only with the postgres driver and the mysql driver\n", configPath), result.Stderr)
	})

	t.Run("WithTheSchemaFlagOnTheSQLiteDriver", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		result := clitest.Run(t, "migrate", "up", "--config", project.ConfigPath, "--database", project.DatabasePath, "--schema", "app")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: use the --schema flag and the key \"schema\" of %s only with the postgres driver\n", project.ConfigPath), result.Stderr)
	})

	t.Run("RefusesTheDesiredFlag", func(t *testing.T) {
		result := clitest.Run(t, "migrate", "up", "--desired", "schema.sql")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: flag provided but not defined: -desired\n", result.Stderr)
	})

	t.Run("WithTheToFlag", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		result := clitest.Run(t, "migrate", "up",
			"--config", project.ConfigPath, "--database", project.DatabasePath, "--to", "20260814101500")
		require.Equal(t, 0, result.ExitCode)
		require.Contains(t, result.Stdout, "20260814101500_init")
		require.NotContains(t, result.Stdout, "20260822143000_notes")
	})

	t.Run("WithTheToFlagAndAVersionThatNoFileHolds", func(t *testing.T) {
		project := clitest.NewMigrationProject(t, "")

		sqltest.WriteSQLFile(t, project.MigrationsDirectory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		result := clitest.Run(t, "migrate", "up",
			"--config", project.ConfigPath, "--database", project.DatabasePath, "--to", "20990101000000")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the directory holds no migration with the version 20990101000000\n", result.Stderr)
	})
}
