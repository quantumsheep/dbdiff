package cmdmigrate_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestMigrateGenerateCommand(t *testing.T) {
	t.Run("WithNoDriver", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		schemaPath := filepath.Join(directory, "schema.sql")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"desired: "+schemaPath+"\nmigrations: "+migrations+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: cannot detect the driver of the current schema %q and the desired schema %q. Name the driver with the --driver flag\n",
			migrations, schemaPath), result.Stderr)
	})

	t.Run("WithNoDesiredSchema", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nmigrations: "+migrations+"\ndatabase: app.sqlite\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the desired schema with the --desired flag, or with the key \"desired\" of %s\n", configPath), result.Stderr)
	})

	t.Run("WithNoMigrations", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ndesired: "+filepath.Join(directory, "schema.sql")+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})

	t.Run("TheDesiredFlagOverridesTheKey", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nmigrations: "+migrations+"\ndesired: "+filepath.Join(directory, "absent.sql")+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath,
			"--desired", filepath.Join(directory, "schema.sql"), "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("TheMigrationsFlag", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "migrations")
		absentPath := filepath.Join(directory, "absent.yaml")

		// The text of the error depends on the operating system.
		_, readError := os.ReadFile(absentPath)
		require.Error(t, readError)

		result := clitest.Run(t, "migrate", "generate", "--driver", "sqlite3",
			"--config", absentPath,
			"--migrations", migrations, "--desired", filepath.Join(directory, "schema.sql"), "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: failed to read the configuration file: "+readError.Error()+"\n", result.Stderr)

		result = clitest.Run(t, "migrate", "generate", "--driver", "sqlite3",
			"--migrations", migrations, "--desired", filepath.Join(directory, "schema.sql"), "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("RefusesTheDatabaseFlag", func(t *testing.T) {
		result := clitest.Run(t, "migrate", "generate", "--database", "app.sqlite", "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: flag provided but not defined: -database\n", result.Stderr)
	})

	t.Run("WithARemovedKey", func(t *testing.T) {
		directory := t.TempDir()

		configPath := clitest.WriteMigrationConfig(t, directory, "target: schema.sql\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: failed to read the configuration file: the key \"target\" of %s no longer exists. "+
			"Use \"database\" for the database, or \"desired\" for the wanted schema of migrate generate\n", configPath), result.Stderr)
	})

	t.Run("WithAVariableThatIsNotSet", func(t *testing.T) {
		directory := t.TempDir()

		configPath := clitest.WriteMigrationConfig(t, directory, "desired: ${DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET}\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: failed to read the configuration file: the variable \"DBDIFF_TEST_VARIABLE_THAT_IS_NOT_SET\" "+
			"of the key \"desired\" of %s is not set\n", configPath), result.Stderr)
	})

	t.Run("WithAVariableOfTheDesiredSchema", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		t.Setenv("DBDIFF_TEST_DESIRED", filepath.Join(directory, "schema.sql"))

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\nmigrations: "+migrations+"\ndesired: ${DBDIFF_TEST_DESIRED}\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("WithTheServerVersionFlagOnTheSQLiteDriver", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, "migrate", "generate", "--driver", "sqlite3", "--server-version", "16",
			"--migrations", filepath.Join(directory, "migrations"), "--desired", filepath.Join(directory, "schema.sql"), "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: use the --server-version flag and the key \"server-version\" of dbdiff.yaml only with the postgres driver and the mysql driver\n", result.Stderr)
	})

	t.Run("AFlagValueStaysLiteral", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "${HOME}")

		result := clitest.Run(t, "migrate", "generate", "--driver", "sqlite3",
			"--config", clitest.WriteMigrationConfig(t, directory, ""),
			"--migrations", migrations, "--desired", filepath.Join(directory, "schema.sql"), "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("WithTheServerVersionFlagOnThePostgresDriver", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", `
			DO $$ BEGIN
				IF current_setting('server_version_num')::int / 10000 <> 17 THEN
					RAISE EXCEPTION 'the server version is %', current_setting('server_version');
				END IF;
			END $$;
			CREATE TABLE users (id INT NOT NULL);
		`)

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: postgres\nmigrations: "+migrations+"\ndesired: "+filepath.Join(directory, "schema.sql")+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "--server-version", "17.6.0", "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("RefusesAnEmptyMigration", func(t *testing.T) {
		directory := t.TempDir()

		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);"

		sqltest.WriteSQLFile(t, directory, "schema.sql", schema)

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		sqltest.WriteSQLFile(t, migrations, "20260814101500_init.sql", schema)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ndesired: "+filepath.Join(directory, "schema.sql")+
				"\nmigrations: "+migrations+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the migrations hold the desired schema already, so dbdiff wrote no file\n", result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("IgnoresATableOfTheConfiguration", func(t *testing.T) {
		directory := t.TempDir()

		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);"

		sqltest.WriteSQLFile(t, directory, "schema.sql", schema+"\nCREATE TABLE example (id INTEGER PRIMARY KEY);")

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		sqltest.WriteSQLFile(t, migrations, "20260814101500_init.sql", schema)

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ndesired: "+filepath.Join(directory, "schema.sql")+
				"\nmigrations: "+migrations+"\nignore-table:\n  - example\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_example")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the migrations hold the desired schema already, so dbdiff wrote no file\n", result.Stderr)
	})

	t.Run("BuildsTheMigrationsDirectory", func(t *testing.T) {
		directory := t.TempDir()

		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		migrations := filepath.Join(directory, "migrations")

		configPath := clitest.WriteMigrationConfig(t, directory,
			"driver: sqlite3\ndesired: "+filepath.Join(directory, "schema.sql")+
				"\nmigrations: "+migrations+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "add_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("EmptyFlag", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)

		configPath := clitest.WriteMigrationConfig(t, directory, "migrations: "+migrations+"\n")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "--empty", "backfill_users")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		entries, err := os.ReadDir(migrations)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		name := entries[0].Name()
		require.Regexp(t, `^\d{14}_backfill_users\.sql$`, name)
		require.Equal(t, filepath.Join(migrations, name)+"\n", result.Stdout)

		moment, err := time.Parse("20060102150405", name[:14])
		require.NoError(t, err)

		content, err := os.ReadFile(filepath.Join(migrations, name))
		require.NoError(t, err)
		require.Equal(t, "-- dbdiff "+helpers.Version()+"\n-- generated "+moment.UTC().Format(time.RFC3339)+"\n", string(content))
	})

	t.Run("GuessRenamesFlag", func(t *testing.T) {
		directory := t.TempDir()

		migrations := clitest.MakeMigrationsDirectory(t, directory)
		sqltest.WriteSQLFile(t, migrations, "20260101000000_add_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, nickname TEXT);")
		sqltest.WriteSQLFile(t, directory, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")

		result := clitest.Run(t, "migrate", "generate", "--driver", "sqlite3", "--guess-renames",
			"--migrations", migrations, "--desired", filepath.Join(directory, "schema.sql"), "rename_nickname")
		require.Equal(t, 0, result.ExitCode, result.Stderr)

		path := strings.TrimSuffix(result.Stdout, "\n")
		name := filepath.Base(path)

		moment, err := time.Parse("20060102150405", name[:14])
		require.NoError(t, err)

		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "-- dbdiff "+helpers.Version()+"\n-- generated "+moment.UTC().Format(time.RFC3339)+"\n"+
			"\n-- Modify the table \"users\"\nALTER TABLE \"users\" RENAME COLUMN \"nickname\" TO \"email\";\n", string(content))
	})

	t.Run("EmptyFlagWithNoMigrations", func(t *testing.T) {
		directory := t.TempDir()

		configPath := clitest.WriteMigrationConfig(t, directory, "")

		result := clitest.Run(t, "migrate", "generate", "--config", configPath, "--empty", "backfill_users")
		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: name the migration directory with the --migrations flag, or with the key \"migrations\" of %s\n", configPath), result.Stderr)
	})
}
