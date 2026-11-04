package main_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestDbdiffCommand(t *testing.T) {
	t.Run("VersionFlag", func(t *testing.T) {
		result := clitest.Run(t, "--version")

		require.Equal(t, 0, result.ExitCode)
		require.Contains(t, result.Stdout, "dbdiff version ")
		require.NotContains(t, result.Stdout, "version \n")
	})

	t.Run("ShellCompletionOfMigrate", func(t *testing.T) {
		result := clitest.Run(t, "migrate", "--generate-shell-completion")

		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Equal(t, `generate:Write the next migration file
status:List each migration and its state
preview:Print the statements of the pending migrations
up:Apply every pending migration
step:Apply every pending migration, and ask before each file
verify:Compare the database against the replay of the applied migrations
repair:Make the history table agree with the migration files
baseline:Record every migration file as applied, and run no file
help:Shows a list of commands or help for one command
`, result.Stdout)
	})

	t.Run("DiffCommandName", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, "diff", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, `CREATE TABLE "users"`)
	})

	t.Run("ExitCodeFlagWithDifferences", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, "diff", "--exit-code", currentPath, finalPath)
		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stdout, `CREATE TABLE "users"`)
		require.Empty(t, result.Stderr)
	})

	t.Run("ExitCodeFlagWithoutDifferences", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "")

		result := clitest.Run(t, "diff", "--exit-code", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
	})

	t.Run("EmptyDriverFlagStartsTheDetection", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, "diff", "--driver", "", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, `CREATE TABLE "users"`)
	})

	t.Run("PrivilegesFlagWithSQLiteDriver", func(t *testing.T) {
		result := clitest.Run(t, "diff", "--privileges", "a.sqlite", "b.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the privileges option applies to the postgres driver and to the mysql driver\n", result.Stderr)
	})

	t.Run("ArgumentWithNoCommandName", func(t *testing.T) {
		result := clitest.Run(t, "current.sqlite", "final.sqlite")

		require.Equal(t, 3, result.ExitCode)
		require.Equal(t, "No help topic for 'current.sqlite'\n", result.Stderr)
	})

	t.Run("MissingCurrentArgument", func(t *testing.T) {
		result := clitest.Run(t, "diff")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: give the current schema as the first argument\n", result.Stderr)
	})

	t.Run("MissingDesiredArgument", func(t *testing.T) {
		result := clitest.Run(t, "diff", "current.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: give the desired schema as the second argument\n", result.Stderr)
	})

	t.Run("ServerVersionFlagWithSQLiteDriver", func(t *testing.T) {
		result := clitest.Run(t, "diff", "--server-version", "16", "current.sqlite", "desired.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: use the --server-version flag and the key \"server-version\" of dbdiff.yaml only with the postgres driver and the mysql driver\n", result.Stderr)
	})

	t.Run("UnsupportedDriver", func(t *testing.T) {
		result := clitest.Run(t, "diff", "--driver", "oracle", "current.sqlite", "desired.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: invalid value \"oracle\" for flag -driver: unsupported driver: oracle\n", result.Stderr)
	})

	t.Run("SchemaFlagWithSQLiteDriver", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		result := clitest.Run(t, "diff", "--schema", "public", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: use the --schema flag and the key \"schema\" of dbdiff.yaml only with the postgres driver\n", result.Stderr)
	})

	t.Run("DiffTwoDatabases", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "")

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
	})

	t.Run("DiffADatabaseThatDoesNotExist", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "absent.sqlite")

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: failed to compare the schemas: the desired database %q does not exist\n", finalPath), result.Stderr)
		require.NoFileExists(t, finalPath)
	})

	t.Run("DataFlag", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		clitest.WriteSQLiteDatabase(t, currentPath, schema+`INSERT INTO users (id, name) VALUES (1, 'Bob');`)
		clitest.WriteSQLiteDatabase(t, finalPath, schema+`INSERT INTO users (id, name) VALUES (1, 'Alice');`)

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "\n", result.Stdout)

		result = clitest.Run(t, "diff", "--data", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "UPDATE \"users\" SET \"name\" = 'Alice' WHERE \"id\" = 1;\n", result.Stdout)
	})

	t.Run("IgnoreTableFlag", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		clitest.WriteSQLiteDatabase(t, currentPath, schema)
		clitest.WriteSQLiteDatabase(t, finalPath, schema+`CREATE TABLE example (id INTEGER PRIMARY KEY);`)

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Contains(t, result.Stdout, "CREATE TABLE \"example\"")

		result = clitest.Run(t, "diff", "--ignore-table", "example", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "\n", result.Stdout)
	})

	t.Run("IgnoreTableKeyOfConfigFile", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		clitest.WriteSQLiteDatabase(t, currentPath, "")
		clitest.WriteSQLiteDatabase(t, finalPath, `CREATE TABLE example (id INTEGER PRIMARY KEY);`)

		configPath := clitest.WriteMigrationConfig(t, directory, "ignore-table:\n  - example\nmigrations: ignored\n")

		result := clitest.Run(t, "diff", "--config", configPath, currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "\n", result.Stdout)
	})

	t.Run("IgnoreTableFlagReplacesTheKeyOfConfigFile", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		clitest.WriteSQLiteDatabase(t, currentPath, "")
		clitest.WriteSQLiteDatabase(t, finalPath, `CREATE TABLE example (id INTEGER PRIMARY KEY);CREATE TABLE other (id INTEGER PRIMARY KEY);`)

		configPath := clitest.WriteMigrationConfig(t, directory, "ignore-table:\n  - example\n")

		result := clitest.Run(t, "diff", "--config", configPath, "--ignore-table", "other", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Contains(t, result.Stdout, "CREATE TABLE \"example\"")
		require.NotContains(t, result.Stdout, "CREATE TABLE \"other\"")
	})

	t.Run("DriverKeyOfConfigFileWithSchemaFlag", func(t *testing.T) {
		directory := t.TempDir()
		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\n")

		result := clitest.Run(t, "diff", "--config", configPath, "--schema", "public", "current.sqlite", "desired.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: use the --schema flag and the key \"schema\" of "+configPath+" only with the postgres driver\n", result.Stderr)
	})

	t.Run("ServerVersionKeyOfConfigFileWithSQLiteDriver", func(t *testing.T) {
		directory := t.TempDir()
		configPath := clitest.WriteMigrationConfig(t, directory, "driver: sqlite3\nserver-version: \"16\"\n")

		result := clitest.Run(t, "diff", "--config", configPath, "current.sqlite", "desired.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: use the --server-version flag and the key \"server-version\" of "+configPath+" only with the postgres driver and the mysql driver\n", result.Stderr)
	})

	t.Run("ConfigFlagWithAPathThatDoesNotExist", func(t *testing.T) {
		missingPath := filepath.Join(t.TempDir(), "missing.yaml")

		result := clitest.Run(t, "diff", "--config", missingPath, "current.sqlite", "desired.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: failed to read the configuration file: open "+missingPath+": no such file or directory\n", result.Stderr)
	})

	t.Run("CommentsFlag", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		clitest.WriteSQLiteDatabase(t, currentPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)
		clitest.WriteSQLiteDatabase(t, finalPath, `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (id INTEGER PRIMARY KEY);
		`)

		result := clitest.Run(t, "diff", "--comments", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, `-- Modify the table "users"
ALTER TABLE "users" ADD COLUMN "name" TEXT;
-- Create the table "posts"
CREATE TABLE "posts" (
	"id" INTEGER PRIMARY KEY
);
`, result.Stdout)
	})

	t.Run("SQLFileCurrent", func(t *testing.T) {
		finalPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("MigrationsDirectoryAsDesired", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_name.up.sql", `
			ALTER TABLE users ADD COLUMN name TEXT;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_name.down.sql", `
			ALTER TABLE users DROP COLUMN name;
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := clitest.Run(t, "diff", currentPath, migrationsDirectory)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("EmptyDirectoryDesired", func(t *testing.T) {
		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, "diff", currentPath, t.TempDir())

		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Equal(t, "DROP TABLE \"users\";\n", result.Stdout)
	})

	t.Run("TwoSQLFilesWithoutDriver", func(t *testing.T) {
		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)

		finalPath := sqltest.WriteSQLFile(t, t.TempDir(), "final.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`)

		result := clitest.Run(t, "diff", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, fmt.Sprintf("dbdiff: cannot detect the driver of the current schema %q and the desired schema %q. Name the driver with the --driver flag\n", currentPath, finalPath), result.Stderr)
		require.Empty(t, result.Stdout)
	})

	t.Run("TwoDifferentEnginesWithoutDriver", func(t *testing.T) {
		result := clitest.Run(t, "diff", "sqlite://current.db", "postgres://user@localhost/desired")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: the current schema \"sqlite://current.db\" names the sqlite3 driver and the desired schema \"postgres://user@localhost/desired\" names the postgres driver. Name the driver with the --driver flag\n", result.Stderr)
		require.Empty(t, result.Stdout)
	})

	t.Run("ServerVersionFlagWithPostgresDriver", func(t *testing.T) {
		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", "")
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql", "SELECT 1;")

		result := clitest.Run(t, "diff", "--driver", "postgres", "--server-version", "8", currentPath, desiredPath)

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: unknown postgres version \"8\"\n", result.Stderr)
	})

	t.Run("SQLFileCurrentWithPostgresDriver", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		finalPath := sqltest.WriteSQLFile(t, t.TempDir(), "final.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		result := clitest.Run(t, "diff", "--driver", "postgres", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" text;\n", result.Stdout)
	})
}
