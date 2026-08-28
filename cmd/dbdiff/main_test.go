package main_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestDbdiffCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("VersionFlag", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "--version")

		require.Equal(t, 0, result.ExitCode)
		require.Contains(t, result.Stdout, "dbdiff version ")
		require.NotContains(t, result.Stdout, "version \n")
	})

	t.Run("DiffCommandName", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, `CREATE TABLE "users"`)
	})

	t.Run("ExitCodeFlagWithDifferences", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, binaryPath, "diff", "--exit-code", currentPath, finalPath)
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

		result := clitest.Run(t, binaryPath, "diff", "--exit-code", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
	})

	t.Run("EmptyDriverFlagStartsTheDetection", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, binaryPath, "diff", "--driver", "", currentPath, finalPath)
		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Contains(t, result.Stdout, `CREATE TABLE "users"`)
	})

	t.Run("PrivilegesFlagWithSQLiteDriver", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "diff", "--privileges", "a.sqlite", "b.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "the --privileges flag applies to the postgres driver only")
	})

	t.Run("ArgumentWithNoCommandName", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "current.sqlite", "final.sqlite")

		require.Equal(t, 3, result.ExitCode)
		require.Contains(t, result.Stderr, "No help topic for 'current.sqlite'")
	})

	t.Run("MissingSourceArgument", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "diff")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "source database URL is required")
	})

	t.Run("UnsupportedDriver", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "diff", "--driver", "mysql", "source.sqlite", "target.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "unsupported driver: mysql")
		require.Equal(t, 1, strings.Count(result.Stderr, "unsupported driver: mysql"))
	})

	t.Run("SchemaFlagWithSQLiteDriver", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		result := clitest.Run(t, binaryPath, "diff", "--schema", "public", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "the --schema flag applies to the postgres driver only")
	})

	t.Run("DiffTwoDatabases", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "final.sqlite")
		clitest.WriteSQLiteDatabase(t, finalPath, "")

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
	})

	t.Run("DiffADatabaseThatDoesNotExist", func(t *testing.T) {
		directory := t.TempDir()

		currentPath := filepath.Join(directory, "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "")

		finalPath := filepath.Join(directory, "absent.sqlite")

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "does not exist")
		require.NoFileExists(t, finalPath)
	})

	t.Run("DataFlag", func(t *testing.T) {
		directory := t.TempDir()
		currentPath := filepath.Join(directory, "current.sqlite")
		finalPath := filepath.Join(directory, "final.sqlite")

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		clitest.WriteSQLiteDatabase(t, currentPath, schema+`INSERT INTO users (id, name) VALUES (1, 'Bob');`)
		clitest.WriteSQLiteDatabase(t, finalPath, schema+`INSERT INTO users (id, name) VALUES (1, 'Alice');`)

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "\n", result.Stdout)

		result = clitest.Run(t, binaryPath, "diff", "--data", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "UPDATE \"users\" SET \"name\" = 'Alice' WHERE \"id\" = 1;\n", result.Stdout)
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

		result := clitest.Run(t, binaryPath, "diff", "--comments", currentPath, finalPath)

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

	t.Run("SQLFileSource", func(t *testing.T) {
		finalPath := clitest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("MigrationsDirectorySource", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		clitest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)
		clitest.WriteSQLFile(t, migrationsDirectory, "002_add_name.up.sql", `
			ALTER TABLE users ADD COLUMN name TEXT;
		`)
		clitest.WriteSQLFile(t, migrationsDirectory, "002_add_name.down.sql", `
			ALTER TABLE users DROP COLUMN name;
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := clitest.Run(t, binaryPath, "diff", currentPath, migrationsDirectory)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("EmptyDirectoryTarget", func(t *testing.T) {
		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		clitest.WriteSQLiteDatabase(t, currentPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

		result := clitest.Run(t, binaryPath, "diff", currentPath, t.TempDir())

		require.Equal(t, 0, result.ExitCode, result.Stderr)
		require.Equal(t, "DROP TABLE \"users\";\n", result.Stdout)
	})

	t.Run("TwoSQLFilesWithoutDriver", func(t *testing.T) {
		currentPath := clitest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)

		finalPath := clitest.WriteSQLFile(t, t.TempDir(), "final.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`)

		result := clitest.Run(t, binaryPath, "diff", currentPath, finalPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "cannot detect the driver")
		require.Contains(t, result.Stderr, "--driver")
		require.Empty(t, result.Stdout)
	})

	t.Run("TwoDifferentEnginesWithoutDriver", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "diff", "sqlite://source.db", "postgres://user@localhost/target")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "names the sqlite3 driver")
		require.Contains(t, result.Stderr, "names the postgres driver")
		require.Empty(t, result.Stdout)
	})

	t.Run("SQLFileSourceWithPostgresDriver", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		currentPath := clitest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		finalPath := clitest.WriteSQLFile(t, t.TempDir(), "final.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		result := clitest.Run(t, binaryPath, "diff", "--driver", "postgres", currentPath, finalPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" text;\n", result.Stdout)
	})
}
