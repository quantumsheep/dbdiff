package main

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type commandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func buildDbdiff(tb testing.TB) string {
	tb.Helper()

	binaryPath := filepath.Join(tb.TempDir(), "dbdiff")

	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}

	build := exec.Command("go", "build", "-o", binaryPath, ".")

	output, err := build.CombinedOutput()
	require.NoError(tb, err, string(output))

	return binaryPath
}

func runDbdiff(tb testing.TB, binaryPath string, args ...string) commandResult {
	tb.Helper()

	var stdout, stderr bytes.Buffer

	command := exec.Command(binaryPath, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	exitCode := 0

	if err != nil {
		var exitError *exec.ExitError
		require.True(tb, errors.As(err, &exitError), err)
		exitCode = exitError.ExitCode()
	}

	return commandResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}
}

func writeSQLFile(tb testing.TB, directory string, name string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, name)

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

func writeSQLiteDatabase(tb testing.TB, path string, sqlStatements string) {
	tb.Helper()

	database, err := sql.Open("sqlite3", path)
	require.NoError(tb, err)

	defer func() {
		require.NoError(tb, database.Close())
	}()

	_, err = database.Exec(sqlStatements)
	require.NoError(tb, err)
}

// TestDbdiffCommand covers the binary. Two cases compare the whole standard output. They
// are the one check of the render path from end to end, because the driver tests compare
// instructions and not text.
func TestDbdiffCommand(t *testing.T) {
	binaryPath := buildDbdiff(t)

	t.Run("MissingSourceArgument", func(t *testing.T) {
		result := runDbdiff(t, binaryPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "source database URL is required")
	})

	t.Run("UnsupportedDriver", func(t *testing.T) {
		result := runDbdiff(t, binaryPath, "--driver", "mysql", "source.sqlite", "target.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "unsupported driver: mysql")
		require.Equal(t, 1, strings.Count(result.Stderr, "unsupported driver: mysql"))
	})

	t.Run("SchemaFlagWithSQLiteDriver", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.sqlite")
		targetPath := filepath.Join(directory, "target.sqlite")

		result := runDbdiff(t, binaryPath, "--schema", "public", sourcePath, targetPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "the --schema flag applies to the postgres driver only")
	})

	t.Run("DiffTwoDatabases", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.sqlite")
		targetPath := filepath.Join(directory, "target.sqlite")

		result := runDbdiff(t, binaryPath, sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
	})

	t.Run("DataFlag", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.sqlite")
		targetPath := filepath.Join(directory, "target.sqlite")

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		writeSQLiteDatabase(t, sourcePath, schema+`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		writeSQLiteDatabase(t, targetPath, schema+`INSERT INTO users (id, name) VALUES (1, 'Bob');`)

		result := runDbdiff(t, binaryPath, sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "\n", result.Stdout)

		result = runDbdiff(t, binaryPath, "--data", sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "UPDATE \"users\" SET \"name\" = 'Alice' WHERE \"id\" = 1;\n", result.Stdout)
	})

	t.Run("SQLFileSource", func(t *testing.T) {
		sourcePath := writeSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`)

		targetPath := filepath.Join(t.TempDir(), "target.sqlite")
		writeSQLiteDatabase(t, targetPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := runDbdiff(t, binaryPath, sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("MigrationsDirectorySource", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		writeSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)
		writeSQLFile(t, migrationsDirectory, "002_add_name.up.sql", `
			ALTER TABLE users ADD COLUMN name TEXT;
		`)
		writeSQLFile(t, migrationsDirectory, "002_add_name.down.sql", `
			ALTER TABLE users DROP COLUMN name;
		`)

		targetPath := filepath.Join(t.TempDir(), "target.sqlite")
		writeSQLiteDatabase(t, targetPath, `CREATE TABLE users (id INTEGER PRIMARY KEY);`)

		result := runDbdiff(t, binaryPath, migrationsDirectory, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" TEXT;\n", result.Stdout)
	})

	t.Run("EmptyDirectorySource", func(t *testing.T) {
		targetPath := filepath.Join(t.TempDir(), "target.sqlite")

		result := runDbdiff(t, binaryPath, t.TempDir(), targetPath)

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "holds no .sql file")
	})

	// The standard output must hold the SQL statements only, and no log of the temporary
	// PostgreSQL server.
	t.Run("SQLFileSourceWithPostgresDriver", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		sourcePath := writeSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		targetPath := writeSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		result := runDbdiff(t, binaryPath, "--driver", "postgres", sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
		require.Equal(t, "ALTER TABLE \"users\" ADD COLUMN \"name\" text;\n", result.Stdout)
	})
}
