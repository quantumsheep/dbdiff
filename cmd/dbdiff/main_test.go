package main

import (
	"bytes"
	"database/sql"
	"errors"
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

// TestDbdiffCommand covers the binary. Two subtests compare the whole standard output. They
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
}
