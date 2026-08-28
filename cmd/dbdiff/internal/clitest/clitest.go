package clitest

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func Build(tb testing.TB) string {
	tb.Helper()

	binaryPath := filepath.Join(tb.TempDir(), "dbdiff")

	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}

	build := exec.Command("go", "build", "-o", binaryPath, "github.com/quantumsheep/dbdiff/cmd/dbdiff")

	output, err := build.CombinedOutput()
	require.NoError(tb, err, string(output))

	return binaryPath
}

func Run(tb testing.TB, binaryPath string, args ...string) Result {
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

	return Result{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}
}

func WriteSQLFile(tb testing.TB, directory string, name string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, name)

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

func WriteSQLiteDatabase(tb testing.TB, path string, sqlStatements string) {
	tb.Helper()

	database, err := sql.Open("sqlite3", path)
	require.NoError(tb, err)

	defer func() {
		require.NoError(tb, database.Close())
	}()

	_, err = database.Exec(sqlStatements)
	require.NoError(tb, err)
}

func WriteMigrationConfig(tb testing.TB, directory string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, "dbdiff.yaml")

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

func MakeMigrationsDirectory(tb testing.TB, directory string) string {
	tb.Helper()

	migrations := filepath.Join(directory, "migrations")
	require.NoError(tb, os.Mkdir(migrations, 0o750))

	return migrations
}
