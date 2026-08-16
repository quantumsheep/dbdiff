package main

import (
	"bytes"
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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
		// The command prints the error one time only.
		require.Equal(t, 1, strings.Count(result.Stderr, "unsupported driver: mysql"))
	})

	t.Run("DiffTwoDatabases", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.sqlite")
		targetPath := filepath.Join(directory, "target.sqlite")

		result := runDbdiff(t, binaryPath, sourcePath, targetPath)

		require.Equal(t, 0, result.ExitCode)
		require.Empty(t, result.Stderr)
	})
}
