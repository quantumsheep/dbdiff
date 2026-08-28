package cmdmigratestatus_test

import (
	"strings"
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateStatusCommand(t *testing.T) {
	binaryPath := clitest.Build(t)

	t.Run("UnsupportedDriverPrintsOnce", func(t *testing.T) {
		result := clitest.Run(t, binaryPath, "migrate", "status", "--driver", "mysql", "--target", "app.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "unsupported driver: mysql")
		require.Equal(t, 1, strings.Count(result.Stderr, "unsupported driver: mysql"))
	})
}
