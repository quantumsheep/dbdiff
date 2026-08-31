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
		result := clitest.Run(t, binaryPath, "migrate", "status", "--driver", "oracle", "--target", "app.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "unsupported driver: oracle")
		require.Equal(t, 1, strings.Count(result.Stderr, "unsupported driver: oracle"))
	})

	t.Run("UndetectableTargetNamesTheDriverFlag", func(t *testing.T) {
		source := clitest.MakeMigrationsDirectory(t, t.TempDir())

		result := clitest.Run(t, binaryPath, "migrate", "status",
			"--source", source, "--target", "oracle://user@localhost/app")

		require.Equal(t, 1, result.ExitCode)
		require.Contains(t, result.Stderr, "cannot detect the driver")
		require.Contains(t, result.Stderr, "--driver")
	})
}
