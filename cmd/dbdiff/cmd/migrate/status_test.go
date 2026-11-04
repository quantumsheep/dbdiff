package cmdmigrate_test

import (
	"testing"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/clitest"
	"github.com/stretchr/testify/require"
)

func TestMigrateStatusCommand(t *testing.T) {
	t.Run("UnsupportedDriverPrintsOnce", func(t *testing.T) {
		result := clitest.Run(t, "migrate", "status", "--driver", "oracle", "--target", "app.sqlite")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: invalid value \"oracle\" for flag -driver: unsupported driver: oracle\n", result.Stderr)
	})

	t.Run("UndetectableTargetNamesTheDriverFlag", func(t *testing.T) {
		source := clitest.MakeMigrationsDirectory(t, t.TempDir())

		result := clitest.Run(t, "migrate", "status",
			"--source", source, "--target", "oracle://user@localhost/app")

		require.Equal(t, 1, result.ExitCode)
		require.Equal(t, "dbdiff: cannot detect the driver of the target \"oracle://user@localhost/app\". Name the driver with the --driver flag\n", result.Stderr)
	})
}
