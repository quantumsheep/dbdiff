package helpers

import (
	"context"
	"fmt"
	"runtime/debug"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/urfave/cli/v3"
)

// The release workflow writes the tag into this variable with the linker flag -X.
var version = ""

func Version() string {
	if version != "" {
		return version
	}

	buildInfo, found := debug.ReadBuildInfo()
	if found && buildInfo.Main.Version != "" && buildInfo.Main.Version != "(devel)" {
		return buildInfo.Main.Version
	}

	return "unknown"
}

func OnUsageError(ctx context.Context, command *cli.Command, err error, isSubcommand bool) error {
	return err
}

// DetectDriverName names the engine of two data sources. The error of the detection
// names no CLI flag, so this wrap names the flag that gives the driver.
func DetectDriverName(source driversshared.DataSource, target driversshared.DataSource) (driversshared.DriverName, error) {
	driverName, err := driversshared.DetectDriver(source, target)

	return driverName, nameTheDriverFlag(err)
}

// DetectDriverNameOfTarget names the engine of the target of a migration.
func DetectDriverNameOfTarget(target driversshared.DataSource) (driversshared.DriverName, error) {
	driverName, err := driversshared.DetectDriverOfTarget(target)

	return driverName, nameTheDriverFlag(err)
}

func nameTheDriverFlag(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w with the --driver flag", err)
}
