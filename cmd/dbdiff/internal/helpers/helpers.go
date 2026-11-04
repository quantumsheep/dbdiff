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
func DetectDriverName(
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (driversshared.DriverName, error) {
	driverName, err := driversshared.DetectDriver(current, desired)

	return driverName, nameTheDriverFlag(err)
}

// DetectDriverNameOfDatabase names the engine of the database of a migration.
func DetectDriverNameOfDatabase(database driversshared.DataSource) (driversshared.DriverName, error) {
	driverName, err := driversshared.DetectDriverOfDatabase(database)

	return driverName, nameTheDriverFlag(err)
}

func nameTheDriverFlag(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w with the --driver flag", err)
}

// CheckDriverSettings refuses a setting that the driver does not read. An empty
// configPath names only the flag, because the diff command reads no configuration file.
func CheckDriverSettings(
	driverName driversshared.DriverName,
	schema string,
	serverVersion string,
	configPath string,
) error {
	if schema != "" && driverName != driversshared.PostgresDriverName {
		return fmt.Errorf("use %s only with the postgres driver", settingName("schema", configPath))
	}

	if serverVersion != "" && driverName == driversshared.SQLiteDriverName {
		return fmt.Errorf("use %s only with the postgres driver and the mysql driver", settingName("server-version", configPath))
	}

	return nil
}

func settingName(name string, configPath string) string {
	if configPath == "" {
		return fmt.Sprintf("the --%s flag", name)
	}

	return fmt.Sprintf("the --%s flag and the key %q of %s", name, name, configPath)
}
