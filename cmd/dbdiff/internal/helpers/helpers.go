package helpers

import (
	"context"
	"fmt"
	"runtime/debug"

	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
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

// DetectDriverName names the engine of two data sources. If the two sides hold SQL text,
// the scratch database names the engine. The error of the detection names no CLI flag, so
// this wrap names the flag that gives the driver.
func DetectDriverName(
	current driversshared.DataSource,
	desired driversshared.DataSource,
	scratchDatabase string,
) (driversshared.DriverName, error) {
	_, currentIsSQLSource := driversshared.SQLSourcePath(current)
	_, desiredIsSQLSource := driversshared.SQLSourcePath(desired)

	if scratchDatabase != "" && currentIsSQLSource && desiredIsSQLSource {
		scratchDriverName, err := driversshared.DetectDriverOfDatabase(driversshared.ConnectionStringDataSource{
			ConnectionString: scratchDatabase,
		})
		if err == nil {
			return scratchDriverName, nil
		}
	}

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

// CheckDriverSettings refuses a setting that the driver does not read.
func CheckDriverSettings(
	driverName driversshared.DriverName,
	schema string,
	serverVersion string,
	scratchDatabase string,
	configPath string,
) error {
	if schema != "" && driverName != driversshared.PostgresDriverName {
		return fmt.Errorf("use %s only with the postgres driver", settingName("schema", configPath))
	}

	if serverVersion != "" && driverName == driversshared.SQLiteDriverName {
		return fmt.Errorf("use %s only with the postgres driver and the mysql driver", settingName("server-version", configPath))
	}

	if scratchDatabase != "" && driverName == driversshared.SQLiteDriverName {
		return fmt.Errorf("use %s only with the postgres driver and the mysql driver", settingName("scratch-database", configPath))
	}

	if scratchDatabase != "" && serverVersion != "" {
		return fmt.Errorf("do not use %s together with %s. dbdiff starts no temporary server when a scratch database is set",
			settingName("scratch-database", configPath), settingName("server-version", configPath))
	}

	return nil
}

func settingName(name string, configPath string) string {
	return fmt.Sprintf("the --%s flag and the key %q of %s", name, name, configPath)
}

func NewDriver(
	config *internalmigrations.Config,
	configPath string,
	current driversshared.DataSource,
	desired driversshared.DataSource,
	comparePrivileges bool,
) (driversshared.Driver, error) {
	driverName := config.Driver
	if driverName == "" {
		detected, err := DetectDriverName(current, desired, config.ScratchDatabase)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	err := CheckDriverSettings(driverName, config.Schema, config.ServerVersion, config.ScratchDatabase, configPath)
	if err != nil {
		return nil, err
	}

	return internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		ServerVersion:     config.ServerVersion,
		ScratchDatabase:   config.ScratchDatabase,
		Schema:            config.Schema,
		ComparePrivileges: comparePrivileges,
		IgnoreTables:      config.IgnoreTables,
	})
}
