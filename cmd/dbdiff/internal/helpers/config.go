package helpers

import (
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

const ConfigFlagUsage = "Path of the configuration file. The default is dbdiff.yaml of the working directory"

// ReadConfigWithFlags reads the configuration file and lets the flags driver, schema,
// server-version, scratch-database, and ignore-table replace their keys.
func ReadConfigWithFlags(command *cli.Command) (*internalmigrations.Config, string, error) {
	path := ConfigPath(command)

	config, err := internalmigrations.ReadConfig(path, command.String("config") == "")
	if err != nil {
		return nil, "", fmt.Errorf("failed to read the configuration file: %w", err)
	}

	driverName := command.String("driver")
	if driverName != "" {
		config.Driver = driversshared.DriverName(driverName)
	}

	schema := command.String("schema")
	if schema != "" {
		config.Schema = schema
	}

	serverVersion := command.String("server-version")
	if serverVersion != "" {
		config.ServerVersion = serverVersion
	}

	scratchDatabase := command.String("scratch-database")
	if scratchDatabase != "" {
		config.ScratchDatabase = scratchDatabase
	}

	ignoreTables := command.StringSlice("ignore-table")
	if len(ignoreTables) > 0 {
		config.IgnoreTables = ignoreTables
	}

	return config, path, nil
}

func ConfigPath(command *cli.Command) string {
	path := command.String("config")
	if path == "" {
		return internalmigrations.DefaultConfigPath
	}

	return path
}
