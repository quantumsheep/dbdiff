package cmdmigrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func migrationConfigFromCommand(command *cli.Command, requiredKey string) (*internalmigrations.Config, string, error) {
	path := command.String("config")

	optional := path == ""
	if optional {
		path = internalmigrations.DefaultConfigPath
	}

	config, err := internalmigrations.ReadConfig(path, optional)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read the configuration file: %w", err)
	}

	driverName := command.String("driver")
	if driverName != "" {
		config.Driver = driversshared.DriverName(driverName)
	}

	overrides := map[string]*string{
		"migrations":     &config.Migrations,
		"database":       &config.Database,
		"desired":        &config.Desired,
		"schema":         &config.Schema,
		"server-version": &config.ServerVersion,
	}

	for flagName, value := range overrides {
		flagValue := command.String(flagName)
		if flagValue != "" {
			*value = flagValue
		}
	}

	ignoreTables := command.StringSlice("ignore-table")
	if len(ignoreTables) > 0 {
		config.IgnoreTables = ignoreTables
	}

	requiredValues := []struct {
		key         string
		description string
		value       string
	}{
		{
			key:         "migrations",
			description: "the migration directory",
			value:       config.Migrations,
		},
		{
			key:         requiredKey,
			description: requiredKeyDescriptions[requiredKey],
			value:       *overrides[requiredKey],
		},
	}

	for _, required := range requiredValues {
		if required.value == "" {
			return nil, "", fmt.Errorf("name %s with the --%s flag, or with the key %q of %s",
				required.description, required.key, required.key, path)
		}
	}

	return config, path, nil
}

var requiredKeyDescriptions = map[string]string{
	"database": "the database",
	"desired":  "the desired schema",
}

func openSet(ctx context.Context, command *cli.Command) (*internalmigrations.Config, internalmigrations.Migrator, *internalmigrations.Set, error) {
	config, path, err := migrationConfigFromCommand(command, "database")
	if err != nil {
		return nil, nil, nil, err
	}

	// A SQL source here gives an opaque driver error without this check.
	database, ok := driversshared.ParseDataSource(config.Database).(driversshared.ConnectionStringDataSource)
	if !ok {
		return nil, nil, nil, fmt.Errorf(`the database %q names SQL text, and this command needs a connection URL. Give the connection URL with the --database flag, or with the key "database" of %s`, config.Database, path)
	}

	driverName := config.Driver
	if driverName == "" {
		detected, err := helpers.DetectDriverNameOfDatabase(database)
		if err != nil {
			return nil, nil, nil, err
		}

		driverName = detected
	}

	err = helpers.CheckDriverSettings(driverName, config.Schema, config.ServerVersion, path)
	if err != nil {
		return nil, nil, nil, err
	}

	migrator, set, err := internaldrivers.OpenMigrationSet(ctx, driverName, database, config.Schema, config.Migrations)

	var openDatabaseError *internaldrivers.OpenDatabaseError
	if errors.As(err, &openDatabaseError) {
		return nil, nil, nil, fmt.Errorf("failed to open the database: %w", err)
	}

	if err != nil {
		return nil, nil, nil, err
	}

	return config, migrator, set, nil
}

func migrationFlags(flags ...cli.Flag) []cli.Flag {
	return append([]cli.Flag{
		&cli.StringFlag{
			Name:  "config",
			Usage: "Path of the configuration file. The default is dbdiff.yaml of the working directory",
		},
		&cli.StringFlag{
			Name:  "migrations",
			Usage: "Directory of the migration files",
		},
	}, flags...)
}

func databaseMigrationFlags(flags ...cli.Flag) []cli.Flag {
	return migrationFlags(append([]cli.Flag{
		&cli.StringFlag{
			Name:  "database",
			Usage: "Connection URL of the database that the command reads or changes",
		},
	}, flags...)...)
}

func withRunner(action func(ctx context.Context, command *cli.Command, config *internalmigrations.Config, runner *internalmigrations.Runner) error) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		config, migrator, set, err := openSet(ctx, command)
		if err != nil {
			return err
		}

		defer func() { _ = migrator.Close() }()

		runner := &internalmigrations.Runner{
			Migrator: migrator,
			Set:      set,
			Output:   command.Root().Writer,
		}

		return action(ctx, command, config, runner)
	}
}
