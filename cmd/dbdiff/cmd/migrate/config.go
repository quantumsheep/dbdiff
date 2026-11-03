package cmdmigrate

import (
	"context"
	"fmt"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func migrationConfigFromCommand(command *cli.Command) (*internalmigrations.Config, error) {
	path := command.String("config")

	optional := path == ""
	if optional {
		path = internalmigrations.DefaultConfigPath
	}

	config, err := internalmigrations.ReadConfig(path, optional)
	if err != nil {
		return nil, fmt.Errorf("failed to read the configuration file: %w", err)
	}

	driverName := command.String("driver")
	if driverName != "" {
		config.Driver = driversshared.DriverName(driverName)
	}

	source := command.String("source")
	if source != "" {
		config.Source = source
	}

	target := command.String("target")
	if target != "" {
		config.Target = target
	}

	if config.Target == "" {
		config.Target = os.Getenv("DBDIFF_TARGET")
	}

	schema := command.String("schema")
	if schema != "" {
		config.Schema = schema
	}

	ignoreTables := command.StringSlice("ignore-table")
	if len(ignoreTables) > 0 {
		config.Ignore.Tables = ignoreTables
	}

	if config.Source == "" {
		return nil, fmt.Errorf("name the source with the --source flag, or with the key source of dbdiff.yaml")
	}

	if config.Target == "" {
		return nil, fmt.Errorf("name the target with the --target flag, with the key target of dbdiff.yaml, or with the DBDIFF_TARGET variable")
	}

	return config, nil
}

func openSet(ctx context.Context, command *cli.Command) (*internalmigrations.Config, internalmigrations.Migrator, *internalmigrations.Set, error) {
	config, err := migrationConfigFromCommand(command)
	if err != nil {
		return nil, nil, nil, err
	}

	// The key target of dbdiff.yaml also holds the wanted schema of generate. A SQL
	// source here gives an opaque driver error without this check.
	target, ok := driversshared.ParseDataSource(config.Target).(driversshared.ConnectionStringDataSource)
	if !ok {
		return nil, nil, nil, fmt.Errorf("the target %q names SQL text, and this command needs a database. Give a connection URL with the --target flag, or with the DBDIFF_TARGET variable", config.Target)
	}

	driverName := config.Driver
	if driverName == "" {
		detected, err := helpers.DetectDriverNameOfTarget(target)
		if err != nil {
			return nil, nil, nil, err
		}

		driverName = detected
	}

	migrator, err := internaldrivers.NewMigrator(ctx, driverName, target, config.Schema)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open the target database: %w", err)
	}

	set, err := internalmigrations.LoadSet(ctx, migrator, config.Source)
	if err != nil {
		_ = migrator.Close()

		return nil, nil, nil, err
	}

	return config, migrator, set, nil
}

const databaseTargetUsage = "Connection URL of the database that the migrations change. The default is the DBDIFF_TARGET variable"

func migrationFlags(targetUsage string, flags ...cli.Flag) []cli.Flag {
	return append([]cli.Flag{
		&cli.StringFlag{
			Name:  "config",
			Usage: "Path of the configuration file. The default is dbdiff.yaml of the working directory",
		},
		&cli.StringFlag{
			Name:  "source",
			Usage: "Directory of the migration files",
		},
		&cli.StringFlag{
			Name:  "target",
			Usage: targetUsage,
		},
	}, flags...)
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
			Output:   os.Stdout,
		}

		return action(ctx, command, config, runner)
	}
}
