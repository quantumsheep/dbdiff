package migrations

import (
	"context"
	"fmt"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func GetMigrationConfigFromCommand(command *cli.Command) (*coremigrations.MigrationConfig, error) {
	path := command.String("config")

	optional := path == ""
	if optional {
		path = coremigrations.DefaultMigrationConfigPath
	}

	config, err := coremigrations.ReadMigrationConfig(path, optional)
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

func OpenSet(ctx context.Context, command *cli.Command) (*coremigrations.MigrationConfig, coremigrations.Migrator, *coremigrations.MigrationSet, error) {
	config, err := GetMigrationConfigFromCommand(command)
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

	migrator, err := coremigrations.NewMigrator(ctx, driverName, target, config.Schema)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open the target database: %w", err)
	}

	set, err := coremigrations.LoadMigrationSet(ctx, migrator, config.Source)
	if err != nil {
		_ = migrator.Close()

		return nil, nil, nil, err
	}

	return config, migrator, set, nil
}
