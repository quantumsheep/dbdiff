package migrations

import (
	"context"
	"fmt"
	"os"

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
		config.Driver = driverName
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

	migrator, err := coremigrations.NewMigrator(ctx, config.Driver, config.Target, config.Schema)
	if err != nil {
		return nil, nil, nil, err
	}

	set, err := LoadMigrationSet(ctx, migrator, config.Source)
	if err != nil {
		migrator.Close()

		return nil, nil, nil, err
	}

	return config, migrator, set, nil
}
