package cmdmigrate

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func generateCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Write the next migration file",
		UsageText: "dbdiff migrate generate [options] <name>",
		Flags: migrationFlags(
			&cli.StringFlag{
				Name:  "desired",
				Usage: "The desired schema. A .sql file, a directory of .sql files, or a connection URL",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:      "name",
				UsageText: "Short name of the migration, for example add_created_at",
			},
		},
		Action: generateAction,
	}
}

func generateAction(ctx context.Context, command *cli.Command) error {
	config, path, err := migrationConfigFromCommand(command, "desired")
	if err != nil {
		return err
	}

	name := command.StringArg("name")
	if name == "" {
		return fmt.Errorf("name the migration, for example add_created_at")
	}

	slug := internalmigrations.Slug(name)
	if slug == "" {
		return fmt.Errorf("the name of a migration holds no letter and no digit")
	}

	err = os.MkdirAll(config.Migrations, 0o750)
	if err != nil {
		return err
	}

	current := driversshared.ParseDataSource(config.Migrations)
	desired := driversshared.ParseDataSource(config.Desired)

	driverName := config.Driver
	if driverName == "" {
		detected, err := helpers.DetectDriverName(current, desired)
		if err != nil {
			return err
		}

		driverName = detected
	}

	err = helpers.CheckDriverSettings(driverName, config.Schema, config.ServerVersion, path)
	if err != nil {
		return err
	}

	driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		ServerVersion: config.ServerVersion,
		Schema:        config.Schema,
		IgnoreTables:  config.IgnoreTables,
	})
	if err != nil {
		return err
	}

	instructions, err := driver.Diff(ctx, current, desired, driversshared.DiffOptions{})
	if err != nil {
		return err
	}

	paths, err := internalmigrations.WriteFiles(config.Migrations, slug,
		time.Now(), helpers.Version(), instructions)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return fmt.Errorf("the migrations hold the desired schema already, so dbdiff wrote no file")
	}

	for _, path := range paths {
		_, err = fmt.Fprintln(command.Root().Writer, path)
		if err != nil {
			return err
		}
	}

	return nil
}
