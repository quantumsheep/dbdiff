package cmdmigrateverify

import (
	"context"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "verify",
		Usage: "Compare the database against the replay of the applied migrations",
		Flags: []cli.Flag{
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
				Usage: "Connection URL of the database that the migrations change. The default is the DBDIFF_TARGET variable",
			},
		},
		OnUsageError: helpers.OnUsageError,
		Action:       action,
	}
}

func changeCountText(count int) string {
	if count == 1 {
		return "1 change"
	}

	return fmt.Sprintf("%d changes", count)
}

func action(ctx context.Context, command *cli.Command) error {
	config, migrator, set, err := migrations.OpenSet(ctx, command)
	if err != nil {
		return err
	}

	defer func() { _ = migrator.Close() }()

	err = set.RecordError()
	if err != nil {
		return err
	}

	for _, entry := range set.Entries {
		if entry.State != coremigrations.MigrationOutOfOrder {
			continue
		}

		fmt.Printf("%s is out of order. The replay does not hold it.\n", entry.FileName())
	}

	directory, cleanup, err := coremigrations.MigrationVerifyDirectory(set)
	if err != nil {
		return err
	}

	defer cleanup()

	source := driversshared.ParseDataSource(config.Target)
	target := driversshared.ParseDataSource(directory)

	driverName := config.Driver
	if driverName == "" {
		detected, err := driversshared.DetectDriver(source, target)
		if err != nil {
			return err
		}

		driverName = detected
	}

	driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		Version:      config.Version,
		Schema:       config.Schema,
		IgnoreTables: config.Ignore.Tables,
	})
	if err != nil {
		return err
	}

	instructions, err := driver.Diff(ctx, source, target, driversshared.DiffOptions{})
	if err != nil {
		return err
	}

	if len(instructions) == 0 {
		fmt.Println("The database holds the schema of the migrations.")

		return nil
	}

	fmt.Println(instructions.String())

	return fmt.Errorf("the database holds %s that no migration made", changeCountText(len(instructions)))
}
