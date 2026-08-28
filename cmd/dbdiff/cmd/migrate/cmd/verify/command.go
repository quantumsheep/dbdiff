package cmdmigrateverify

import (
	"context"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	"github.com/quantumsheep/dbdiff/internal/drivers"
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

	directory, cleanup, err := coremigrations.MigrationVerifyDirectory(set)
	if err != nil {
		return err
	}

	defer cleanup()

	driver, err := drivers.NewDriver(ctx, config.Driver, config.Target, directory,
		config.Schema, config.Version, false, false)
	if err != nil {
		return err
	}

	defer func() { _ = driver.Close() }()

	instructions, err := driver.Diff(ctx)
	if err != nil {
		return err
	}

	if len(instructions) == 0 {
		fmt.Println("The database holds the schema of the migrations.")

		return nil
	}

	fmt.Println(drivers.RenderInstructions(instructions))

	return fmt.Errorf("the database holds %s that no migration made", changeCountText(len(instructions)))
}
