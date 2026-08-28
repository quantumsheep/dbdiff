package cmdmigraterepair

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "repair",
		Usage: "Make the history table agree with the migration files",
		Description: "Update the checksum of a changed row, and delete a missing row and a dirty row. " +
			"A dirty row names a half-applied file. Repair the database before you delete that row",
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

func action(ctx context.Context, command *cli.Command) error {
	_, migrator, set, err := migrations.OpenSet(ctx, command)
	if err != nil {
		return err
	}

	defer func() { _ = migrator.Close() }()

	return coremigrations.RunMigrationRepair(ctx, migrator, set, os.Stdout)
}
