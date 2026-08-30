package cmdmigratebaseline

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
		Name:  "baseline",
		Usage: "Record every migration file as applied, and run no file",
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
			&cli.StringFlag{
				Name:  "to",
				Usage: "Version of the last migration to record, for example 20260822143000. The default records every migration file",
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

	return coremigrations.RunMigrationBaseline(ctx, migrator, set, command.String("to"), os.Stdout)
}
