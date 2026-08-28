package cmdmigrateup

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "up",
		Usage: "Apply every pending migration",
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
				Usage: "Version of the last migration to apply, for example 20260822143000. The default applies every pending migration",
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

	return migrations.ApplyMigrations(ctx, migrator, set, command.String("to"), os.Stdout)
}
