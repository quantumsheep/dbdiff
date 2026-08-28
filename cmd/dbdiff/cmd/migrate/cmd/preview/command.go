package cmdmigratepreview

import (
	"context"
	"fmt"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "preview",
		Usage: "Print the statements of the pending migrations",
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
			&cli.BoolFlag{
				Name:  "run",
				Usage: "Apply the statements of every pending file in one transaction, and roll that transaction back",
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

	preview, err := coremigrations.RenderMigrationPreview(set)
	if err != nil {
		return err
	}

	fmt.Print(preview)

	if !command.Bool("run") {
		return nil
	}

	return coremigrations.RunMigrationPreview(ctx, migrator, set, os.Stdout)
}
