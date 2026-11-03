package cmdmigrate

import (
	"context"
	"fmt"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func previewCommand() *cli.Command {
	return &cli.Command{
		Name:  "preview",
		Usage: "Print the statements of the pending migrations",
		Flags: migrationFlags(databaseTargetUsage,
			&cli.BoolFlag{
				Name:  "run",
				Usage: "Apply the statements of every pending file in one transaction, and roll that transaction back",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(previewAction),
	}
}

func previewAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
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
