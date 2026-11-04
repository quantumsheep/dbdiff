package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func previewCommand() *cli.Command {
	return &cli.Command{
		Name:  "preview",
		Usage: "Print the statements of the pending migrations",
		Flags: databaseMigrationFlags(
			&cli.BoolFlag{
				Name:  "run",
				Usage: "Apply the statements of every pending file in one transaction, and roll that transaction back",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(previewAction),
	}
}

func previewAction(ctx context.Context, command *cli.Command, _ *internalmigrations.Config, runner *internalmigrations.Runner) error {
	err := runner.Preview()
	if err != nil {
		return err
	}

	if !command.Bool("run") {
		return nil
	}

	return runner.PreviewInTransaction(ctx)
}
