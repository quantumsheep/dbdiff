package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func upCommand() *cli.Command {
	return &cli.Command{
		Name:  "up",
		Usage: "Apply every pending migration",
		Flags: databaseMigrationFlags(
			&cli.StringFlag{
				Name:  "to",
				Usage: "Version of the last migration to apply, for example 20260822143000. The default applies every pending migration",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(upAction),
	}
}

func upAction(ctx context.Context, command *cli.Command, _ *internalmigrations.Config, runner *internalmigrations.Runner) error {
	return runner.Up(ctx, command.String("to"))
}
