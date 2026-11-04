package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func stepCommand() *cli.Command {
	return &cli.Command{
		Name:         "step",
		Usage:        "Apply every pending migration, and ask before each file",
		Flags:        databaseMigrationFlags(),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(stepAction),
	}
}

func stepAction(ctx context.Context, command *cli.Command, _ *internalmigrations.Config, runner *internalmigrations.Runner) error {
	return runner.Step(ctx, command.Root().Reader)
}
