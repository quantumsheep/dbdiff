package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func statusCommand() *cli.Command {
	return &cli.Command{
		Name:         "status",
		Usage:        "List each migration and its state",
		Flags:        databaseMigrationFlags(),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(statusAction),
	}
}

func statusAction(ctx context.Context, command *cli.Command, _ *internalmigrations.Config, runner *internalmigrations.Runner) error {
	return runner.Status()
}
