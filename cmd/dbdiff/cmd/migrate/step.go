package cmdmigrate

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func stepCommand() *cli.Command {
	return &cli.Command{
		Name:         "step",
		Usage:        "Apply every pending migration, and ask before each file",
		Flags:        migrationFlags(databaseTargetUsage),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(stepAction),
	}
}

func stepAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	return coremigrations.StepMigration(ctx, migrator, set, os.Stdin, os.Stdout)
}
