package cmdmigrate

import (
	"context"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func statusCommand() *cli.Command {
	return &cli.Command{
		Name:         "status",
		Usage:        "List each migration and its state",
		Flags:        migrationFlags(databaseTargetUsage),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(statusAction),
	}
}

func statusAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	fmt.Print(coremigrations.RenderMigrationStatus(set))

	return set.RecordError()
}
