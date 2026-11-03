package cmdmigrate

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func repairCommand() *cli.Command {
	return &cli.Command{
		Name:  "repair",
		Usage: "Make the history table agree with the migration files",
		Description: "Update the checksum of a changed row, and delete a missing row and a dirty row. " +
			"A dirty row names a half-applied file. Repair the database before you delete that row",
		Flags:        migrationFlags(databaseTargetUsage),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(repairAction),
	}
}

func repairAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	return coremigrations.RunMigrationRepair(ctx, migrator, set, os.Stdout)
}
