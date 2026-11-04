package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func repairCommand() *cli.Command {
	return &cli.Command{
		Name:  "repair",
		Usage: "Make the history table agree with the migration files",
		Description: "Update the checksum of a changed row, and delete a missing row and a dirty row. " +
			"A dirty row names a half-applied file. Repair the database before you delete that row",
		Flags:        databaseMigrationFlags(),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(repairAction),
	}
}

func repairAction(
	ctx context.Context,
	command *cli.Command,
	_ *internalmigrations.Config,
	runner *internalmigrations.Runner,
) error {
	return runner.Repair(ctx)
}
