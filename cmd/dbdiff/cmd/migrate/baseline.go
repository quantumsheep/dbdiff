package cmdmigrate

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func baselineCommand() *cli.Command {
	return &cli.Command{
		Name:  "baseline",
		Usage: "Record every migration file as applied, and run no file",
		Flags: migrationFlags(databaseTargetUsage,
			&cli.StringFlag{
				Name:  "to",
				Usage: "Version of the last migration to record, for example 20260822143000. The default records every migration file",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(baselineAction),
	}
}

func baselineAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	return coremigrations.RunMigrationBaseline(ctx, migrator, set, command.String("to"), os.Stdout)
}
