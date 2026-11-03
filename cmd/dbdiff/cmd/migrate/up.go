package cmdmigrate

import (
	"context"
	"os"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func upCommand() *cli.Command {
	return &cli.Command{
		Name:  "up",
		Usage: "Apply every pending migration",
		Flags: migrationFlags(databaseTargetUsage,
			&cli.StringFlag{
				Name:  "to",
				Usage: "Version of the last migration to apply, for example 20260822143000. The default applies every pending migration",
			},
		),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(upAction),
	}
}

func upAction(ctx context.Context, command *cli.Command, _ *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	return coremigrations.ApplyMigrations(ctx, migrator, set, command.String("to"), os.Stdout)
}
