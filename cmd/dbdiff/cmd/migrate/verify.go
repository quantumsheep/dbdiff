package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func verifyCommand() *cli.Command {
	return &cli.Command{
		Name:         "verify",
		Usage:        "Compare the database against the replay of the applied migrations",
		Flags:        databaseMigrationFlags(),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(verifyAction),
	}
}

func verifyAction(
	ctx context.Context,
	command *cli.Command,
	config *internalmigrations.Config,
	runner *internalmigrations.Runner,
) error {
	database := driversshared.ParseDataSource(config.Database)

	return runner.Verify(ctx, func(ctx context.Context, replay driversshared.DataSource) (driversshared.Instructions, error) {
		driver, err := helpers.NewDriver(config, helpers.ConfigPath(command), database, replay, false)
		if err != nil {
			return nil, err
		}

		return driver.Diff(ctx, database, replay, driversshared.DiffOptions{})
	})
}
