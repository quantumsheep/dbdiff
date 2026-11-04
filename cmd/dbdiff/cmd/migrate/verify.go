package cmdmigrate

import (
	"context"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
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
		driverName := config.Driver
		if driverName == "" {
			detected, err := helpers.DetectDriverName(database, replay)
			if err != nil {
				return nil, err
			}

			driverName = detected
		}

		driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
			ServerVersion: config.ServerVersion,
			Schema:        config.Schema,
			IgnoreTables:  config.IgnoreTables,
		})
		if err != nil {
			return nil, err
		}

		return driver.Diff(ctx, database, replay, driversshared.DiffOptions{})
	})
}
