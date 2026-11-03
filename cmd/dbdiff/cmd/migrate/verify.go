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
		Flags:        migrationFlags(databaseTargetUsage),
		OnUsageError: helpers.OnUsageError,
		Action:       withRunner(verifyAction),
	}
}

func verifyAction(ctx context.Context, command *cli.Command, config *internalmigrations.Config, runner *internalmigrations.Runner) error {
	source := driversshared.ParseDataSource(config.Target)

	return runner.Verify(ctx, func(ctx context.Context, replay driversshared.DataSource) (driversshared.Instructions, error) {
		driverName := config.Driver
		if driverName == "" {
			detected, err := helpers.DetectDriverName(source, replay)
			if err != nil {
				return nil, err
			}

			driverName = detected
		}

		driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
			Version:      config.Version,
			Schema:       config.Schema,
			IgnoreTables: config.Ignore.Tables,
		})
		if err != nil {
			return nil, err
		}

		return driver.Diff(ctx, source, replay, driversshared.DiffOptions{})
	})
}
