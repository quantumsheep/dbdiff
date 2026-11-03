package cmdmigrate

import (
	"context"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func verifyCommand() *cli.Command {
	return &cli.Command{
		Name:         "verify",
		Usage:        "Compare the database against the replay of the applied migrations",
		Flags:        migrationFlags(databaseTargetUsage),
		OnUsageError: helpers.OnUsageError,
		Action:       withSet(verifyAction),
	}
}

func changeCountText(count int) string {
	if count == 1 {
		return "1 change"
	}

	return fmt.Sprintf("%d changes", count)
}

func verifyAction(ctx context.Context, command *cli.Command, config *coremigrations.MigrationConfig, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) error {
	err := set.RecordError()
	if err != nil {
		return err
	}

	for _, entry := range set.Entries {
		if entry.State != coremigrations.MigrationOutOfOrder {
			continue
		}

		fmt.Printf("%s is out of order. The replay does not hold it.\n", entry.FileName())
	}

	directory, cleanup, err := coremigrations.MigrationVerifyDirectory(set)
	if err != nil {
		return err
	}

	defer cleanup()

	source := driversshared.ParseDataSource(config.Target)
	target := driversshared.ParseDataSource(directory)

	driverName := config.Driver
	if driverName == "" {
		detected, err := helpers.DetectDriverName(source, target)
		if err != nil {
			return err
		}

		driverName = detected
	}

	driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		Version:      config.Version,
		Schema:       config.Schema,
		IgnoreTables: config.Ignore.Tables,
	})
	if err != nil {
		return err
	}

	instructions, err := driver.Diff(ctx, source, target, driversshared.DiffOptions{})
	if err != nil {
		return err
	}

	if len(instructions) == 0 {
		fmt.Println("The database holds the schema of the migrations.")

		return nil
	}

	fmt.Println(instructions.String())

	return fmt.Errorf("the database holds %s that no migration made", changeCountText(len(instructions)))
}
