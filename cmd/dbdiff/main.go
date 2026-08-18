package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/quantumsheep/dbdiff/drivers"
	"github.com/urfave/cli/v3"
)

func main() {
	command := &cli.Command{
		Name:        "dbdiff",
		Usage:       "Compare database schemas and generate migration scripts",
		Description: "Compare database schemas and generate migration scripts",
		Action:      action,
		UsageText:   "dbdiff [global options] <source> <target>",
		OnUsageError: func(ctx context.Context, command *cli.Command, err error, isSubcommand bool) error {
			return err
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "driver",
				Usage: "Database driver to use. Supported drivers: sqlite3, postgres. The default is the driver of the source and the target",
				Validator: func(s string) error {
					if slices.Contains(drivers.SupportedDriverNames, s) {
						return nil
					}
					return fmt.Errorf("unsupported driver: %s", s)
				},
			},
			&cli.StringFlag{
				Name:  "schema",
				Usage: "Name of the schema to compare. The postgres driver accepts this flag. The default is the schema of the search path",
			},
			&cli.BoolFlag{
				Name:  "data",
				Usage: "Compare the rows of each table that the source and the target both hold. The comparison needs a primary key",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:      "source",
				UsageText: "Connection URL of the source database, a .sql file, or a directory of .sql files",
			},
			&cli.StringArg{
				Name:      "target",
				UsageText: "Connection URL of the target database, a .sql file, or a directory of .sql files",
			},
		},
	}

	err := command.Run(context.Background(), os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbdiff: %v\n", err)
		os.Exit(1)
	}
}

func action(ctx context.Context, command *cli.Command) error {
	sourceDatabaseURL := command.StringArg("source")
	if sourceDatabaseURL == "" {
		return fmt.Errorf("source database URL is required")
	}

	targetDatabaseURL := command.StringArg("target")
	if targetDatabaseURL == "" {
		return fmt.Errorf("target database URL is required")
	}

	var driver drivers.Driver
	var err error

	driverName := command.String("driver")
	if driverName == "" {
		driverName, err = drivers.DetectDriver(sourceDatabaseURL, targetDatabaseURL)
		if err != nil {
			return err
		}
	}

	schemaFlag := command.String("schema")

	compareData := command.Bool("data")

	switch driverName {
	case drivers.SQLiteDriverName:
		if schemaFlag != "" {
			return fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		driver, err = drivers.NewSQLiteDriver(ctx, &drivers.SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabaseURL,
			TargetDatabasePath: targetDatabaseURL,
			CompareData:        compareData,
		})
		if err != nil {
			return fmt.Errorf("failed to create sqlite3 driver: %w", err)
		}
	case drivers.PostgresDriverName:
		driver, err = drivers.NewPostgresDriver(ctx, &drivers.PostgresDriverConfig{
			SourceConnectionString: sourceDatabaseURL,
			TargetConnectionString: targetDatabaseURL,
			SourceSchema:           schemaFlag,
			TargetSchema:           schemaFlag,
			CompareData:            compareData,
		})
		if err != nil {
			return fmt.Errorf("failed to create postgres driver: %w", err)
		}
	default:
		return fmt.Errorf("unsupported driver: %s", driverName)
	}

	defer driver.Close()

	instructions, err := driver.Diff(ctx)
	if err != nil {
		return fmt.Errorf("failed to diff databases: %w", err)
	}

	fmt.Println(drivers.RenderInstructions(instructions))

	return nil
}
