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
	cmd := &cli.Command{
		Name:        "dbdiff",
		Usage:       "Compare database schemas and generate migration scripts",
		Description: "Compare database schemas and generate migration scripts",
		Action:      action,
		UsageText:   "dbdiff [global options] <url1> <url2>",
		// main prints the error. Without this handler the command prints it too.
		OnUsageError: func(ctx context.Context, cmd *cli.Command, err error, isSubcommand bool) error {
			return err
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "driver",
				Usage: "Database driver to use. Supported drivers: sqlite3, postgres",
				Validator: func(s string) error {
					if slices.Contains([]string{"sqlite3", "postgres"}, s) {
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
				UsageText: "Database connection URL or path for the source database",
			},
			&cli.StringArg{
				Name:      "target",
				UsageText: "Database connection URL or path for the target database",
			},
		},
	}

	err := cmd.Run(context.Background(), os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbdiff: %v\n", err)
		os.Exit(1)
	}
}

func action(ctx context.Context, cmd *cli.Command) error {
	sourceDatabaseURL := cmd.StringArg("source")
	if sourceDatabaseURL == "" {
		return fmt.Errorf("source database URL is required")
	}

	targetDatabaseURL := cmd.StringArg("target")
	if targetDatabaseURL == "" {
		return fmt.Errorf("target database URL is required")
	}

	var driver drivers.Driver
	var err error

	driverFlag := cmd.String("driver")
	if driverFlag == "" {
		driverFlag = "sqlite3"
	}

	schemaFlag := cmd.String("schema")

	compareData := cmd.Bool("data")

	switch driverFlag {
	case "sqlite3":
		if schemaFlag != "" {
			return fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		driver, err = drivers.NewSQLiteDriver(&drivers.SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabaseURL,
			TargetDatabasePath: targetDatabaseURL,
			CompareData:        compareData,
		})
		if err != nil {
			return fmt.Errorf("failed to create sqlite3 driver: %w", err)
		}
	case "postgres":
		driver, err = drivers.NewPostgresDriver(&drivers.PostgresDriverConfig{
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
		return fmt.Errorf("unsupported driver: %s", cmd.String("driver"))
	}

	defer driver.Close()

	diff, err := driver.Diff(ctx)
	if err != nil {
		return fmt.Errorf("failed to diff databases: %w", err)
	}

	fmt.Println(diff)

	return nil
}
