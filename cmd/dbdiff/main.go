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
		Description: "Compare database schemas and generate migration scripts",
		Action:      action,
		UsageText:   "dbdiff [global options] <url1> <url2>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "driver",
				Usage: "Database driver to use. Supported drivers: sqlite3",
				Validator: func(s string) error {
					if slices.Contains([]string{"sqlite3"}, s) {
						return nil
					}
					return fmt.Errorf("unsupported driver: %s", s)
				},
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
	cmd.Run(context.Background(), os.Args)
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

	switch driverFlag {
	case "sqlite3":
		driver, err = drivers.NewSQLiteDriver(&drivers.SQLLiteDriverConfig{
			SourceDatabasePath: sourceDatabaseURL,
			TargetDatabasePath: targetDatabaseURL,
		})
		if err != nil {
			return fmt.Errorf("failed to create sqlite3 driver: %w", err)
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
