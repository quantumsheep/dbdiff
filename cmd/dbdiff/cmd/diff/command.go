package cmddiff

import (
	"context"
	"errors"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/urfave/cli/v3"
)

// ErrDifferencesFound reaches main, which exits with the code 1 and prints nothing.
var ErrDifferencesFound = errors.New("the schemas differ")

func Command() *cli.Command {
	return &cli.Command{
		Name:         "diff",
		Usage:        "Compare two schemas and print the statements that change the source",
		UsageText:    "dbdiff diff [options] <source> <target>",
		Action:       action,
		OnUsageError: helpers.OnUsageError,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "privileges",
				Usage: "Compare the owner and the privileges of each object. A role belongs to the server, so this comparison is off by default",
			},
			&cli.BoolFlag{
				Name:  "comments",
				Usage: "Print a comment before each object that the output changes",
			},
			&cli.BoolFlag{
				Name:  "data",
				Usage: "Compare the rows of each table that the source and the target both hold. The comparison needs a primary key",
			},
			&cli.BoolFlag{
				Name:  "exit-code",
				Usage: "Exit with the code 1 when the schemas differ, like diff(1)",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:      "source",
				UsageText: "The current schema, which the output changes. A connection URL, a .sql file, or a directory of .sql files",
			},
			&cli.StringArg{
				Name:      "target",
				UsageText: "The final schema, which the output builds. A connection URL, a .sql file, or a directory of .sql files",
			},
		},
	}
}

func action(ctx context.Context, command *cli.Command) error {
	currentSchema := command.StringArg("source")
	if currentSchema == "" {
		return fmt.Errorf("source database URL is required")
	}

	finalSchema := command.StringArg("target")
	if finalSchema == "" {
		return fmt.Errorf("target database URL is required")
	}

	source := driversshared.ParseDataSource(currentSchema)
	target := driversshared.ParseDataSource(finalSchema)

	driverName := driversshared.DriverName(command.String("driver"))
	if driverName == "" {
		detected, err := driversshared.DetectDriver(source, target)
		if err != nil {
			return err
		}

		driverName = detected
	}

	driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		Schema:            command.String("schema"),
		ComparePrivileges: command.Bool("privileges"),
		IgnoreTables:      command.StringSlice("ignore-table"),
	})
	if err != nil {
		return err
	}

	instructions, err := driver.Diff(ctx, source, target, driversshared.DiffOptions{
		CompareData: command.Bool("data"),
	})
	if err != nil {
		return fmt.Errorf("failed to diff databases: %w", err)
	}

	if command.Bool("comments") {
		instructions = driversshared.AnnotateInstructions(instructions)
	}

	fmt.Println(instructions.String())

	if command.Bool("exit-code") && len(instructions) > 0 {
		return ErrDifferencesFound
	}

	return nil
}
