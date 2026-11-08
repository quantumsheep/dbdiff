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

// ErrDifferencesFound reaches ExitCode of cmdroot, which returns the code 1 and prints nothing.
var ErrDifferencesFound = errors.New("the schemas differ")

func Command() *cli.Command {
	return &cli.Command{
		Name:         "diff",
		Usage:        "Print the statements that make the current schema equal to the desired schema",
		UsageText:    "dbdiff diff [options] <current> <desired>",
		Action:       action,
		OnUsageError: helpers.OnUsageError,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: helpers.ConfigFlagUsage,
			},
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
				Usage: "Compare the rows of each table that the current schema and the desired schema both hold. The comparison needs a primary key",
			},
			&cli.BoolFlag{
				Name:  "guess-renames",
				Usage: "Rename a column if exactly one removed column of the table has the same attributes as the added column. Without this flag, dbdiff drops the removed column and adds the added column",
			},
			&cli.BoolFlag{
				Name:  "exit-code",
				Usage: "Exit with the code 1 when the schemas differ, like diff(1)",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:      "current",
				UsageText: "The current schema, which the output changes. A connection URL, a .sql file, or a directory of .sql files",
			},
			&cli.StringArg{
				Name:      "desired",
				UsageText: "The desired schema, which the output builds. A connection URL, a .sql file, or a directory of .sql files",
			},
		},
	}
}

func action(ctx context.Context, command *cli.Command) error {
	currentArgument := command.StringArg("current")
	if currentArgument == "" {
		return fmt.Errorf("give the current schema as the first argument")
	}

	desiredArgument := command.StringArg("desired")
	if desiredArgument == "" {
		return fmt.Errorf("give the desired schema as the second argument")
	}

	current := driversshared.ParseDataSource(currentArgument)
	desired := driversshared.ParseDataSource(desiredArgument)

	config, path, err := helpers.ReadConfigWithFlags(command)
	if err != nil {
		return err
	}

	driverName := config.Driver
	if driverName == "" {
		detected, err := helpers.DetectDriverName(current, desired, config.ScratchDatabase)
		if err != nil {
			return err
		}

		driverName = detected
	}

	err = helpers.CheckDriverSettings(driverName, config.Schema, config.ServerVersion, config.ScratchDatabase, path)
	if err != nil {
		return err
	}

	driver, err := internaldrivers.NewDriver(driverName, internaldrivers.DriverOptions{
		ServerVersion:     config.ServerVersion,
		ScratchDatabase:   config.ScratchDatabase,
		Schema:            config.Schema,
		ComparePrivileges: command.Bool("privileges"),
		IgnoreTables:      config.IgnoreTables,
	})
	if err != nil {
		return err
	}

	instructions, err := driver.Diff(ctx, current, desired, driversshared.DiffOptions{
		CompareData:  command.Bool("data"),
		GuessRenames: command.Bool("guess-renames"),
	})
	if err != nil {
		return fmt.Errorf("failed to compare the schemas: %w", err)
	}

	if command.Bool("comments") {
		instructions = driversshared.AnnotateInstructions(instructions)
	}

	_, err = fmt.Fprintln(command.Root().Writer, instructions.String())
	if err != nil {
		return err
	}

	if command.Bool("exit-code") && len(instructions) > 0 {
		return ErrDifferencesFound
	}

	return nil
}
