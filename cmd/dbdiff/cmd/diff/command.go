package cmddiff

import (
	"context"
	"fmt"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/drivers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	dbdiffdrivers "github.com/quantumsheep/dbdiff/drivers"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:         "diff",
		Usage:        "Compare two schemas and print the statements that change the target",
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

	driver, err := drivers.NewDriver(ctx, command.String("driver"), currentSchema, finalSchema,
		command.String("schema"), "", command.Bool("data"), command.Bool("privileges"))
	if err != nil {
		return err
	}

	defer func() { _ = driver.Close() }()

	instructions, err := driver.Diff(ctx)
	if err != nil {
		return fmt.Errorf("failed to diff databases: %w", err)
	}

	if command.Bool("comments") {
		instructions = dbdiffdrivers.AnnotateInstructions(instructions)
	}

	fmt.Println(dbdiffdrivers.RenderInstructions(instructions))

	return nil
}
