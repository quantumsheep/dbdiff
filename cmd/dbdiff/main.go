package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	cmddiff "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/diff"
	cmdmigrate "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/drivers"
	"github.com/urfave/cli/v3"
)

func main() {
	command := &cli.Command{
		Name:           "dbdiff",
		Version:        helpers.Version(),
		Usage:          "Compare database schemas and generate migration scripts",
		Description:    "Compare database schemas and generate migration scripts",
		UsageText:      "dbdiff [command] [options] <source> <target>",
		DefaultCommand: "diff",
		OnUsageError:   helpers.OnUsageError,
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
				Usage: "Name of the schema. The postgres driver accepts this flag. The default is the schema of the search path",
			},
		},
		Commands: []*cli.Command{
			cmddiff.Command(),
			cmdmigrate.Command(),
		},
	}

	err := command.Run(context.Background(), os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbdiff: %v\n", err)
		os.Exit(1)
	}
}
