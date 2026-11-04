package cmdroot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	cmddiff "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/diff"
	cmdmigrate "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:                  "dbdiff",
		Version:               helpers.Version(),
		Usage:                 "Compare database schemas and generate migration scripts",
		Description:           "Compare database schemas and generate migration scripts",
		UsageText:             "dbdiff <command> [options]",
		OnUsageError:          helpers.OnUsageError,
		EnableShellCompletion: true,
		// The default handler calls os.Exit inside Run. ExitCode handles the error instead.
		ExitErrHandler: func(ctx context.Context, command *cli.Command, err error) {},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "driver",
				Usage: "Database driver to use. Supported drivers: sqlite3, postgres, mysql. The default is the driver that dbdiff detects from the connection URLs",
				// An empty value starts the detection of DetectDriver.
				Validator: func(s string) error {
					if s == "" || slices.Contains(driversshared.SupportedDriverNames, driversshared.DriverName(s)) {
						return nil
					}

					return fmt.Errorf("unsupported driver: %s", s)
				},
			},
			&cli.StringFlag{
				Name:  "schema",
				Usage: "Name of the schema. The postgres driver accepts this flag. The default is the schema of the search path",
			},
			&cli.StringFlag{
				Name:  "server-version",
				Usage: "Version of the temporary server that runs the SQL files. The postgres driver and the mysql driver accept this flag",
			},
			&cli.StringFlag{
				Name:  "scratch-database",
				Usage: "Connection URL of an existing server that runs the SQL files instead of a temporary server. dbdiff creates its temporary databases on this server and drops them at the end. The user of the URL must have the privilege to create and drop a database. The postgres driver and the mysql driver accept this flag",
			},
			&cli.StringSliceFlag{
				Name:  "ignore-table",
				Usage: "Name of a table that the diff ignores. Repeat the flag for each table",
			},
		},
		Commands: []*cli.Command{
			cmddiff.Command(),
			cmdmigrate.Command(),
		},
	}
}

// ExitCode writes the message of the error of Run to errorWriter and returns the exit
// code of the process.
func ExitCode(err error, errorWriter io.Writer) int {
	if err == nil {
		return 0
	}

	if errors.Is(err, cmddiff.ErrDifferencesFound) {
		return 1
	}

	var exitCoder cli.ExitCoder

	if errors.As(err, &exitCoder) {
		message := exitCoder.Error()
		if message != "" {
			_, _ = fmt.Fprintln(errorWriter, message)
		}

		return exitCoder.ExitCode()
	}

	_, _ = fmt.Fprintf(errorWriter, "dbdiff: %v\n", err)

	return 1
}
