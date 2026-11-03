package cmdmigrate

import (
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:         "migrate",
		Usage:        "Generate a migration file, and apply it to a database",
		OnUsageError: helpers.OnUsageError,
		Commands: []*cli.Command{
			generateCommand(),
			statusCommand(),
			previewCommand(),
			upCommand(),
			stepCommand(),
			verifyCommand(),
			repairCommand(),
			baselineCommand(),
		},
	}
}
