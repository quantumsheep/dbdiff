package cmdmigrate

import (
	cmdmigrategenerate "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/generate"
	cmdmigratepreview "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/preview"
	cmdmigraterepair "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/repair"
	cmdmigratestatus "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/status"
	cmdmigratestep "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/step"
	cmdmigrateup "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/up"
	cmdmigrateverify "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/verify"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:         "migrate",
		Usage:        "Generate a migration file, and apply it to a database",
		OnUsageError: helpers.OnUsageError,
		Commands: []*cli.Command{
			cmdmigrategenerate.Command(),
			cmdmigratestatus.Command(),
			cmdmigratepreview.Command(),
			cmdmigrateup.Command(),
			cmdmigratestep.Command(),
			cmdmigrateverify.Command(),
			cmdmigraterepair.Command(),
		},
	}
}
