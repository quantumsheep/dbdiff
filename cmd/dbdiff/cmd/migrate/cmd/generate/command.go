package cmdmigrategenerate

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/helpers"
	"github.com/quantumsheep/dbdiff/cmd/dbdiff/internal/migrations"
	"github.com/quantumsheep/dbdiff/internal/drivers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
	"github.com/urfave/cli/v3"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Write the next migration file",
		UsageText: "dbdiff migrate generate [options] <name>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: "Path of the configuration file. The default is dbdiff.yaml of the working directory",
			},
			&cli.StringFlag{
				Name:  "source",
				Usage: "Directory of the migration files",
			},
			&cli.StringFlag{
				Name:  "target",
				Usage: "The wanted schema. A .sql file, a directory of .sql files, or a connection URL. The default is the DBDIFF_TARGET variable",
			},
		},
		OnUsageError: helpers.OnUsageError,
		Arguments: []cli.Argument{
			&cli.StringArg{
				Name:      "name",
				UsageText: "Short name of the migration, for example add_created_at",
			},
		},
		Action: action,
	}
}

func action(ctx context.Context, command *cli.Command) error {
	config, err := migrations.GetMigrationConfigFromCommand(command)
	if err != nil {
		return err
	}

	name := command.StringArg("name")
	if name == "" {
		return fmt.Errorf("name the migration, for example add_created_at")
	}

	slug := coremigrations.MigrationSlug(name)
	if slug == "" {
		return fmt.Errorf("the name of a migration holds no letter and no digit")
	}

	err = os.MkdirAll(config.Source, 0o750)
	if err != nil {
		return err
	}

	driver, err := drivers.NewDriver(ctx, config.Driver, config.Source, config.Target,
		config.Schema, config.Version, false, false, config.Ignore.Tables)
	if err != nil {
		return err
	}

	defer func() { _ = driver.Close() }()

	instructions, err := driver.Diff(ctx)
	if err != nil {
		return err
	}

	paths, err := coremigrations.WriteMigrationFiles(config.Source, slug,
		time.Now(), helpers.Version(), instructions)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return fmt.Errorf("the migrations hold the schema of the source already, so dbdiff wrote no file")
	}

	for _, path := range paths {
		fmt.Println(path)
	}

	return nil
}
