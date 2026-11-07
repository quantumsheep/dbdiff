package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	embeddedmysql "github.com/quantumsheep/embedded-mysql"
	"github.com/samber/lo"
)

type MySQLDriverConfig struct {
	ComparePrivileges    bool
	ScratchServerVersion string
	ScratchDatabase      string
	IgnoreTables         []string
}

type MySQLDriver struct {
	DesiredDatabaseConnection *sql.DB
	CurrentDatabaseConnection *sql.DB
	ComparePrivileges         bool
	ScratchServerVersion      string
	ScratchDatabase           string
	IgnoreTables              []string

	scratchServer       *embeddedmysql.EmbeddedMySQL
	scratchDatabases    []*mysqlScratchDatabase
	detailsByConnection map[*sql.DB]*mysqlConnectionDetails
}

type mysqlConnectionDetails struct {
	mariadb          bool
	databaseName     string
	defaultEngine    string
	defaultCollation string
}

func isMariaDB(version string) bool {
	return strings.Contains(strings.ToLower(version), "mariadb")
}

func NewMySQLDriver(config *MySQLDriverConfig) *MySQLDriver {
	return &MySQLDriver{
		ComparePrivileges:    config.ComparePrivileges,
		ScratchServerVersion: config.ScratchServerVersion,
		ScratchDatabase:      config.ScratchDatabase,
		IgnoreTables:         config.IgnoreTables,
	}
}

func (d *MySQLDriver) registerConnection(ctx context.Context, db *sql.DB) error {
	row := db.QueryRowContext(ctx, `
		SELECT VERSION(), DATABASE(), @@default_storage_engine,
			(SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA
				WHERE SCHEMA_NAME = DATABASE());
	`)

	var version, defaultEngine string
	var databaseName, defaultCollation sql.NullString

	err := row.Scan(&version, &databaseName, &defaultEngine, &defaultCollation)
	if err != nil {
		return err
	}

	if !databaseName.Valid {
		return fmt.Errorf("the connection names no database")
	}

	d.detailsByConnection[db] = &mysqlConnectionDetails{
		mariadb:          isMariaDB(version),
		databaseName:     databaseName.String,
		defaultEngine:    defaultEngine,
		defaultCollation: defaultCollation.String,
	}

	return nil
}

func (d *MySQLDriver) openSides(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (func(), error) {
	d.detailsByConnection = make(map[*sql.DB]*mysqlConnectionDetails)

	desiredDatabaseConnection, err := d.OpenSide(ctx, desired, current, "desired")
	if err != nil {
		_ = d.releaseScratchResources(ctx)
		return nil, err
	}

	d.DesiredDatabaseConnection = desiredDatabaseConnection

	err = d.registerConnection(ctx, desiredDatabaseConnection)
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	currentDatabaseConnection, err := d.OpenSide(ctx, current, desired, "current")
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	d.CurrentDatabaseConnection = currentDatabaseConnection

	err = d.registerConnection(ctx, currentDatabaseConnection)
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.CurrentDatabaseConnection.Close()
		_ = d.releaseScratchResources(ctx)

		return nil, err
	}

	return func() {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.CurrentDatabaseConnection.Close()
		d.DesiredDatabaseConnection = nil
		d.CurrentDatabaseConnection = nil
		_ = d.releaseScratchResources(ctx)
	}, nil
}

// If the context of the diff stops, the cleanup must still run. The time limit stops a
// cleanup that waits for a server that does not answer.
func (d *MySQLDriver) releaseScratchResources(ctx context.Context) error {
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	var firstError error

	for _, scratch := range d.scratchDatabases {
		err := scratch.drop(cleanupContext)
		if err != nil && firstError == nil {
			firstError = err
		}
	}

	d.scratchDatabases = nil

	err := d.StopScratchServer()
	if err != nil && firstError == nil {
		firstError = err
	}

	return firstError
}

func (d *MySQLDriver) Diff(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
	options driversshared.DiffOptions,
) (driversshared.Instructions, error) {
	release, err := d.openSides(ctx, current, desired)
	if err != nil {
		return nil, err
	}

	defer release()

	desiredTables, err := d.GetTables(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentTables, err := d.GetTables(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	steps := []func(context.Context) ([]driversshared.Instruction, error){
		// A column default can read a sequence, so the sequences come before the tables.
		d.DiffSequences,
		func(context.Context) ([]driversshared.Instruction, error) {
			return d.DiffTables(desiredTables, currentTables)
		},
		// A view can call a function, and MySQL checks the call at the creation of the view,
		// so the routines come before the views.
		d.DiffRoutines,
		d.DiffViews,
		d.DiffEvents,
	}

	if d.ComparePrivileges {
		steps = append(steps, d.DiffPrivileges)
	}

	if options.CompareData {
		steps = append(steps, func(ctx context.Context) ([]driversshared.Instruction, error) {
			return d.DiffData(ctx, desiredTables, currentTables)
		})
	}

	var instructions []driversshared.Instruction

	for _, step := range steps {
		stepInstructions, err := step(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, stepInstructions...)
	}

	if needsForeignKeyChecksOff(desiredTables, currentTables) || dataNeedsForeignKeyChecksOff(instructions, desiredTables) {
		instructions = append([]driversshared.Instruction{&MySQLSetForeignKeyChecksInstruction{}}, instructions...)
		instructions = append(instructions, &MySQLSetForeignKeyChecksInstruction{Enabled: true})
	}

	return instructions, nil
}

func (d *MySQLDriver) DiffTables(
	desiredTables []*MySQLTable,
	currentTables []*MySQLTable,
) ([]driversshared.Instruction, error) {
	columnsDiffs := map[string]*MySQLTableColumnsDiff{}

	for _, desiredTable := range desiredTables {
		currentTable, found := lo.Find(currentTables, func(table *MySQLTable) bool {
			return table.Name == desiredTable.Name
		})
		if found {
			columnsDiffs[desiredTable.Name] = desiredTable.DiffColumns(currentTable)
		}
	}

	var dropDependents []driversshared.Instruction
	var addDependents []driversshared.Instruction

	desiredForeignKeyNames := lo.FlatMap(desiredTables, func(table *MySQLTable, _ int) []string {
		return lo.Map(table.ForeignKeys, func(foreignKey *MySQLForeignKey, _ int) string {
			return foreignKey.Name
		})
	})
	desiredCheckNames := lo.FlatMap(desiredTables, func(table *MySQLTable, _ int) []string {
		return lo.Map(table.CheckConstraints, func(check *MySQLCheckConstraint, _ int) string {
			return check.Name
		})
	})

	additions, removals, err := driversshared.DiffByKey(desiredTables, currentTables, driversshared.DiffRules[*MySQLTable]{
		Key: func(table *MySQLTable) string {
			return table.Name
		},
		Create: func(table *MySQLTable) []driversshared.Instruction {
			return table.Instructions()
		},
		Change: func(desired *MySQLTable, current *MySQLTable) ([]driversshared.Instruction, error) {
			tableDiff, err := desired.DiffTable(current, columnsDiffs)
			if err != nil {
				return nil, err
			}

			dropDependents = append(dropDependents, tableDiff.DropDependents...)
			addDependents = append(addDependents, tableDiff.AddDependents...)

			return tableDiff.Alter, nil
		},
		// The constraint names are unique in the schema, and the table drops come last. So a
		// name that a desired table takes again must be free before the creations.
		Drop: func(table *MySQLTable) []driversshared.Instruction {
			for _, foreignKey := range table.ForeignKeys {
				if slices.Contains(desiredForeignKeyNames, foreignKey.Name) {
					dropDependents = append(dropDependents, &MySQLAlterTableInstruction{
						Name: table.Name,
						Action: &MySQLDropForeignKeyAction{
							Name: foreignKey.Name,
						},
					})
				}
			}

			for _, check := range table.CheckConstraints {
				if slices.Contains(desiredCheckNames, check.Name) {
					dropDependents = append(dropDependents, &MySQLAlterTableInstruction{
						Name: table.Name,
						Action: &MySQLDropCheckConstraintAction{
							Name: check.Name,
						},
					})
				}
			}

			return []driversshared.Instruction{&MySQLDropTableInstruction{
				Name: table.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	instructions := append(dropDependents, additions...)
	instructions = append(instructions, addDependents...)

	return append(instructions, removals...), nil
}

// The creation order and the removal order of the tables read no dependency, so a foreign
// key of a created table or of a dropped table needs the enforcement off.
func needsForeignKeyChecksOff(desiredTables []*MySQLTable, currentTables []*MySQLTable) bool {
	tableByName := func(tables []*MySQLTable, name string) (*MySQLTable, bool) {
		return lo.Find(tables, func(table *MySQLTable) bool {
			return table.Name == name
		})
	}

	for _, desiredTable := range desiredTables {
		_, found := tableByName(currentTables, desiredTable.Name)
		if !found && len(desiredTable.ForeignKeys) > 0 {
			return true
		}

		// A kept table can gain a key to a new table. The new table holds no row before the
		// data diff, so a check of the current rows can fail.
		for _, foreignKey := range desiredTable.ForeignKeys {
			_, referencedExists := tableByName(currentTables, foreignKey.ReferencedTable)
			if !referencedExists {
				return true
			}
		}
	}

	for _, currentTable := range currentTables {
		_, found := tableByName(desiredTables, currentTable.Name)
		if found {
			continue
		}

		if len(currentTable.ForeignKeys) > 0 {
			return true
		}

		referenced := lo.SomeBy(currentTables, func(table *MySQLTable) bool {
			return lo.SomeBy(table.ForeignKeys, func(foreignKey *MySQLForeignKey) bool {
				return foreignKey.ReferencedTable == currentTable.Name
			})
		})
		if referenced {
			return true
		}
	}

	return false
}

func (d *MySQLDriver) DiffViews(ctx context.Context) ([]driversshared.Instruction, error) {
	getSortedViews := func(ctx context.Context, db *sql.DB) ([]*MySQLView, error) {
		views, err := d.GetViews(ctx, db)
		if err != nil {
			return nil, err
		}

		return sortViewsByDependency(views), nil
	}

	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, getSortedViews, driversshared.DiffRules[*MySQLView]{
		Key: func(view *MySQLView) string {
			return view.Name
		},
		Create: func(view *MySQLView) []driversshared.Instruction {
			return view.Instructions()
		},
		Change: func(desired *MySQLView, current *MySQLView) ([]driversshared.Instruction, error) {
			return desired.Diff(current)
		},
		Drop: func(view *MySQLView) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropViewInstruction{
				Name: view.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}
