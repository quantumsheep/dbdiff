package driverssqlite

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	_ "modernc.org/sqlite"
)

type SQLiteDriverConfig struct {
	IgnoreTables []string
}

type SQLiteDriver struct {
	DesiredDatabaseConnection *sql.DB
	CurrentDatabaseConnection *sql.DB
	IgnoreTables              []string

	temporaryDirectory string
}

const recreationWarning = "This script recreates a table. Apply it with sqlite3 -bail. " +
	"Without -bail, a failed copy deletes the rows of the table."

func NewSQLiteDriver(config *SQLiteDriverConfig) *SQLiteDriver {
	return &SQLiteDriver{
		IgnoreTables: config.IgnoreTables,
	}
}

func TrimSQLitePrefix(path string) string {
	return strings.TrimPrefix(path, "sqlite://")
}

func (d *SQLiteDriver) Diff(
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

	steps := []func(context.Context) ([]driversshared.Instruction, error){
		func(ctx context.Context) ([]driversshared.Instruction, error) {
			return d.DiffTables(ctx, options.GuessRenames)
		},
		d.DiffVirtualTables,
		d.DiffViews,
	}

	if options.CompareData {
		steps = append(steps, d.DiffData)
	}

	var instructions []driversshared.Instruction

	for _, step := range steps {
		stepInstructions, err := step(ctx)
		if err != nil {
			return nil, err
		}

		instructions = append(instructions, stepInstructions...)
	}

	// SQLite refuses the DROP TABLE of a recreation while another table names the
	// dropped table and the enforcement of the foreign keys is on. PRAGMA foreign_keys does
	// nothing inside a transaction, so it comes before the savepoint. With the enforcement
	// off, only the check finds a broken foreign key, so it comes before the release.
	recreatedTableNames := recreatedTableNames(instructions)
	if len(recreatedTableNames) > 0 {
		// Without -bail, the sqlite3 shell runs the next statement after a failed statement. A
		// failed copy then lets the DROP TABLE statement and the RELEASE statement run.
		instructions = slices.Concat(
			[]driversshared.Instruction{
				&driversshared.SQLCommentInstruction{
					Text: recreationWarning,
				},
				&SQLitePragmaForeignKeysInstruction{},
				&SQLiteSavepointInstruction{},
			},
			instructions,
			[]driversshared.Instruction{
				&SQLiteForeignKeyCheckInstruction{
					Step: createForeignKeyCheckTable,
				},
				&SQLiteForeignKeyCheckInstruction{
					Step:                countForeignKeyViolations,
					RecreatedTableNames: recreatedTableNames,
				},
				&SQLiteForeignKeyCheckInstruction{
					Step: dropForeignKeyCheckTable,
				},
				&SQLiteSavepointInstruction{
					Release: true,
				},
				&SQLitePragmaForeignKeysInstruction{
					Enabled: true,
				},
			})
	}

	return instructions, nil
}

func (d *SQLiteDriver) openSides(
	ctx context.Context,
	current driversshared.DataSource,
	desired driversshared.DataSource,
) (func(), error) {
	desiredDatabaseConnection, err := d.OpenSide(ctx, desired, "desired")
	if err != nil {
		_ = d.RemoveTemporaryDirectory()

		return nil, err
	}

	d.DesiredDatabaseConnection = desiredDatabaseConnection

	currentDatabaseConnection, err := d.OpenSide(ctx, current, "current")
	if err != nil {
		_ = d.DesiredDatabaseConnection.Close()
		_ = d.RemoveTemporaryDirectory()

		return nil, err
	}

	d.CurrentDatabaseConnection = currentDatabaseConnection

	return func() {
		_ = d.CurrentDatabaseConnection.Close()
		_ = d.DesiredDatabaseConnection.Close()
		d.CurrentDatabaseConnection = nil
		d.DesiredDatabaseConnection = nil
		_ = d.RemoveTemporaryDirectory()
	}, nil
}

// SQLite compares two identifiers without the case of the ASCII letters. It keeps the case
// of every other letter.
func identifierKey(name string) string {
	return strings.Map(func(character rune) rune {
		if character >= 'A' && character <= 'Z' {
			return character + ('a' - 'A')
		}

		return character
	}, name)
}

func recreatedTableNames(instructions []driversshared.Instruction) []string {
	var names []string

	for _, instruction := range instructions {
		recreation, isRecreation := instruction.(*SQLiteTableRecreationInstruction)
		if isRecreation && !slices.Contains(names, recreation.TableName) {
			names = append(names, recreation.TableName)
		}
	}

	return names
}

func (d *SQLiteDriver) DiffTables(ctx context.Context, guessRenames bool) ([]driversshared.Instruction, error) {
	desiredTables, err := d.GetTables(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentTables, err := d.GetTables(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	reservedNames, err := d.getReservedNames(ctx)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(desiredTables, currentTables, driversshared.DiffRules[*SQLiteTable]{
		Key: func(table *SQLiteTable) string {
			return identifierKey(table.Name)
		},
		Create: func(table *SQLiteTable) []driversshared.Instruction {
			return table.Instructions()
		},
		Change: func(desired *SQLiteTable, current *SQLiteTable) ([]driversshared.Instruction, error) {
			return desired.DiffTable(current, reservedNames, guessRenames)
		},
		Drop: func(table *SQLiteTable) []driversshared.Instruction {
			return []driversshared.Instruction{&driversshared.SQLDropTableInstruction{
				Name: table.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	// The reverse order gives each DROP TABLE statement before the table that it names.
	slices.Reverse(removals)

	return append(additions, removals...), nil
}

// A table, an index, and a view share one namespace. The set holds the key of each object
// of the two sides, so the temporary table of a recreation can take a free name.
func (d *SQLiteDriver) getReservedNames(ctx context.Context) (map[string]bool, error) {
	reservedNames := make(map[string]bool)

	for _, db := range []*sql.DB{d.CurrentDatabaseConnection, d.DesiredDatabaseConnection} {
		names, err := driversshared.QueryAll(ctx, db, "SELECT name FROM sqlite_master ORDER BY name;", func(rows *sql.Rows) (string, error) {
			var name string

			err := rows.Scan(&name)

			return name, err
		})
		if err != nil {
			return nil, err
		}

		for _, name := range names {
			reservedNames[identifierKey(name)] = true
		}
	}

	return reservedNames, nil
}

func (d *SQLiteDriver) DiffViews(ctx context.Context) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, d.GetViews, driversshared.DiffRules[*SQLiteView]{
		Key: func(view *SQLiteView) string {
			return identifierKey(view.Name)
		},
		Create: func(view *SQLiteView) []driversshared.Instruction {
			return view.Instructions()
		},
		Change: func(desired *SQLiteView, current *SQLiteView) ([]driversshared.Instruction, error) {
			return desired.Diff(current)
		},
		Drop: func(view *SQLiteView) []driversshared.Instruction {
			return []driversshared.Instruction{&driversshared.SQLDropViewInstruction{
				Name: view.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}
