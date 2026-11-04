package driverssqlite

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// SQLite holds no ALTER statement for a virtual table, so a new definition prints a DROP
// statement and a CREATE statement. The module builds the shadow tables.
type SQLiteVirtualTable struct {
	Name string

	// The statement holds no semicolon.
	SQL string
}

func (t *SQLiteVirtualTable) CreateInstruction() *SQLiteCreateVirtualTableInstruction {
	return &SQLiteCreateVirtualTableInstruction{Definition: t.SQL}
}

func (t *SQLiteVirtualTable) DropInstruction() *driversshared.SQLDropTableInstruction {
	return &driversshared.SQLDropTableInstruction{Name: t.Name}
}

func (d *SQLiteDriver) GetVirtualTables(ctx context.Context, db *sql.DB) ([]*SQLiteVirtualTable, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT master.name, master.sql
		FROM sqlite_master AS master
		WHERE master.type = 'table' AND master.name NOT LIKE 'sqlite_%'
		AND EXISTS (
			SELECT 1 FROM pragma_table_list AS list
			WHERE list.schema = 'main' AND list.name = master.name AND list.type = 'virtual'
		)
		ORDER BY master.rowid;
	`, func(rows *sql.Rows) (*SQLiteVirtualTable, error) {
		table := &SQLiteVirtualTable{}

		err := rows.Scan(&table.Name, &table.SQL)

		return table, err
	})
}

func (d *SQLiteDriver) DiffVirtualTables(ctx context.Context) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, d.GetVirtualTables, driversshared.ReplaceRules(
		func(table *SQLiteVirtualTable) string {
			return table.Name
		},
		func(desired *SQLiteVirtualTable, current *SQLiteVirtualTable) bool {
			return desired.SQL == current.SQL
		},
		func(table *SQLiteVirtualTable) driversshared.Instruction {
			return table.CreateInstruction()
		},
		func(table *SQLiteVirtualTable) driversshared.Instruction {
			return table.DropInstruction()
		}))
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}
