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
	rows, err := db.QueryContext(ctx, `
		SELECT master.name, master.sql
		FROM sqlite_master AS master
		WHERE master.type = 'table' AND master.name NOT LIKE 'sqlite_%'
		AND EXISTS (
			SELECT 1 FROM pragma_table_list AS list
			WHERE list.schema = 'main' AND list.name = master.name AND list.type = 'virtual'
		)
		ORDER BY master.rowid;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var tables []*SQLiteVirtualTable

	for rows.Next() {
		table := &SQLiteVirtualTable{}

		err := rows.Scan(&table.Name, &table.SQL)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (d *SQLiteDriver) DiffVirtualTables(ctx context.Context) ([]driversshared.Instruction, error) {
	targetTables, err := d.GetVirtualTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetVirtualTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetTables, sourceTables, driversshared.DiffRules[*SQLiteVirtualTable]{
		Key: func(table *SQLiteVirtualTable) string {
			return table.Name
		},
		Create: func(table *SQLiteVirtualTable) []driversshared.Instruction {
			return []driversshared.Instruction{table.CreateInstruction()}
		},
		Change: func(target *SQLiteVirtualTable, source *SQLiteVirtualTable) ([]driversshared.Instruction, error) {
			if target.SQL == source.SQL {
				return nil, nil
			}

			return []driversshared.Instruction{source.DropInstruction(), target.CreateInstruction()}, nil
		},
		Drop: func(table *SQLiteVirtualTable) []driversshared.Instruction {
			return []driversshared.Instruction{table.DropInstruction()}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}
