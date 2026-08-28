package drivers

import (
	"context"
	"database/sql"

	"github.com/samber/lo"
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

func (t *SQLiteVirtualTable) DropInstruction() *SQLDropTableInstruction {
	return &SQLDropTableInstruction{Name: t.Name}
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

func (d *SQLiteDriver) DiffVirtualTables(ctx context.Context) ([]Instruction, error) {
	targetTables, err := d.GetVirtualTables(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceTables, err := d.GetVirtualTables(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	var instructions []Instruction

	for _, targetTable := range targetTables {
		sourceTable, found := lo.Find(sourceTables, func(table *SQLiteVirtualTable) bool {
			return table.Name == targetTable.Name
		})
		if !found {
			instructions = append(instructions, targetTable.CreateInstruction())
			continue
		}

		if targetTable.SQL != sourceTable.SQL {
			instructions = append(instructions,
				sourceTable.DropInstruction(), targetTable.CreateInstruction())
		}
	}

	for _, sourceTable := range sourceTables {
		_, found := lo.Find(targetTables, func(table *SQLiteVirtualTable) bool {
			return table.Name == sourceTable.Name
		})
		if !found {
			instructions = append(instructions, sourceTable.DropInstruction())
		}
	}

	return instructions, nil
}
