package driverssqlite

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestSQLiteVirtualTable(t *testing.T) {
	t.Run("CreateVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE VIRTUAL TABLE docs USING fts5(title, body);
			CREATE TABLE plain (id INTEGER);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "plain",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts5(title, body)",
			},
		})
	})

	t.Run("DropVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts5(title, body);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "docs"},
		})
	})

	t.Run("ModifyVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts5(title, body);`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts5(title, body, tags);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "docs"},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts5(title, body, tags)",
			},
		})
	})

	t.Run("EqualVirtualTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts5(title, body);`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts5(title, body);`)
		driver.RequireInstructions(nil)
	})
}
