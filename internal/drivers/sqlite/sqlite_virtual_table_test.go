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

	t.Run("VirtualTableNameInOtherCaseIsTheSameTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE Docs USING fts5(title, body);`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts5(title, body);`)
		driver.RequireInstructions(nil)
	})

	t.Run("VirtualTableWithOtherSpacingCommentsAndCase", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts5(title, body, tokenize = 'porter ascii');`)
		driver.ExecOnDesired(`
			create virtual table docs using FTS5 (
				title,
				body, -- the text
				TOKENIZE='porter ascii'
			);
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("ModifyAQuotedArgumentOfAVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts5(body, tokenize = 'porter ascii');`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts5(body, tokenize = 'unicode61');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "docs"},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts5(body, tokenize = 'unicode61')",
			},
		})
	})

	t.Run("IgnoredVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE VIRTUAL TABLE docs USING fts5(title, body);
			CREATE VIRTUAL TABLE notes USING fts5(body);
		`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE notes USING fts5(body, tags);`)

		driver.IgnoreTables = []string{"Docs", "notes"}

		driver.RequireInstructions(nil)
	})
}
