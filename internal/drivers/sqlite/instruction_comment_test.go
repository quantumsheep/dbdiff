package driverssqlite

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func commentInstruction(text string) driversshared.Instruction {
	return &driversshared.SQLCommentInstruction{
		Text: text,
	}
}

func TestInstructionComments(t *testing.T) {
	t.Run("SQLiteAlterTable", func(t *testing.T) {
		instruction := &SQLiteAlterTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateTable", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Create the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateVirtualTable", func(t *testing.T) {
		instruction := &SQLiteCreateVirtualTableInstruction{
			Definition: `CREATE VIRTUAL TABLE "documents" USING fts5(title, body)`,
		}

		require.Equal(t, `Create the virtual table "documents"`, instruction.Comment())
	})

	t.Run("SQLiteTableRecreationInstruction", func(t *testing.T) {
		instruction := &SQLiteTableRecreationInstruction{
			Instruction: &driversshared.SQLDropTableInstruction{
				Name: "users",
			},
			TableName: "users",
		}

		require.Equal(t, `Recreate the table "users"`, instruction.Comment())
	})

	t.Run("SQLitePragmaForeignKeys", func(t *testing.T) {
		require.Equal(t, "Turn the enforcement of the foreign keys off for the recreation of a table",
			(&SQLitePragmaForeignKeysInstruction{}).Comment())
		require.Equal(t, "Turn the enforcement of the foreign keys on again",
			(&SQLitePragmaForeignKeysInstruction{Enabled: true}).Comment())
	})

	t.Run("SQLiteCreateIndex", func(t *testing.T) {
		instruction := &SQLiteCreateIndexInstruction{
			Name:      "users_email",
			TableName: "users",
		}

		require.Equal(t, `Create the index "users_email" of the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateTrigger", func(t *testing.T) {
		instruction := &SQLiteCreateTriggerInstruction{
			Definition: `CREATE TRIGGER "log_insert" AFTER INSERT ON "users" BEGIN SELECT 1; END`,
		}

		require.Equal(t, `Create the trigger "log_insert" of the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateView", func(t *testing.T) {
		instruction := &SQLiteCreateViewInstruction{
			Definition: `CREATE VIEW "active_users" AS SELECT id FROM users`,
		}

		require.Equal(t, `Create the view "active_users"`, instruction.Comment())
	})

	t.Run("SQLiteDropTrigger", func(t *testing.T) {
		instruction := &SQLiteDropTriggerInstruction{
			Name: "log_delete",
		}

		require.Equal(t, `Drop the trigger "log_delete"`, instruction.Comment())
	})

	t.Run("DefinitionWithAQuotedName", func(t *testing.T) {
		instruction := &SQLiteCreateViewInstruction{
			Definition: `CREATE VIEW "we""ird" AS SELECT 1`,
		}

		require.Equal(t, `Create the view "we""ird"`, instruction.Comment())
	})
}

func TestAnnotateInstructions(t *testing.T) {
	t.Run("OneCommentForEachObject", func(t *testing.T) {
		createTable := &SQLiteCreateTableInstruction{
			Name: "users",
		}
		dropTable := &driversshared.SQLDropTableInstruction{
			Name: "audit",
		}

		require.Equal(t, driversshared.Instructions{
			commentInstruction(`Create the table "users"`),
			createTable,
			commentInstruction(`Drop the table "audit"`),
			dropTable,
		}, driversshared.AnnotateInstructions([]driversshared.Instruction{createTable, dropTable}))
	})

	t.Run("TableRecreation", func(t *testing.T) {
		createTemporaryTable := &SQLiteTableRecreationInstruction{
			Instruction: &SQLiteCreateTableInstruction{
				Name: "_users_temp",
			},
			TableName: "users",
		}
		copyRows := &SQLiteTableRecreationInstruction{
			Instruction: &driversshared.SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id"},
				SelectExpressions: []string{`"id"`},
				SourceTableName:   "users",
			},
			TableName: "users",
		}
		dropTable := &SQLiteTableRecreationInstruction{
			Instruction: &driversshared.SQLDropTableInstruction{
				Name: "users",
			},
			TableName: "users",
		}
		renameTable := &SQLiteTableRecreationInstruction{
			Instruction: &SQLiteAlterTableInstruction{
				Name: "_users_temp",
				Action: &driversshared.SQLRenameTableAction{
					NewName: "users",
				},
			},
			TableName: "users",
		}
		createIndex := &SQLiteTableRecreationInstruction{
			Instruction: &SQLiteCreateIndexInstruction{
				Name:      "users_email",
				TableName: "users",
			},
			TableName: "users",
		}
		createTrigger := &SQLiteTableRecreationInstruction{
			Instruction: &SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER "log_insert" AFTER INSERT ON "users" BEGIN SELECT 1; END`,
			},
			TableName: "users",
		}
		createOtherTable := &SQLiteCreateTableInstruction{
			Name: "posts",
		}

		require.Equal(t, driversshared.Instructions{
			commentInstruction(`Recreate the table "users"`),
			createTemporaryTable,
			copyRows,
			dropTable,
			renameTable,
			createIndex,
			createTrigger,
			commentInstruction(`Create the table "posts"`),
			createOtherTable,
		}, driversshared.AnnotateInstructions([]driversshared.Instruction{
			createTemporaryTable,
			copyRows,
			dropTable,
			renameTable,
			createIndex,
			createTrigger,
			createOtherTable,
		}))
	})

	t.Run("TemporaryTableWithoutARename", func(t *testing.T) {
		createTemporaryTable := &SQLiteCreateTableInstruction{
			Name: "_users_temp",
		}

		require.Equal(t, driversshared.Instructions{
			commentInstruction(`Create the table "_users_temp"`),
			createTemporaryTable,
		}, driversshared.AnnotateInstructions([]driversshared.Instruction{createTemporaryTable}))
	})
}
