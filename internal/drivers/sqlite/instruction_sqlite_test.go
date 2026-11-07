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

func TestSQLiteInstructions(t *testing.T) {
	t.Run("SQLiteTableRecreationInstruction", func(t *testing.T) {
		instruction := &SQLiteTableRecreationInstruction{
			Instruction: &driversshared.SQLDropTableInstruction{
				Name: "users",
			},
			TableName: "users",
		}

		require.Equal(t, `DROP TABLE "users";`, instruction.String())
	})

	t.Run("SQLiteAlterTableAddColumn", func(t *testing.T) {
		instruction := &SQLiteAlterTableInstruction{
			Name: "users",
			Action: &SQLiteAddColumnAction{
				Column: &SQLiteColumn{
					Name: "email",
					Type: "TEXT",
				},
			},
		}

		require.Equal(t, `ALTER TABLE "users" ADD COLUMN "email" TEXT;`, instruction.String())
	})

	t.Run("SQLiteAlterTableRenameColumn", func(t *testing.T) {
		instruction := &SQLiteAlterTableInstruction{
			Name: "users",
			Action: &driversshared.SQLRenameColumnAction{
				ColumnName:    "name",
				NewColumnName: "full_name",
			},
		}

		require.Equal(t,
			`ALTER TABLE "users" RENAME COLUMN "name" TO "full_name";`,
			instruction.String())
	})

	t.Run("SQLiteCreateTable", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "users",
			Columns: []*SQLiteColumn{
				{
					Name:       "id",
					Type:       "INTEGER",
					PrimaryKey: true,
				},
				{
					Name:    "name",
					Type:    "TEXT",
					NotNull: true,
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" INTEGER PRIMARY KEY,
	"name" TEXT NOT NULL
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithATableConstraint", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "memberships",
			Columns: []*SQLiteColumn{
				{
					Name:    "team",
					Type:    "TEXT",
					NotNull: true,
				},
				{
					Name:    "member",
					Type:    "TEXT",
					NotNull: true,
				},
			},
			PrimaryKey: []string{"team", "member"},
			UniqueConstraints: []*SQLiteUniqueConstraint{
				{Columns: []string{"member"}},
			},
			ForeignKeys: []*SQLiteForeignKey{
				{
					Table: "teams",
					From:  []string{"team"},
					To:    []string{"name"},
				},
			},
		}

		require.Equal(t, `CREATE TABLE "memberships" (
	"team" TEXT NOT NULL,
	"member" TEXT NOT NULL,
	PRIMARY KEY ("team", "member"),
	UNIQUE ("member"),
	FOREIGN KEY ("team") REFERENCES "teams" ("name")
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithANamedPrimaryKey", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "memberships",
			Columns: []*SQLiteColumn{
				{
					Name: "team",
					Type: "TEXT",
				},
			},
			PrimaryKey:     []string{"team"},
			PrimaryKeyName: "pk_memberships",
		}

		require.Equal(t, `CREATE TABLE "memberships" (
	"team" TEXT,
	CONSTRAINT "pk_memberships" PRIMARY KEY ("team")
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithAForeignKeyWithoutParentColumns", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "children",
			Columns: []*SQLiteColumn{
				{
					Name: "parent",
					Type: "INTEGER",
				},
			},
			ForeignKeys: []*SQLiteForeignKey{
				{
					Table: "parents",
					From:  []string{"parent"},
				},
			},
		}

		require.Equal(t, `CREATE TABLE "children" (
	"parent" INTEGER,
	FOREIGN KEY ("parent") REFERENCES "parents"
);`, instruction.String())
	})

	t.Run("SQLitePragmaForeignKeys", func(t *testing.T) {
		require.Equal(t, "PRAGMA foreign_keys = OFF;",
			(&SQLitePragmaForeignKeysInstruction{}).String())
		require.Equal(t, "PRAGMA foreign_keys = ON;",
			(&SQLitePragmaForeignKeysInstruction{Enabled: true}).String())
	})

	t.Run("SQLiteCreateIndex", func(t *testing.T) {
		instruction := &SQLiteCreateIndexInstruction{
			Name:      "idx_users_name",
			TableName: "users",
			Keys:      []string{`"name"`},
		}

		require.Equal(t,
			`CREATE INDEX "idx_users_name" ON "users" ("name");`,
			instruction.String())
	})

	t.Run("SQLiteCreateUniquePartialIndex", func(t *testing.T) {
		instruction := &SQLiteCreateIndexInstruction{
			Unique:    true,
			Name:      "idx_users_active",
			TableName: "users",
			Keys:      []string{`"name"`, "lower(email)"},
			Where:     "active = 1",
		}

		require.Equal(t,
			`CREATE UNIQUE INDEX "idx_users_active" ON "users" ("name", lower(email)) WHERE active = 1;`,
			instruction.String())
	})

	t.Run("SQLiteCreateTrigger", func(t *testing.T) {
		instruction := &SQLiteCreateTriggerInstruction{
			Definition: "CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END",
		}

		require.Equal(t,
			"CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END;",
			instruction.String())
	})

	t.Run("SQLiteCreateView", func(t *testing.T) {
		instruction := &SQLiteCreateViewInstruction{
			Definition: "CREATE VIEW user_ids AS SELECT id FROM users",
		}

		require.Equal(t,
			"CREATE VIEW user_ids AS SELECT id FROM users;",
			instruction.String())
	})

	t.Run("SQLiteDropTrigger", func(t *testing.T) {
		instruction := &SQLiteDropTriggerInstruction{Name: "set_timestamp"}

		require.Equal(t, `DROP TRIGGER "set_timestamp";`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithGeneratedColumns", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "measures",
			Columns: []*SQLiteColumn{
				{
					Name: "value",
					Type: "INTEGER",
				},
				{
					Name:                "stored_double",
					Type:                "INTEGER",
					GeneratedExpression: "(value * 2)",
					GeneratedStored:     true,
				},
				{
					Name:                "virtual_triple",
					Type:                "INTEGER",
					GeneratedExpression: "(value * 3)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "measures" (
	"value" INTEGER,
	"stored_double" INTEGER GENERATED ALWAYS AS (value * 2) STORED,
	"virtual_triple" INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithATypelessGeneratedColumn", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "lines",
			Columns: []*SQLiteColumn{
				{
					Name: "price",
					Type: "INTEGER",
				},
				{
					Name:                "doubled",
					GeneratedExpression: "(price * 2)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "lines" (
	"price" INTEGER,
	"doubled" GENERATED ALWAYS AS (price * 2) VIRTUAL
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithoutRowID", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "sessions",
			Columns: []*SQLiteColumn{
				{
					Name:       "id",
					Type:       "TEXT",
					PrimaryKey: true,
				},
			},
			WithoutRowID: true,
		}

		require.Equal(t, `CREATE TABLE "sessions" (
	"id" TEXT PRIMARY KEY
) WITHOUT ROWID;`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithoutRowIDAndStrict", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "sessions",
			Columns: []*SQLiteColumn{
				{
					Name:       "id",
					Type:       "TEXT",
					PrimaryKey: true,
				},
			},
			WithoutRowID: true,
			Strict:       true,
		}

		require.Equal(t, `CREATE TABLE "sessions" (
	"id" TEXT PRIMARY KEY
) WITHOUT ROWID, STRICT;`, instruction.String())
	})

	t.Run("SQLiteCreateTableStrict", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "events",
			Columns: []*SQLiteColumn{
				{
					Name: "id",
					Type: "INTEGER",
				},
			},
			Strict: true,
		}

		require.Equal(t, `CREATE TABLE "events" (
	"id" INTEGER
) STRICT;`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithAutoIncrement", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "logs",
			Columns: []*SQLiteColumn{
				{
					Name:          "id",
					Type:          "INTEGER",
					PrimaryKey:    true,
					AutoIncrement: true,
				},
			},
		}

		require.Equal(t, `CREATE TABLE "logs" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithCollationAndCheck", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "people",
			Columns: []*SQLiteColumn{
				{
					Name:      "name",
					Type:      "TEXT",
					Collation: "NOCASE",
				},
				{
					Name:   "age",
					Type:   "INTEGER",
					Checks: "CHECK (age > 0)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "people" (
	"name" TEXT COLLATE "NOCASE",
	"age" INTEGER CHECK (age > 0)
);`, instruction.String())
	})

	t.Run("SQLiteCreateTableWithATableCheck", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "people",
			Columns: []*SQLiteColumn{
				{
					Name: "name",
					Type: "TEXT",
				},
			},
			CheckConstraints: []*SQLiteCheckConstraint{
				{Expression: "(length(name) < 100)"},
			},
		}

		require.Equal(t, `CREATE TABLE "people" (
	"name" TEXT,
	CHECK (length(name) < 100)
);`, instruction.String())
	})

	t.Run("SQLiteCreateVirtualTable", func(t *testing.T) {
		instruction := &SQLiteCreateVirtualTableInstruction{
			Definition: "CREATE VIRTUAL TABLE docs USING fts4(title, body)",
		}

		require.Equal(t,
			`CREATE VIRTUAL TABLE docs USING fts4(title, body);`,
			instruction.String())
	})

	t.Run("SQLiteTableInstructions", func(t *testing.T) {
		table := &SQLiteTable{
			Name: "users",
			Columns: []*SQLiteColumn{
				{
					Name:       "id",
					Type:       "INTEGER",
					PrimaryKey: true,
				},
			},
			Indexes: []*SQLiteIndex{
				{
					Table: "users",
					Name:  "idx_users_id",
					Keys:  []string{`"id"`},
				},
			},
			Triggers: []*SQLiteTrigger{
				{
					Name: "t",
					SQL:  "CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END",
				},
			},
		}

		instructions := table.Instructions()

		require.Len(t, instructions, 3)
		require.Equal(t, `CREATE TABLE "users" (
	"id" INTEGER PRIMARY KEY
);`, instructions[0].String())
		require.Equal(t, `CREATE INDEX "idx_users_id" ON "users" ("id");`, instructions[1].String())
		require.Equal(t,
			"CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END;",
			instructions[2].String())
	})

	t.Run("SQLiteIndexCreateInstructionCarriesThePredicate", func(t *testing.T) {
		index := &SQLiteIndex{
			Table:  "users",
			Name:   "idx_users_active",
			Keys:   []string{`"name"`},
			Where:  "active = 1",
			Unique: true,
		}

		require.Equal(t,
			`CREATE UNIQUE INDEX "idx_users_active" ON "users" ("name") WHERE active = 1;`,
			index.CreateInstruction().String())
	})

	t.Run("ColumnDefinitionIsNotAnInstruction", func(t *testing.T) {
		// A fragment must never satisfy Instruction. Go cannot express that negatively at
		// compile time, so this check runs at run time.
		fragments := map[string]any{
			"SQLiteColumn":     &SQLiteColumn{},
			"SQLiteForeignKey": &SQLiteForeignKey{},
		}

		for name, fragment := range fragments {
			_, isInstruction := fragment.(driversshared.Instruction)
			require.False(t, isInstruction, name)
		}

		column := &SQLiteColumn{
			Name:    "id",
			Type:    "INTEGER",
			NotNull: true,
		}
		require.Equal(t, `"id" INTEGER NOT NULL`, column.Definition())

		foreignKey := &SQLiteForeignKey{
			Table: "teams",
			From:  []string{"team"},
			To:    []string{"name"},
		}
		require.Equal(t, `FOREIGN KEY ("team") REFERENCES "teams" ("name")`, foreignKey.Clause())
	})
}

func TestSQLiteInstructionComments(t *testing.T) {
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

func TestSQLiteAnnotateInstructions(t *testing.T) {
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
				FromTableName:     "users",
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
