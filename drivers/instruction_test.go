package drivers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstructions(t *testing.T) {
	t.Run("EqualityCondition", func(t *testing.T) {
		condition := &SQLEqualityCondition{
			ColumnName: "id",
			Expression: "1",
		}

		require.Equal(t, `"id" = 1`, condition.ConditionClause())
	})

	t.Run("EqualityConditionQuotesTheName", func(t *testing.T) {
		condition := &SQLEqualityCondition{
			ColumnName: `we"ird`,
			Expression: "'a'",
		}

		require.Equal(t, `"we""ird" = 'a'`, condition.ConditionClause())
	})

	t.Run("IsNullCondition", func(t *testing.T) {
		condition := &SQLIsNullCondition{ColumnName: "name"}

		require.Equal(t, `"name" IS NULL`, condition.ConditionClause())
	})

	t.Run("ConjunctionCondition", func(t *testing.T) {
		condition := &SQLConjunctionCondition{
			Conditions: []Condition{
				&SQLEqualityCondition{
					ColumnName: "team",
					Expression: "'red'",
				},
				&SQLEqualityCondition{
					ColumnName: "member",
					Expression: "'Alice'",
				},
			},
		}

		require.Equal(t, `"team" = 'red' AND "member" = 'Alice'`, condition.ConditionClause())
	})

	t.Run("IndexPredicateCondition", func(t *testing.T) {
		condition := &SQLiteIndexPredicateCondition{Expression: "active = 1"}

		require.Equal(t, "active = 1", condition.ConditionClause())
	})

	t.Run("SetClause", func(t *testing.T) {
		clause := &SQLSetClause{
			ColumnName: "name",
			Expression: "'Alice'",
		}

		require.Equal(t, `"name" = 'Alice'`, clause.Clause())
	})

	t.Run("RenderInstructionsJoinsWithOneNewline", func(t *testing.T) {
		instructions := []Instruction{
			&testInstruction{statement: "FIRST;"},
			&testInstruction{statement: "SECOND;"},
		}

		require.Equal(t, "FIRST;\nSECOND;", RenderInstructions(instructions))
	})

	t.Run("RenderInstructionsOfAnEmptyList", func(t *testing.T) {
		require.Equal(t, "", RenderInstructions(nil))
	})

	t.Run("Insert", func(t *testing.T) {
		instruction := &SQLInsertInstruction{
			TableName:   "users",
			ColumnNames: []string{"id", "name"},
			Expressions: []string{"1", "'Alice'"},
		}

		require.Equal(t,
			`INSERT INTO "users" ("id", "name") VALUES (1, 'Alice');`,
			instruction.String())
	})

	t.Run("InsertSelect", func(t *testing.T) {
		instruction := &SQLInsertSelectInstruction{
			TableName:         "_users_temp",
			ColumnNames:       []string{"id", "email"},
			SelectExpressions: []string{`"id"`, "NULL"},
			SourceTableName:   "users",
		}

		require.Equal(t,
			`INSERT INTO "_users_temp" ("id", "email") SELECT "id", NULL FROM "users";`,
			instruction.String())
	})

	t.Run("Update", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName: "users",
			SetClauses: []*SQLSetClause{
				{
					ColumnName: "name",
					Expression: "'Alice'",
				},
			},
			Condition: &SQLEqualityCondition{
				ColumnName: "id",
				Expression: "1",
			},
		}

		require.Equal(t,
			`UPDATE "users" SET "name" = 'Alice' WHERE "id" = 1;`,
			instruction.String())
	})

	t.Run("UpdateTwoColumns", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName: "users",
			SetClauses: []*SQLSetClause{
				{
					ColumnName: "name",
					Expression: "'Alice'",
				},
				{
					ColumnName: "age",
					Expression: "30",
				},
			},
			Condition: &SQLEqualityCondition{
				ColumnName: "id",
				Expression: "1",
			},
		}

		require.Equal(t,
			`UPDATE "users" SET "name" = 'Alice', "age" = 30 WHERE "id" = 1;`,
			instruction.String())
	})

	t.Run("UpdateWithoutACondition", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName: "users",
			SetClauses: []*SQLSetClause{
				{
					ColumnName: "name",
					Expression: "'Alice'",
				},
			},
		}

		require.Equal(t, `UPDATE "users" SET "name" = 'Alice';`, instruction.String())
	})

	t.Run("Delete", func(t *testing.T) {
		instruction := &SQLDeleteInstruction{
			TableName: "users",
			Condition: &SQLEqualityCondition{
				ColumnName: "id",
				Expression: "2",
			},
		}

		require.Equal(t, `DELETE FROM "users" WHERE "id" = 2;`, instruction.String())
	})

	t.Run("DeleteWithoutACondition", func(t *testing.T) {
		instruction := &SQLDeleteInstruction{TableName: "users"}

		require.Equal(t, `DELETE FROM "users";`, instruction.String())
	})

	t.Run("DropTable", func(t *testing.T) {
		instruction := &SQLDropTableInstruction{Name: "users"}

		require.Equal(t, `DROP TABLE "users";`, instruction.String())
	})

	t.Run("DropView", func(t *testing.T) {
		instruction := &SQLDropViewInstruction{Name: "user_ids"}

		require.Equal(t, `DROP VIEW "user_ids";`, instruction.String())
	})

	t.Run("DropIndex", func(t *testing.T) {
		instruction := &SQLDropIndexInstruction{Name: "idx_users_name"}

		require.Equal(t, `DROP INDEX "idx_users_name";`, instruction.String())
	})

	t.Run("Comment", func(t *testing.T) {
		instruction := &SQLCommentInstruction{
			Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
		}

		require.Equal(t,
			`-- The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			instruction.String())
	})

	t.Run("CommentWithANewline", func(t *testing.T) {
		instruction := &SQLCommentInstruction{Text: "first\nDROP TABLE users;"}

		require.Equal(t, "-- first DROP TABLE users;", instruction.String())
	})

	t.Run("DropColumnAction", func(t *testing.T) {
		action := &SQLDropColumnAction{ColumnName: "email"}

		require.Equal(t, `DROP COLUMN "email"`, action.TableActionClause())
	})

	t.Run("RenameColumnAction", func(t *testing.T) {
		action := &SQLRenameColumnAction{
			ColumnName:    "name",
			NewColumnName: "full_name",
		}

		require.Equal(t, `RENAME COLUMN "name" TO "full_name"`, action.TableActionClause())
	})

	t.Run("RenameTableAction", func(t *testing.T) {
		action := &SQLRenameTableAction{NewName: "users"}

		require.Equal(t, `RENAME TO "users"`, action.TableActionClause())
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
			Action: &SQLRenameColumnAction{
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
			Condition: &SQLiteIndexPredicateCondition{Expression: "active = 1"},
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

	t.Run("PostgresAlterTableAddColumn", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []AlterTableAction{
				&PostgresAddColumnAction{
					Column: &PostgresColumn{
						Name: "email",
						Type: "text",
					},
				},
			},
		}

		require.Equal(t, `ALTER TABLE "users" ADD COLUMN "email" text;`, instruction.String())
	})

	t.Run("PostgresAlterTableTwoActions", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []AlterTableAction{
				&PostgresDropConstraintAction{ConstraintName: "users_pkey"},
				&PostgresSetNotNullAction{ColumnName: "id"},
			},
		}

		require.Equal(t,
			`ALTER TABLE "users" DROP CONSTRAINT "users_pkey", ALTER COLUMN "id" SET NOT NULL;`,
			instruction.String())
	})

	t.Run("PostgresAlterColumnTypeAction", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "age",
			DataType:   "integer",
		}

		require.Equal(t, `ALTER COLUMN "age" TYPE integer`, action.TableActionClause())
	})

	t.Run("PostgresAlterColumnTypeActionWithACast", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "age",
			DataType:   "integer",
			UsingCast:  true,
		}

		require.Equal(t,
			`ALTER COLUMN "age" TYPE integer USING "age"::integer`,
			action.TableActionClause())
	})

	t.Run("PostgresSetNotNullAction", func(t *testing.T) {
		action := &PostgresSetNotNullAction{ColumnName: "name"}

		require.Equal(t, `ALTER COLUMN "name" SET NOT NULL`, action.TableActionClause())
	})

	t.Run("PostgresDropNotNullAction", func(t *testing.T) {
		action := &PostgresDropNotNullAction{ColumnName: "name"}

		require.Equal(t, `ALTER COLUMN "name" DROP NOT NULL`, action.TableActionClause())
	})

	t.Run("PostgresSetDefaultAction", func(t *testing.T) {
		action := &PostgresSetDefaultAction{
			ColumnName: "age",
			Expression: "0",
		}

		require.Equal(t, `ALTER COLUMN "age" SET DEFAULT 0`, action.TableActionClause())
	})

	t.Run("PostgresDropDefaultAction", func(t *testing.T) {
		action := &PostgresDropDefaultAction{ColumnName: "age"}

		require.Equal(t, `ALTER COLUMN "age" DROP DEFAULT`, action.TableActionClause())
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

	// SQLite accepts a column with no type. The short form of a generated column, for
	// example "total AS (price * quantity)", holds no type in most schemas.
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
					Name:  "age",
					Type:  "INTEGER",
					Check: "(age > 0)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "people" (
	"name" TEXT COLLATE NOCASE,
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

	t.Run("PostgresCreateTablePartitionedByRange", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "events",
			Columns: []*PostgresColumn{
				{
					Name: "created",
					Type: "date",
				},
			},
			PartitionKey: "RANGE (created)",
		}

		require.Equal(t, `CREATE TABLE "events" (
	"created" date
) PARTITION BY RANGE (created);`, instruction.String())
	})

	t.Run("PostgresCreateTablePartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "events_2024",
			ParentName: "events",
			Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
		}

		require.Equal(t,
			`CREATE TABLE "events_2024" PARTITION OF "events" FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');`,
			instruction.String())
	})

	t.Run("PostgresCreateTableDefaultPartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "events_default",
			ParentName: "events",
			Bound:      "DEFAULT",
		}

		require.Equal(t,
			`CREATE TABLE "events_default" PARTITION OF "events" DEFAULT;`,
			instruction.String())
	})

	t.Run("PostgresCreateMaterializedView", func(t *testing.T) {
		instruction := &PostgresCreateMaterializedViewInstruction{
			Name:  "active_users",
			Query: " SELECT id\n   FROM users;",
		}

		require.Equal(t, `CREATE MATERIALIZED VIEW "active_users" AS  SELECT id
   FROM users;`, instruction.String())
	})

	t.Run("PostgresDropMaterializedView", func(t *testing.T) {
		instruction := &PostgresDropMaterializedViewInstruction{Name: "active_users"}

		require.Equal(t, `DROP MATERIALIZED VIEW "active_users";`, instruction.String())
	})

	t.Run("PostgresCreateTableWithACollation", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "people",
			Columns: []*PostgresColumn{
				{
					Name:      "name",
					Type:      "text",
					Collation: "C",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "people" (
	"name" text COLLATE "C"
);`, instruction.String())
	})

	t.Run("PostgresAlterColumnTypeActionWithACollation", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "name",
			DataType:   "text",
			Collation:  "C",
		}

		require.Equal(t,
			`ALTER COLUMN "name" TYPE text COLLATE "C"`,
			action.TableActionClause())
	})

	t.Run("PostgresCommentOnTable", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name:    "users",
			Comment: "the people of the site",
		}

		require.Equal(t,
			`COMMENT ON TABLE "users" IS 'the people of the site';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnTableThatRemovesTheComment", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{Name: "users"}

		require.Equal(t, `COMMENT ON TABLE "users" IS NULL;`, instruction.String())
	})

	t.Run("PostgresCommentOnColumn", func(t *testing.T) {
		instruction := &PostgresCommentOnColumnInstruction{
			TableName:  "users",
			ColumnName: "id",
			Comment:    "the key",
		}

		require.Equal(t,
			`COMMENT ON COLUMN "users"."id" IS 'the key';`,
			instruction.String())
	})

	t.Run("PostgresCommentWithAQuote", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name:    "users",
			Comment: "the site's people",
		}

		require.Equal(t,
			`COMMENT ON TABLE "users" IS 'the site''s people';`,
			instruction.String())
	})

	t.Run("PostgresEnableRowLevelSecurityAction", func(t *testing.T) {
		action := &PostgresRowLevelSecurityAction{Mode: "ENABLE"}

		require.Equal(t, `ENABLE ROW LEVEL SECURITY`, action.TableActionClause())
	})

	t.Run("PostgresNoForceRowLevelSecurityAction", func(t *testing.T) {
		action := &PostgresRowLevelSecurityAction{Mode: "NO FORCE"}

		require.Equal(t, `NO FORCE ROW LEVEL SECURITY`, action.TableActionClause())
	})

	t.Run("PostgresCreatePolicy", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:       "docs_read",
			TableName:  "docs",
			Permissive: "PERMISSIVE",
			Command:    "SELECT",
			Roles:      []string{"public"},
			Using:      "(NOT secret)",
		}

		require.Equal(t,
			`CREATE POLICY "docs_read" ON "docs" AS PERMISSIVE FOR SELECT TO public USING (NOT secret);`,
			instruction.String())
	})

	t.Run("PostgresCreatePolicyWithACheck", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:       "docs_write",
			TableName:  "docs",
			Permissive: "PERMISSIVE",
			Command:    "INSERT",
			Roles:      []string{"public"},
			WithCheck:  "(owner = CURRENT_USER)",
		}

		require.Equal(t,
			`CREATE POLICY "docs_write" ON "docs" AS PERMISSIVE FOR INSERT TO public WITH CHECK (owner = CURRENT_USER);`,
			instruction.String())
	})

	t.Run("PostgresDropPolicy", func(t *testing.T) {
		instruction := &PostgresDropPolicyInstruction{
			Name:      "docs_read",
			TableName: "docs",
		}

		require.Equal(t, `DROP POLICY "docs_read" ON "docs";`, instruction.String())
	})

	t.Run("PostgresCreateTableThatInherits", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "child",
			Columns: []*PostgresColumn{
				{
					Name: "a",
					Type: "integer",
				},
			},
			Inherits: []string{"parent"},
		}

		require.Equal(t, `CREATE TABLE "child" (
	"a" integer
) INHERITS ("parent");`, instruction.String())
	})

	t.Run("SQLiteCreateVirtualTable", func(t *testing.T) {
		instruction := &SQLiteCreateVirtualTableInstruction{
			Definition: "CREATE VIRTUAL TABLE docs USING fts4(title, body)",
		}

		require.Equal(t,
			`CREATE VIRTUAL TABLE docs USING fts4(title, body);`,
			instruction.String())
	})

	t.Run("PostgresAddIdentityAction", func(t *testing.T) {
		action := &PostgresAddIdentityAction{
			ColumnName: "id",
			Identity:   "ALWAYS",
		}

		require.Equal(t,
			`ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY`,
			action.TableActionClause())
	})

	t.Run("PostgresSetIdentityAction", func(t *testing.T) {
		action := &PostgresSetIdentityAction{
			ColumnName: "id",
			Identity:   "BY DEFAULT",
		}

		require.Equal(t,
			`ALTER COLUMN "id" SET GENERATED BY DEFAULT`,
			action.TableActionClause())
	})

	t.Run("PostgresDropIdentityAction", func(t *testing.T) {
		action := &PostgresDropIdentityAction{ColumnName: "id"}

		require.Equal(t, `ALTER COLUMN "id" DROP IDENTITY`, action.TableActionClause())
	})

	t.Run("PostgresAddConstraintAction", func(t *testing.T) {
		action := &PostgresAddConstraintAction{
			Constraint: &PostgresConstraint{
				Name: "users_pkey",
				Def:  "PRIMARY KEY (id)",
			},
		}

		require.Equal(t,
			`ADD CONSTRAINT "users_pkey" PRIMARY KEY (id)`,
			action.TableActionClause())
	})

	t.Run("PostgresDropConstraintAction", func(t *testing.T) {
		action := &PostgresDropConstraintAction{ConstraintName: "users_pkey"}

		require.Equal(t, `DROP CONSTRAINT "users_pkey"`, action.TableActionClause())
	})

	t.Run("PostgresCreateTable", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
			Columns: []*PostgresColumn{
				{
					Name:    "id",
					Type:    "integer",
					NotNull: true,
				},
			},
			Constraints: []*PostgresConstraint{
				{
					Name: "users_pkey",
					Def:  "PRIMARY KEY (id)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL,
	CONSTRAINT "users_pkey" PRIMARY KEY (id)
);`, instruction.String())
	})

	t.Run("PostgresCreateTableWithAnIdentityColumn", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
			Columns: []*PostgresColumn{
				{
					Name:     "id",
					Type:     "integer",
					NotNull:  true,
					Identity: "ALWAYS",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL GENERATED ALWAYS AS IDENTITY
);`, instruction.String())
	})

	t.Run("PostgresCreateTableWithAGeneratedColumn", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "measures",
			Columns: []*PostgresColumn{
				{
					Name: "value",
					Type: "integer",
				},
				{
					Name:                "doubled",
					Type:                "integer",
					GeneratedExpression: "(value * 2)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "measures" (
	"value" integer,
	"doubled" integer GENERATED ALWAYS AS (value * 2) STORED
);`, instruction.String())
	})

	t.Run("PostgresCreateIndex", func(t *testing.T) {
		instruction := &PostgresCreateIndexInstruction{
			Definition: `CREATE INDEX idx_users_name ON users USING btree (name)`,
		}

		require.Equal(t,
			`CREATE INDEX idx_users_name ON users USING btree (name);`,
			instruction.String())
	})

	t.Run("PostgresCreateTrigger", func(t *testing.T) {
		instruction := &PostgresCreateTriggerInstruction{
			Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
		}

		require.Equal(t,
			"CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();",
			instruction.String())
	})

	t.Run("PostgresDropTrigger", func(t *testing.T) {
		instruction := &PostgresDropTriggerInstruction{
			Name:      "set_timestamp",
			TableName: "users",
		}

		require.Equal(t,
			`DROP TRIGGER "set_timestamp" ON "users";`,
			instruction.String())
	})

	t.Run("PostgresCreateView", func(t *testing.T) {
		instruction := &PostgresCreateViewInstruction{
			Name:  "user_ids",
			Query: " SELECT id\n   FROM users;",
		}

		require.Equal(t, "CREATE VIEW \"user_ids\" AS  SELECT id\n   FROM users;",
			instruction.String())
	})

	t.Run("PostgresCreateEnumType", func(t *testing.T) {
		instruction := &PostgresCreateEnumTypeInstruction{
			Name:   "mood",
			Labels: []string{"sad", "happy"},
		}

		require.Equal(t, `CREATE TYPE "mood" AS ENUM ('sad', 'happy');`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValue", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "calm",
		}

		require.Equal(t, `ALTER TYPE "mood" ADD VALUE 'calm';`, instruction.String())
	})

	t.Run("PostgresDropType", func(t *testing.T) {
		instruction := &PostgresDropTypeInstruction{Name: "mood"}

		require.Equal(t, `DROP TYPE "mood";`, instruction.String())
	})

	t.Run("PostgresCreateCompositeType", func(t *testing.T) {
		instruction := &PostgresCreateCompositeTypeInstruction{
			Name: "address",
			Attributes: []*PostgresCompositeTypeAttribute{
				{
					Name: "street",
					Type: "text",
				},
				{
					Name: "number",
					Type: "integer",
				},
			},
		}

		require.Equal(t, `CREATE TYPE "address" AS (
	"street" text,
	"number" integer
);`, instruction.String())
	})

	t.Run("PostgresCreateDomain", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
		}

		require.Equal(t, `CREATE DOMAIN "positive" AS integer;`, instruction.String())
	})

	t.Run("PostgresCreateDomainWithEveryPart", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
			Default: sql.NullString{
				String: "1",
				Valid:  true,
			},
			NotNull: true,
			Constraints: []*PostgresDomainConstraint{
				{
					Name: "positive_check",
					Def:  "CHECK ((VALUE > 0))",
				},
			},
		}

		require.Equal(t,
			`CREATE DOMAIN "positive" AS integer DEFAULT 1 NOT NULL CONSTRAINT "positive_check" CHECK ((VALUE > 0));`,
			instruction.String())
	})

	t.Run("PostgresDropDomain", func(t *testing.T) {
		instruction := &PostgresDropDomainInstruction{Name: "positive"}

		require.Equal(t, `DROP DOMAIN "positive";`, instruction.String())
	})

	t.Run("PostgresAlterDomainSetDefault", func(t *testing.T) {
		instruction := &PostgresAlterDomainInstruction{
			Name:   "positive",
			Action: &PostgresSetDomainDefaultAction{Expression: "5"},
		}

		require.Equal(t, `ALTER DOMAIN "positive" SET DEFAULT 5;`, instruction.String())
	})

	t.Run("PostgresDropDomainDefaultAction", func(t *testing.T) {
		action := &PostgresDropDomainDefaultAction{}

		require.Equal(t, "DROP DEFAULT", action.DomainActionClause())
	})

	t.Run("PostgresSetDomainNotNullAction", func(t *testing.T) {
		action := &PostgresSetDomainNotNullAction{}

		require.Equal(t, "SET NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresDropDomainNotNullAction", func(t *testing.T) {
		action := &PostgresDropDomainNotNullAction{}

		require.Equal(t, "DROP NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresAddDomainConstraintAction", func(t *testing.T) {
		action := &PostgresAddDomainConstraintAction{
			ConstraintName: "positive_check",
			Definition:     "CHECK ((VALUE > 0))",
		}

		require.Equal(t,
			`ADD CONSTRAINT "positive_check" CHECK ((VALUE > 0))`,
			action.DomainActionClause())
	})

	t.Run("PostgresDropDomainConstraintAction", func(t *testing.T) {
		action := &PostgresDropDomainConstraintAction{ConstraintName: "positive_check"}

		require.Equal(t, `DROP CONSTRAINT "positive_check"`, action.DomainActionClause())
	})

	t.Run("PostgresCreateFunction", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql",
		}

		require.Equal(t,
			"CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;",
			instruction.String())
	})

	t.Run("PostgresDropFunction", func(t *testing.T) {
		instruction := &PostgresDropFunctionInstruction{
			Name:      "add",
			Arguments: "integer",
		}

		require.Equal(t, `DROP FUNCTION "add"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateAggregate", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:               "total",
			Arguments:          "integer",
			TransitionFunction: "int4pl",
			StateType:          "integer",
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = "int4pl", STYPE = integer);`,
			instruction.String())
	})

	t.Run("PostgresCreateAggregateWithEveryOption", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:               "total",
			Arguments:          "integer",
			TransitionFunction: "int4pl",
			StateType:          "integer",
			FinalFunction: sql.NullString{
				String: "int4out",
				Valid:  true,
			},
			InitialCondition: sql.NullString{
				String: "0",
				Valid:  true,
			},
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = "int4pl", STYPE = integer, FINALFUNC = "int4out", INITCOND = '0');`,
			instruction.String())
	})

	t.Run("PostgresDropAggregate", func(t *testing.T) {
		instruction := &PostgresDropAggregateInstruction{
			Name:      "total",
			Arguments: "integer",
		}

		require.Equal(t, `DROP AGGREGATE "total"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateOperator", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "int4eq",
			LeftArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t,
			`CREATE OPERATOR === (FUNCTION = "int4eq", LEFTARG = integer, RIGHTARG = integer);`,
			instruction.String())
	})

	t.Run("PostgresDropOperatorWithoutALeftArgument", func(t *testing.T) {
		instruction := &PostgresDropOperatorInstruction{
			Name: "===",
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t, `DROP OPERATOR === (NONE, integer);`, instruction.String())
	})

	t.Run("PostgresCreateExtension", func(t *testing.T) {
		instruction := &PostgresCreateExtensionInstruction{Name: "pgcrypto"}

		require.Equal(t, `CREATE EXTENSION "pgcrypto";`, instruction.String())
	})

	t.Run("PostgresAlterExtension", func(t *testing.T) {
		instruction := &PostgresAlterExtensionInstruction{
			Name:       "pgcrypto",
			NewVersion: "1.3",
		}

		require.Equal(t, `ALTER EXTENSION "pgcrypto" UPDATE TO '1.3';`, instruction.String())
	})

	t.Run("PostgresDropExtension", func(t *testing.T) {
		instruction := &PostgresDropExtensionInstruction{Name: "pgcrypto"}

		require.Equal(t, `DROP EXTENSION "pgcrypto";`, instruction.String())
	})

	t.Run("PostgresCreateSequence", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:      "counter",
			DataType:  "bigint",
			Increment: 1,
			Min:       1,
			Max:       9223372036854775807,
			Start:     1,
		}

		require.Equal(t,
			`CREATE SEQUENCE "counter" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 NO CYCLE;`,
			instruction.String())
	})

	t.Run("PostgresCreateSequenceWithCycle", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:      "counter",
			DataType:  "bigint",
			Increment: 1,
			Min:       1,
			Max:       10,
			Start:     1,
			Cycle:     true,
		}

		require.Equal(t,
			`CREATE SEQUENCE "counter" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 10 START WITH 1 CYCLE;`,
			instruction.String())
	})

	t.Run("PostgresAlterSequenceOneClause", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "counter",
			Increment: sql.NullInt64{
				Int64: 2,
				Valid: true,
			},
		}

		require.Equal(t, `ALTER SEQUENCE "counter" INCREMENT BY 2;`, instruction.String())
	})

	t.Run("PostgresAlterSequenceEveryClauseInOrder", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "counter",
			DataType: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			Increment: sql.NullInt64{
				Int64: 2,
				Valid: true,
			},
			Min: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
			Max: sql.NullInt64{
				Int64: 50,
				Valid: true,
			},
			Start: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
			Cycle: sql.NullBool{
				Bool:  true,
				Valid: true,
			},
			Restart: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
		}

		require.Equal(t,
			`ALTER SEQUENCE "counter" AS integer INCREMENT BY 2 MINVALUE 5 MAXVALUE 50 START WITH 5 CYCLE RESTART WITH 5;`,
			instruction.String())
	})

	t.Run("PostgresDropSequence", func(t *testing.T) {
		instruction := &PostgresDropSequenceInstruction{Name: "counter"}

		require.Equal(t, `DROP SEQUENCE "counter";`, instruction.String())
	})

	t.Run("ColumnDefinitionIsNotAnInstruction", func(t *testing.T) {
		// A fragment must never satisfy Instruction. Go cannot express that negatively at
		// compile time, so this check runs at run time.
		fragments := map[string]any{
			"SQLiteColumn":       &SQLiteColumn{},
			"PostgresColumn":     &PostgresColumn{},
			"PostgresConstraint": &PostgresConstraint{},
			"SQLiteForeignKey":   &SQLiteForeignKey{},
			"SQLSetClause":       &SQLSetClause{},
		}

		for name, fragment := range fragments {
			_, isInstruction := fragment.(Instruction)
			require.False(t, isInstruction, name)
		}

		column := &SQLiteColumn{
			Name:    "id",
			Type:    "INTEGER",
			NotNull: true,
		}
		require.Equal(t, `"id" INTEGER NOT NULL`, column.Definition())

		postgresColumn := &PostgresColumn{
			Name:    "id",
			Type:    "integer",
			NotNull: true,
		}
		require.Equal(t, `"id" integer NOT NULL`, postgresColumn.Definition())

		constraint := &PostgresConstraint{
			Name: "users_pkey",
			Def:  "PRIMARY KEY (id)",
		}
		require.Equal(t, `CONSTRAINT "users_pkey" PRIMARY KEY (id)`, constraint.Clause())

		foreignKey := &SQLiteForeignKey{
			Table: "teams",
			From:  []string{"team"},
			To:    []string{"name"},
		}
		require.Equal(t, `FOREIGN KEY ("team") REFERENCES "teams" ("name")`, foreignKey.Clause())
	})

	t.Run("RowKeyConditionOfOneColumn", func(t *testing.T) {
		row := map[string]string{"id": "1"}

		condition := rowKeyCondition([]string{"id"}, row)

		require.Equal(t, `"id" = 1`, condition.ConditionClause())
	})

	t.Run("RowKeyConditionOfTwoColumns", func(t *testing.T) {
		row := map[string]string{"team": "'red'", "member": "'Alice'"}

		condition := rowKeyCondition([]string{"team", "member"}, row)

		require.Equal(t, `"team" = 'red' AND "member" = 'Alice'`, condition.ConditionClause())
	})

	t.Run("RowKeyConditionOfANullValue", func(t *testing.T) {
		row := map[string]string{"id": "1", "email": sqlNullLiteral}

		condition := rowKeyCondition([]string{"id", "email"}, row)

		require.Equal(t, `"id" = 1 AND "email" IS NULL`, condition.ConditionClause())
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
}

// testInstruction covers RenderInstructions without a statement type of the catalogue.
type testInstruction struct {
	statement string
}

func (i *testInstruction) String() string {
	return i.statement
}
