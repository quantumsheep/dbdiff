package drivers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstructions(t *testing.T) {
	t.Run("EqualityCondition", func(t *testing.T) {
		condition := &SQLEqualityCondition{ColumnName: "id", Expression: "1"}

		require.Equal(t, `"id" = 1`, condition.ConditionClause())
	})

	t.Run("EqualityConditionQuotesTheName", func(t *testing.T) {
		condition := &SQLEqualityCondition{ColumnName: `we"ird`, Expression: "'a'"}

		require.Equal(t, `"we""ird" = 'a'`, condition.ConditionClause())
	})

	t.Run("IsNullCondition", func(t *testing.T) {
		condition := &SQLIsNullCondition{ColumnName: "name"}

		require.Equal(t, `"name" IS NULL`, condition.ConditionClause())
	})

	t.Run("ConjunctionCondition", func(t *testing.T) {
		condition := &SQLConjunctionCondition{
			Conditions: []Condition{
				&SQLEqualityCondition{ColumnName: "team", Expression: "'red'"},
				&SQLEqualityCondition{ColumnName: "member", Expression: "'Alice'"},
			},
		}

		require.Equal(t, `"team" = 'red' AND "member" = 'Alice'`, condition.ConditionClause())
	})

	t.Run("IndexPredicateCondition", func(t *testing.T) {
		condition := &SQLiteIndexPredicateCondition{Expression: "active = 1"}

		require.Equal(t, "active = 1", condition.ConditionClause())
	})

	t.Run("SetClause", func(t *testing.T) {
		clause := &SQLSetClause{ColumnName: "name", Expression: "'Alice'"}

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
			Values:      []string{"1", "'Alice'"},
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
				{ColumnName: "name", Expression: "'Alice'"},
			},
			Condition: &SQLEqualityCondition{ColumnName: "id", Expression: "1"},
		}

		require.Equal(t,
			`UPDATE "users" SET "name" = 'Alice' WHERE "id" = 1;`,
			instruction.String())
	})

	t.Run("UpdateTwoColumns", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName: "users",
			SetClauses: []*SQLSetClause{
				{ColumnName: "name", Expression: "'Alice'"},
				{ColumnName: "age", Expression: "30"},
			},
			Condition: &SQLEqualityCondition{ColumnName: "id", Expression: "1"},
		}

		require.Equal(t,
			`UPDATE "users" SET "name" = 'Alice', "age" = 30 WHERE "id" = 1;`,
			instruction.String())
	})

	t.Run("UpdateWithoutACondition", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName:  "users",
			SetClauses: []*SQLSetClause{{ColumnName: "name", Expression: "'Alice'"}},
		}

		require.Equal(t, `UPDATE "users" SET "name" = 'Alice';`, instruction.String())
	})

	t.Run("Delete", func(t *testing.T) {
		instruction := &SQLDeleteInstruction{
			TableName: "users",
			Condition: &SQLEqualityCondition{ColumnName: "id", Expression: "2"},
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

	t.Run("DropColumnAction", func(t *testing.T) {
		action := &SQLDropColumnAction{ColumnName: "email"}

		require.Equal(t, `DROP COLUMN "email"`, action.TableActionClause())
	})

	t.Run("RenameColumnAction", func(t *testing.T) {
		action := &SQLRenameColumnAction{ColumnName: "name", NewColumnName: "full_name"}

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
				Column: &SQLiteColumn{Name: "email", Type: "TEXT"},
			},
		}

		require.Equal(t, `ALTER TABLE "users" ADD COLUMN "email" TEXT;`, instruction.String())
	})

	t.Run("SQLiteAlterTableRenameColumn", func(t *testing.T) {
		instruction := &SQLiteAlterTableInstruction{
			Name:   "users",
			Action: &SQLRenameColumnAction{ColumnName: "name", NewColumnName: "full_name"},
		}

		require.Equal(t,
			`ALTER TABLE "users" RENAME COLUMN "name" TO "full_name";`,
			instruction.String())
	})

	t.Run("SQLiteCreateTable", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "users",
			Columns: []*SQLiteColumn{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "name", Type: "TEXT", NotNull: true},
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
				{Name: "team", Type: "TEXT", NotNull: true},
				{Name: "member", Type: "TEXT", NotNull: true},
			},
			PrimaryKey:        []string{"team", "member"},
			UniqueConstraints: [][]string{{"member"}},
			ForeignKeys: []*SQLiteForeignKey{
				{Table: "teams", From: []string{"team"}, To: []string{"name"}},
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
					Column: &PostgresColumn{Name: "email", Type: "text"},
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
		action := &PostgresAlterColumnTypeAction{ColumnName: "age", DataType: "integer"}

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
		action := &PostgresSetDefaultAction{ColumnName: "age", Expression: "0"}

		require.Equal(t, `ALTER COLUMN "age" SET DEFAULT 0`, action.TableActionClause())
	})

	t.Run("PostgresDropDefaultAction", func(t *testing.T) {
		action := &PostgresDropDefaultAction{ColumnName: "age"}

		require.Equal(t, `ALTER COLUMN "age" DROP DEFAULT`, action.TableActionClause())
	})

	t.Run("PostgresAddConstraintAction", func(t *testing.T) {
		action := &PostgresAddConstraintAction{
			Constraint: &PostgresConstraint{Name: "users_pkey", Def: "PRIMARY KEY (id)"},
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
				{Name: "id", Type: "integer", NotNull: true},
			},
			Constraints: []*PostgresConstraint{
				{Name: "users_pkey", Def: "PRIMARY KEY (id)"},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL,
	CONSTRAINT "users_pkey" PRIMARY KEY (id)
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
			Values: []string{"sad", "happy"},
		}

		require.Equal(t, `CREATE TYPE "mood" AS ENUM ('sad', 'happy');`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValue", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{Name: "mood", Value: "calm"}

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
				{Name: "street", Type: "text"},
				{Name: "number", Type: "integer"},
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
			Default:  sql.NullString{String: "1", Valid: true},
			NotNull:  true,
			Constraints: []*PostgresDomainConstraint{
				{Name: "positive_check", Def: "CHECK ((VALUE > 0))"},
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
}

// testInstruction covers RenderInstructions without a statement type of the catalogue.
type testInstruction struct {
	statement string
}

func (i *testInstruction) String() string {
	return i.statement
}
