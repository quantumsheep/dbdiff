package drivers

import (
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
}

// testInstruction covers RenderInstructions without a statement type of the catalogue.
type testInstruction struct {
	statement string
}

func (i *testInstruction) String() string {
	return i.statement
}
