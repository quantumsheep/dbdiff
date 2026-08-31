package driversshared

import (
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

	t.Run("SetClause", func(t *testing.T) {
		clause := &SQLSetClause{
			ColumnName: "name",
			Expression: "'Alice'",
		}

		require.Equal(t, `"name" = 'Alice'`, clause.Clause())
	})

	t.Run("SetClauseIsNotAnInstruction", func(t *testing.T) {
		// A fragment must never satisfy Instruction. Go cannot express that negatively at
		// compile time, so this check runs at run time.
		var fragment any = &SQLSetClause{}

		_, isInstruction := fragment.(Instruction)
		require.False(t, isInstruction)
	})

	t.Run("InstructionsJoinWithOneNewline", func(t *testing.T) {
		instructions := Instructions{
			&testInstruction{statement: "FIRST;"},
			&testInstruction{statement: "SECOND;"},
		}

		require.Equal(t, "FIRST;\nSECOND;", instructions.String())
	})

	t.Run("EmptyInstructionsGiveAnEmptyText", func(t *testing.T) {
		require.Equal(t, "", Instructions(nil).String())
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

	t.Run("RowKeyConditionOfOneColumn", func(t *testing.T) {
		row := map[string]string{"id": "1"}

		condition := RowKeyCondition([]string{"id"}, row)

		require.Equal(t, `"id" = 1`, condition.ConditionClause())
	})

	t.Run("RowKeyConditionOfTwoColumns", func(t *testing.T) {
		row := map[string]string{"team": "'red'", "member": "'Alice'"}

		condition := RowKeyCondition([]string{"team", "member"}, row)

		require.Equal(t, `"team" = 'red' AND "member" = 'Alice'`, condition.ConditionClause())
	})

	t.Run("RowKeyConditionOfANullValue", func(t *testing.T) {
		row := map[string]string{"id": "1", "email": SQLNullLiteral}

		condition := RowKeyCondition([]string{"id", "email"}, row)

		require.Equal(t, `"id" = 1 AND "email" IS NULL`, condition.ConditionClause())
	})
}

type testInstruction struct {
	statement string
}

func (i *testInstruction) String() string {
	return i.statement
}

func (i *testInstruction) Comment() string {
	return ""
}
