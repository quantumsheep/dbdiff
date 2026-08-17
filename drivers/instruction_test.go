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
}

// testInstruction covers RenderInstructions without a statement type of the catalogue.
type testInstruction struct {
	statement string
}

func (i *testInstruction) String() string {
	return i.statement
}
