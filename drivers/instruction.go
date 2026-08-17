package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// An Instruction is one complete SQL statement, or one comment line. A pointer type
// implements this interface. A value type never does.
type Instruction interface {
	String() string
}

// An AlterTableAction renders one action of an ALTER TABLE statement.
type AlterTableAction interface {
	TableActionClause() string
}

// An AlterDomainAction renders the action of an ALTER DOMAIN statement.
type AlterDomainAction interface {
	DomainActionClause() string
}

// A Condition renders the body of a WHERE clause.
type Condition interface {
	ConditionClause() string
}

// RenderInstructions joins the statements with one newline.
func RenderInstructions(instructions []Instruction) string {
	statements := lo.Map(instructions, func(instruction Instruction, _ int) string {
		return instruction.String()
	})

	return strings.Join(statements, "\n")
}

// The literal of a NULL value. formatSQLiteValue and formatPostgresValue return it.
const sqlNullLiteral = "NULL"

// column_name = expression
type SQLEqualityCondition struct {
	ColumnName string
	Expression string
}

func (c *SQLEqualityCondition) ConditionClause() string {
	return fmt.Sprintf("%s = %s", quoteIdentifier(c.ColumnName), c.Expression)
}

// column_name IS NULL
type SQLIsNullCondition struct {
	ColumnName string
}

func (c *SQLIsNullCondition) ConditionClause() string {
	return fmt.Sprintf("%s IS NULL", quoteIdentifier(c.ColumnName))
}

// condition AND condition [ AND ... ]
type SQLConjunctionCondition struct {
	Conditions []Condition
}

func (c *SQLConjunctionCondition) ConditionClause() string {
	clauses := lo.Map(c.Conditions, func(condition Condition, _ int) string {
		return condition.ConditionClause()
	})

	return strings.Join(clauses, " AND ")
}

// The expression comes from sqlite_master, so dbdiff replays the text of the source.
type SQLiteIndexPredicateCondition struct {
	Expression string
}

func (c *SQLiteIndexPredicateCondition) ConditionClause() string {
	return c.Expression
}

// column_name = expression
type SQLSetClause struct {
	ColumnName string
	Expression string
}

func (c *SQLSetClause) Clause() string {
	return fmt.Sprintf("%s = %s", quoteIdentifier(c.ColumnName), c.Expression)
}
