package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// An Instruction is one complete SQL statement, or one comment line. Every type of the
// catalogue uses a pointer receiver, so only a pointer implements this interface.
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

// sqlNullLiteral holds the literal of a NULL value. formatSQLiteValue and
// formatPostgresValue return it.
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

// INSERT INTO table_name ( column_name [, ...] ) VALUES ( expression [, ...] )
type SQLInsertInstruction struct {
	TableName   string
	ColumnNames []string
	Values      []string
}

func (i *SQLInsertInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		quoteIdentifier(i.TableName),
		strings.Join(quoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.Values, ", "))
}

// INSERT INTO table_name ( column_name [, ...] ) SELECT expression [, ...] FROM table_name
// An expression holds the text of a quoted column, of a default value, or of NULL.
type SQLInsertSelectInstruction struct {
	TableName         string
	ColumnNames       []string
	SelectExpressions []string
	SourceTableName   string
}

func (i *SQLInsertSelectInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;",
		quoteIdentifier(i.TableName),
		strings.Join(quoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.SelectExpressions, ", "),
		quoteIdentifier(i.SourceTableName))
}

// UPDATE table_name SET { column_name = expression } [, ...] [ WHERE condition ]
type SQLUpdateInstruction struct {
	TableName  string
	SetClauses []*SQLSetClause
	Condition  Condition
}

func (i *SQLUpdateInstruction) String() string {
	clauses := lo.Map(i.SetClauses, func(clause *SQLSetClause, _ int) string {
		return clause.Clause()
	})

	statement := fmt.Sprintf("UPDATE %s SET %s",
		quoteIdentifier(i.TableName), strings.Join(clauses, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

// DELETE FROM table_name [ WHERE condition ]
type SQLDeleteInstruction struct {
	TableName string
	Condition Condition
}

func (i *SQLDeleteInstruction) String() string {
	statement := "DELETE FROM " + quoteIdentifier(i.TableName)

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

// DROP TABLE name
type SQLDropTableInstruction struct {
	Name string
}

func (i *SQLDropTableInstruction) String() string {
	return "DROP TABLE " + quoteIdentifier(i.Name) + ";"
}

// DROP VIEW name
type SQLDropViewInstruction struct {
	Name string
}

func (i *SQLDropViewInstruction) String() string {
	return "DROP VIEW " + quoteIdentifier(i.Name) + ";"
}

// DROP INDEX name
type SQLDropIndexInstruction struct {
	Name string
}

func (i *SQLDropIndexInstruction) String() string {
	return "DROP INDEX " + quoteIdentifier(i.Name) + ";"
}

// A comment line of the output. The text carries no leading dashes.
type SQLCommentInstruction struct {
	Text string
}

func (i *SQLCommentInstruction) String() string {
	// If the text holds a newline, the comment ends there and the rest of the line runs as
	// SQL. One space replaces every newline.
	return "-- " + strings.ReplaceAll(i.Text, "\n", " ")
}

// DROP COLUMN column_name
type SQLDropColumnAction struct {
	ColumnName string
}

func (a *SQLDropColumnAction) TableActionClause() string {
	return "DROP COLUMN " + quoteIdentifier(a.ColumnName)
}

// RENAME COLUMN column_name TO new_column_name
type SQLRenameColumnAction struct {
	ColumnName    string
	NewColumnName string
}

func (a *SQLRenameColumnAction) TableActionClause() string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s",
		quoteIdentifier(a.ColumnName), quoteIdentifier(a.NewColumnName))
}

// RENAME TO new_name
type SQLRenameTableAction struct {
	NewName string
}

func (a *SQLRenameTableAction) TableActionClause() string {
	return "RENAME TO " + quoteIdentifier(a.NewName)
}

// rowKeyCondition builds the WHERE body that selects one row by its primary key. A NULL
// value needs IS NULL, because a comparison with NULL matches no row.
func rowKeyCondition(primaryKeyColumnNames []string, row map[string]string) Condition {
	conditions := lo.Map(primaryKeyColumnNames, func(name string, _ int) Condition {
		if row[name] == sqlNullLiteral {
			return &SQLIsNullCondition{ColumnName: name}
		}

		return &SQLEqualityCondition{ColumnName: name, Expression: row[name]}
	})

	return &SQLConjunctionCondition{Conditions: conditions}
}
