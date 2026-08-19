package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// Every type of the catalogue uses a pointer receiver, so only a pointer satisfies this
// interface.
type Instruction interface {
	String() string
}

type AlterTableAction interface {
	TableActionClause() string
}

type AlterDomainAction interface {
	DomainActionClause() string
}

type Condition interface {
	ConditionClause() string
}

func RenderInstructions(instructions []Instruction) string {
	statements := lo.Map(instructions, func(instruction Instruction, _ int) string {
		return instruction.String()
	})

	return strings.Join(statements, "\n")
}

const sqlNullLiteral = "NULL"

type SQLEqualityCondition struct {
	ColumnName string
	Expression string
}

func (c *SQLEqualityCondition) ConditionClause() string {
	return fmt.Sprintf("%s = %s", quoteIdentifier(c.ColumnName), c.Expression)
}

type SQLIsNullCondition struct {
	ColumnName string
}

func (c *SQLIsNullCondition) ConditionClause() string {
	return fmt.Sprintf("%s IS NULL", quoteIdentifier(c.ColumnName))
}

type SQLConjunctionCondition struct {
	Conditions []Condition
}

func (c *SQLConjunctionCondition) ConditionClause() string {
	clauses := lo.Map(c.Conditions, func(condition Condition, _ int) string {
		return condition.ConditionClause()
	})

	return strings.Join(clauses, " AND ")
}

type SQLiteIndexPredicateCondition struct {
	Expression string
}

func (c *SQLiteIndexPredicateCondition) ConditionClause() string {
	return c.Expression
}

type SQLSetClause struct {
	ColumnName string
	Expression string
}

func (c *SQLSetClause) Clause() string {
	return fmt.Sprintf("%s = %s", quoteIdentifier(c.ColumnName), c.Expression)
}

type SQLInsertInstruction struct {
	TableName   string
	ColumnNames []string
	Expressions []string
}

func (i *SQLInsertInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		quoteIdentifier(i.TableName),
		strings.Join(quoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.Expressions, ", "))
}

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

type SQLDropTableInstruction struct {
	Name string
}

func (i *SQLDropTableInstruction) String() string {
	return "DROP TABLE " + quoteIdentifier(i.Name) + ";"
}

type SQLDropViewInstruction struct {
	Name string
}

func (i *SQLDropViewInstruction) String() string {
	return "DROP VIEW " + quoteIdentifier(i.Name) + ";"
}

type SQLDropIndexInstruction struct {
	Name string
}

func (i *SQLDropIndexInstruction) String() string {
	return "DROP INDEX " + quoteIdentifier(i.Name) + ";"
}

type SQLCommentInstruction struct {
	Text string
}

func (i *SQLCommentInstruction) String() string {
	// If the text holds a newline, the comment ends there and the rest of the line runs as
	// SQL. One space replaces every newline.
	return "-- " + strings.ReplaceAll(i.Text, "\n", " ")
}

type SQLDropColumnAction struct {
	ColumnName string
}

func (a *SQLDropColumnAction) TableActionClause() string {
	return "DROP COLUMN " + quoteIdentifier(a.ColumnName)
}

type SQLRenameColumnAction struct {
	ColumnName    string
	NewColumnName string
}

func (a *SQLRenameColumnAction) TableActionClause() string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s",
		quoteIdentifier(a.ColumnName), quoteIdentifier(a.NewColumnName))
}

type SQLRenameTableAction struct {
	NewName string
}

func (a *SQLRenameTableAction) TableActionClause() string {
	return "RENAME TO " + quoteIdentifier(a.NewName)
}

// A NULL value needs IS NULL, because a comparison with NULL matches no row.
func rowKeyCondition(primaryKeyColumnNames []string, row map[string]string) Condition {
	conditions := lo.Map(primaryKeyColumnNames, func(name string, _ int) Condition {
		if row[name] == sqlNullLiteral {
			return &SQLIsNullCondition{ColumnName: name}
		}

		return &SQLEqualityCondition{
			ColumnName: name,
			Expression: row[name],
		}
	})

	return &SQLConjunctionCondition{Conditions: conditions}
}
