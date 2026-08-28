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
	// The name of the object that the statement changes, and not a comment of the schema.
	Comment() string
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
	return fmt.Sprintf("%s = %s", QuoteIdentifier(c.ColumnName), c.Expression)
}

type SQLIsNullCondition struct {
	ColumnName string
}

func (c *SQLIsNullCondition) ConditionClause() string {
	return fmt.Sprintf("%s IS NULL", QuoteIdentifier(c.ColumnName))
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
	return fmt.Sprintf("%s = %s", QuoteIdentifier(c.ColumnName), c.Expression)
}

type SQLInsertInstruction struct {
	TableName   string
	ColumnNames []string
	Expressions []string
}

func (i *SQLInsertInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		QuoteIdentifier(i.TableName),
		strings.Join(QuoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.Expressions, ", "))
}

func (i *SQLInsertInstruction) Comment() string {
	return ownedObjectComment("Change", "rows", "table", i.TableName)
}

type SQLInsertSelectInstruction struct {
	TableName         string
	ColumnNames       []string
	SelectExpressions []string
	SourceTableName   string
}

func (i *SQLInsertSelectInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;",
		QuoteIdentifier(i.TableName),
		strings.Join(QuoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.SelectExpressions, ", "),
		QuoteIdentifier(i.SourceTableName))
}

func (i *SQLInsertSelectInstruction) Comment() string {
	return ownedObjectComment("Change", "rows", "table", i.TableName)
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
		QuoteIdentifier(i.TableName), strings.Join(clauses, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *SQLUpdateInstruction) Comment() string {
	return ownedObjectComment("Change", "rows", "table", i.TableName)
}

type SQLDeleteInstruction struct {
	TableName string
	Condition Condition
}

func (i *SQLDeleteInstruction) String() string {
	statement := "DELETE FROM " + QuoteIdentifier(i.TableName)

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *SQLDeleteInstruction) Comment() string {
	return ownedObjectComment("Change", "rows", "table", i.TableName)
}

type SQLDropTableInstruction struct {
	Name string
}

func (i *SQLDropTableInstruction) String() string {
	return "DROP TABLE " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropTableInstruction) Comment() string {
	return objectComment("Drop", "table", i.Name)
}

type SQLDropViewInstruction struct {
	Name string
}

func (i *SQLDropViewInstruction) String() string {
	return "DROP VIEW " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropViewInstruction) Comment() string {
	return objectComment("Drop", "view", i.Name)
}

type SQLDropIndexInstruction struct {
	Name string
}

func (i *SQLDropIndexInstruction) String() string {
	return "DROP INDEX " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropIndexInstruction) Comment() string {
	return objectComment("Drop", "index", i.Name)
}

type SQLCommentInstruction struct {
	Text string
}

func (i *SQLCommentInstruction) String() string {
	// If the text holds a newline, the comment ends there and the rest of the line runs as
	// SQL. One space replaces every newline.
	return "-- " + strings.ReplaceAll(i.Text, "\n", " ")
}

// A comment of the output takes no comment of its own.
func (i *SQLCommentInstruction) Comment() string {
	return ""
}

type SQLDropColumnAction struct {
	ColumnName string
}

func (a *SQLDropColumnAction) TableActionClause() string {
	return "DROP COLUMN " + QuoteIdentifier(a.ColumnName)
}

type SQLRenameColumnAction struct {
	ColumnName    string
	NewColumnName string
}

func (a *SQLRenameColumnAction) TableActionClause() string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s",
		QuoteIdentifier(a.ColumnName), QuoteIdentifier(a.NewColumnName))
}

type SQLRenameTableAction struct {
	NewName string
}

func (a *SQLRenameTableAction) TableActionClause() string {
	return "RENAME TO " + QuoteIdentifier(a.NewName)
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
