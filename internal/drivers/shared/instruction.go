package driversshared

import (
	"fmt"
	"strings"
	"unicode"

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

// Instructions holds the whole output of a diff.
type Instructions []Instruction

// String gives the instructions as one SQL text, one statement per line.
func (instructions Instructions) String() string {
	statements := lo.Map(instructions, func(instruction Instruction, _ int) string {
		return instruction.String()
	})

	return strings.Join(statements, "\n")
}

const SQLNullLiteral = "NULL"

// A quoter is a zero-size type, so an instruction value stays comparable in a test.
type IdentifierQuoter interface {
	QuoteIdentifier(name string) string
}

type DoubleQuote struct{}

func (DoubleQuote) QuoteIdentifier(name string) string {
	return QuoteIdentifier(name)
}

func quoteWith[Quoter IdentifierQuoter](name string) string {
	var quoter Quoter

	return quoter.QuoteIdentifier(name)
}

type EqualityCondition[Quoter IdentifierQuoter] struct {
	ColumnName string
	Expression string
}

func (c *EqualityCondition[Quoter]) ConditionClause() string {
	return fmt.Sprintf("%s = %s", quoteWith[Quoter](c.ColumnName), c.Expression)
}

type IsNullCondition[Quoter IdentifierQuoter] struct {
	ColumnName string
}

func (c *IsNullCondition[Quoter]) ConditionClause() string {
	return fmt.Sprintf("%s IS NULL", quoteWith[Quoter](c.ColumnName))
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

type SetClause[Quoter IdentifierQuoter] struct {
	ColumnName string
	Expression string
}

func (c *SetClause[Quoter]) Clause() string {
	return fmt.Sprintf("%s = %s", quoteWith[Quoter](c.ColumnName), c.Expression)
}

type InsertInstruction[Quoter IdentifierQuoter] struct {
	TableName   string
	ColumnNames []string
	Expressions []string
}

func (i *InsertInstruction[Quoter]) String() string {
	quotedColumnNames := lo.Map(i.ColumnNames, func(name string, _ int) string {
		return quoteWith[Quoter](name)
	})

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		quoteWith[Quoter](i.TableName),
		strings.Join(quotedColumnNames, ", "),
		strings.Join(i.Expressions, ", "))
}

func (i *InsertInstruction[Quoter]) Comment() string {
	return OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type SQLInsertSelectInstruction struct {
	TableName         string
	ColumnNames       []string
	SelectExpressions []string
	FromTableName     string
}

func (i *SQLInsertSelectInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;",
		QuoteIdentifier(i.TableName),
		strings.Join(QuoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.SelectExpressions, ", "),
		QuoteIdentifier(i.FromTableName))
}

func (i *SQLInsertSelectInstruction) Comment() string {
	return OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type UpdateInstruction[Quoter IdentifierQuoter] struct {
	TableName  string
	SetClauses []*SetClause[Quoter]
	Condition  Condition
}

func (i *UpdateInstruction[Quoter]) String() string {
	clauses := lo.Map(i.SetClauses, func(clause *SetClause[Quoter], _ int) string {
		return clause.Clause()
	})

	statement := fmt.Sprintf("UPDATE %s SET %s",
		quoteWith[Quoter](i.TableName), strings.Join(clauses, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *UpdateInstruction[Quoter]) Comment() string {
	return OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type DeleteInstruction[Quoter IdentifierQuoter] struct {
	TableName string
	Condition Condition
}

func (i *DeleteInstruction[Quoter]) String() string {
	statement := "DELETE FROM " + quoteWith[Quoter](i.TableName)

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *DeleteInstruction[Quoter]) Comment() string {
	return OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type (
	SQLEqualityCondition = EqualityCondition[DoubleQuote]
	SQLIsNullCondition   = IsNullCondition[DoubleQuote]
	SQLSetClause         = SetClause[DoubleQuote]
	SQLInsertInstruction = InsertInstruction[DoubleQuote]
	SQLUpdateInstruction = UpdateInstruction[DoubleQuote]
	SQLDeleteInstruction = DeleteInstruction[DoubleQuote]
)

type SQLDropTableInstruction struct {
	Name string
}

func (i *SQLDropTableInstruction) String() string {
	return "DROP TABLE " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropTableInstruction) Comment() string {
	return ObjectComment("Drop", "table", i.Name)
}

type SQLDropViewInstruction struct {
	Name string
}

func (i *SQLDropViewInstruction) String() string {
	return "DROP VIEW " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropViewInstruction) Comment() string {
	return ObjectComment("Drop", "view", i.Name)
}

type SQLDropIndexInstruction struct {
	Name string
}

func (i *SQLDropIndexInstruction) String() string {
	return "DROP INDEX " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLDropIndexInstruction) Comment() string {
	return ObjectComment("Drop", "index", i.Name)
}

type SQLCommentInstruction struct {
	Text string
}

func (i *SQLCommentInstruction) String() string {
	// If the text holds a line break, the comment ends there and the rest of the line runs as
	// SQL. PostgreSQL also ends a comment at a carriage return. One space replaces every
	// control character.
	return "-- " + strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}

		return character
	}, i.Text)
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
func RowKeyCondition[Quoter IdentifierQuoter](primaryKeyColumnNames []string, row map[string]string) Condition {
	conditions := lo.Map(primaryKeyColumnNames, func(name string, _ int) Condition {
		if row[name] == SQLNullLiteral {
			return &IsNullCondition[Quoter]{
				ColumnName: name,
			}
		}

		return &EqualityCondition[Quoter]{
			ColumnName: name,
			Expression: row[name],
		}
	})

	return &SQLConjunctionCondition{
		Conditions: conditions,
	}
}
