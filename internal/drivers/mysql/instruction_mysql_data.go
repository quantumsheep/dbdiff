package driversmysql

import (
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type MySQLInsertInstruction struct {
	TableName   string
	ColumnNames []string
	Expressions []string
}

func (i *MySQLInsertInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		QuoteIdentifier(i.TableName),
		strings.Join(QuoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.Expressions, ", "))
}

func (i *MySQLInsertInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type MySQLSetClause struct {
	ColumnName string
	Expression string
}

func (c *MySQLSetClause) Clause() string {
	return QuoteIdentifier(c.ColumnName) + " = " + c.Expression
}

type MySQLUpdateInstruction struct {
	TableName  string
	SetClauses []*MySQLSetClause
	Condition  driversshared.Condition
}

func (i *MySQLUpdateInstruction) String() string {
	clauses := lo.Map(i.SetClauses, func(clause *MySQLSetClause, _ int) string {
		return clause.Clause()
	})

	statement := fmt.Sprintf("UPDATE %s SET %s",
		QuoteIdentifier(i.TableName), strings.Join(clauses, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *MySQLUpdateInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type MySQLDeleteInstruction struct {
	TableName string
	Condition driversshared.Condition
}

func (i *MySQLDeleteInstruction) String() string {
	statement := "DELETE FROM " + QuoteIdentifier(i.TableName)

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *MySQLDeleteInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "rows", "table", i.TableName)
}

type MySQLEqualityCondition struct {
	ColumnName string
	Expression string
}

func (c *MySQLEqualityCondition) ConditionClause() string {
	return QuoteIdentifier(c.ColumnName) + " = " + c.Expression
}

type MySQLIsNullCondition struct {
	ColumnName string
}

func (c *MySQLIsNullCondition) ConditionClause() string {
	return QuoteIdentifier(c.ColumnName) + " IS NULL"
}

// A NULL value needs IS NULL, because a comparison with NULL matches no row.
func MySQLRowKeyCondition(primaryKeyColumnNames []string, row map[string]string) driversshared.Condition {
	conditions := lo.Map(primaryKeyColumnNames, func(name string, _ int) driversshared.Condition {
		if row[name] == driversshared.SQLNullLiteral {
			return &MySQLIsNullCondition{
				ColumnName: name,
			}
		}

		return &MySQLEqualityCondition{
			ColumnName: name,
			Expression: row[name],
		}
	})

	return &driversshared.SQLConjunctionCondition{
		Conditions: conditions,
	}
}
