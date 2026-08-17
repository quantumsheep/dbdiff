package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// ALTER TABLE name action [, ...]
// PostgreSQL accepts a list of actions for one statement.
type PostgresAlterTableInstruction struct {
	Name    string
	Actions []AlterTableAction
}

func (i *PostgresAlterTableInstruction) String() string {
	clauses := lo.Map(i.Actions, func(action AlterTableAction, _ int) string {
		return action.TableActionClause()
	})

	return fmt.Sprintf("ALTER TABLE %s %s;",
		quoteIdentifier(i.Name), strings.Join(clauses, ", "))
}

// ADD COLUMN column_definition
type PostgresAddColumnAction struct {
	Column *PostgresColumn
}

func (a *PostgresAddColumnAction) TableActionClause() string {
	return "ADD COLUMN " + a.Column.Definition()
}

// ALTER COLUMN column_name TYPE data_type [ USING expression ]
// UsingCast adds the cast that PostgreSQL needs when no automatic cast exists.
type PostgresAlterColumnTypeAction struct {
	ColumnName string
	DataType   string
	UsingCast  bool
}

func (a *PostgresAlterColumnTypeAction) TableActionClause() string {
	clause := fmt.Sprintf("ALTER COLUMN %s TYPE %s",
		quoteIdentifier(a.ColumnName), a.DataType)

	if a.UsingCast {
		clause += fmt.Sprintf(" USING %s::%s", quoteIdentifier(a.ColumnName), a.DataType)
	}

	return clause
}

// ALTER COLUMN column_name SET NOT NULL
type PostgresSetNotNullAction struct {
	ColumnName string
}

func (a *PostgresSetNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " SET NOT NULL"
}

// ALTER COLUMN column_name DROP NOT NULL
type PostgresDropNotNullAction struct {
	ColumnName string
}

func (a *PostgresDropNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP NOT NULL"
}

// ALTER COLUMN column_name SET DEFAULT expression
type PostgresSetDefaultAction struct {
	ColumnName string
	Expression string
}

func (a *PostgresSetDefaultAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s",
		quoteIdentifier(a.ColumnName), a.Expression)
}

// ALTER COLUMN column_name DROP DEFAULT
type PostgresDropDefaultAction struct {
	ColumnName string
}

func (a *PostgresDropDefaultAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP DEFAULT"
}

// ADD table_constraint
type PostgresAddConstraintAction struct {
	Constraint *PostgresConstraint
}

func (a *PostgresAddConstraintAction) TableActionClause() string {
	return "ADD " + a.Constraint.Clause()
}

// DROP CONSTRAINT constraint_name
type PostgresDropConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropConstraintAction) TableActionClause() string {
	return "DROP CONSTRAINT " + quoteIdentifier(a.ConstraintName)
}

// CREATE TABLE name ( column_definition [, ...] [, table_constraint [, ...] ] )
type PostgresCreateTableInstruction struct {
	Name        string
	Columns     []*PostgresColumn
	Constraints []*PostgresConstraint
}

func (i *PostgresCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	for _, constraint := range i.Constraints {
		lines = append(lines, "\t"+constraint.Clause())
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);",
		quoteIdentifier(i.Name), strings.Join(lines, ",\n"))
}

// The definition comes from pg_indexes.indexdef, so dbdiff replays the text of the source.
type PostgresCreateIndexInstruction struct {
	Definition string
}

func (i *PostgresCreateIndexInstruction) String() string {
	return i.Definition + ";"
}

// The definition comes from pg_get_triggerdef, so dbdiff replays the text of the source.
type PostgresCreateTriggerInstruction struct {
	Definition string
}

func (i *PostgresCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

// DROP TRIGGER name ON table_name
type PostgresDropTriggerInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropTriggerInstruction) String() string {
	return fmt.Sprintf("DROP TRIGGER %s ON %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))
}

// CREATE VIEW name AS query
// The query comes from information_schema.views, and it ends with a semicolon. String
// removes that semicolon and adds one, so every instruction ends the same way. The query
// keeps its leading space, so the output text does not change.
type PostgresCreateViewInstruction struct {
	Name  string
	Query string
}

func (i *PostgresCreateViewInstruction) String() string {
	return "CREATE VIEW " + quoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";") + ";"
}
