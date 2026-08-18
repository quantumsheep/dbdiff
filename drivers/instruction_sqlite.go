package drivers

import (
	"fmt"
	"strings"
)

// ALTER TABLE name action
// SQLite accepts one action for each statement.
type SQLiteAlterTableInstruction struct {
	Name   string
	Action AlterTableAction
}

func (i *SQLiteAlterTableInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s %s;",
		quoteIdentifier(i.Name), i.Action.TableActionClause())
}

// ADD COLUMN column_definition
type SQLiteAddColumnAction struct {
	Column *SQLiteColumn
}

func (a *SQLiteAddColumnAction) TableActionClause() string {
	return "ADD COLUMN " + a.Column.Definition()
}

// CREATE TABLE name ( column_definition [, ...] [, table_constraint [, ...] ] )
// A key or a constraint of one column stays in the definition of that column. PrimaryKey
// and UniqueConstraints hold a group of two or more columns only.
type SQLiteCreateTableInstruction struct {
	Name              string
	Columns           []*SQLiteColumn
	PrimaryKey        []string
	UniqueConstraints [][]string
	ForeignKeys       []*SQLiteForeignKey
}

func (i *SQLiteCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	if len(i.PrimaryKey) > 0 {
		lines = append(lines, fmt.Sprintf("\tPRIMARY KEY (%s)",
			strings.Join(quoteIdentifiers(i.PrimaryKey), ", ")))
	}

	for _, constraint := range i.UniqueConstraints {
		lines = append(lines, fmt.Sprintf("\tUNIQUE (%s)",
			strings.Join(quoteIdentifiers(constraint), ", ")))
	}

	for _, foreignKey := range i.ForeignKeys {
		lines = append(lines, "\t"+foreignKey.Clause())
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);",
		quoteIdentifier(i.Name), strings.Join(lines, ",\n"))
}

// CREATE [ UNIQUE ] INDEX name ON table_name ( key [, ...] ) [ WHERE condition ]
// A key holds the quoted name of a column, or the text of an expression.
type SQLiteCreateIndexInstruction struct {
	Unique    bool
	Name      string
	TableName string
	Keys      []string
	Condition Condition
}

func (i *SQLiteCreateIndexInstruction) String() string {
	statement := "CREATE "

	if i.Unique {
		statement += "UNIQUE "
	}

	statement += fmt.Sprintf("INDEX %s ON %s (%s)",
		quoteIdentifier(i.Name),
		quoteIdentifier(i.TableName),
		strings.Join(i.Keys, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

// The definition comes from sqlite_master, so dbdiff replays the text of the source.
type SQLiteCreateTriggerInstruction struct {
	Definition string
}

func (i *SQLiteCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

// The definition comes from sqlite_master, so dbdiff replays the text of the source.
type SQLiteCreateViewInstruction struct {
	Definition string
}

func (i *SQLiteCreateViewInstruction) String() string {
	return i.Definition + ";"
}

// DROP TRIGGER name
// SQLite accepts no ON clause.
type SQLiteDropTriggerInstruction struct {
	Name string
}

func (i *SQLiteDropTriggerInstruction) String() string {
	return "DROP TRIGGER " + quoteIdentifier(i.Name) + ";"
}
