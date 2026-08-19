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
	Name               string
	Columns            []*SQLiteColumn
	PrimaryKey         []string
	PrimaryKeyConflict string
	UniqueConstraints  []*SQLiteUniqueConstraint
	ForeignKeys        []*SQLiteForeignKey
	CheckConstraints   []*SQLiteCheckConstraint

	// SQLite accepts a table option after the closing parenthesis. WITHOUT ROWID comes
	// first, because SQLite refuses the reverse order.
	WithoutRowID bool
	Strict       bool
}

func (i *SQLiteCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	if len(i.PrimaryKey) > 0 {
		lines = append(lines, fmt.Sprintf("\tPRIMARY KEY (%s)%s",
			strings.Join(quoteIdentifiers(i.PrimaryKey), ", "),
			conflictClause(i.PrimaryKeyConflict)))
	}

	for _, constraint := range i.UniqueConstraints {
		lines = append(lines, "\t"+constraint.Clause())
	}

	for _, check := range i.CheckConstraints {
		lines = append(lines, "\t"+check.Clause())
	}

	for _, foreignKey := range i.ForeignKeys {
		lines = append(lines, "\t"+foreignKey.Clause())
	}

	var options []string

	if i.WithoutRowID {
		options = append(options, "WITHOUT ROWID")
	}

	if i.Strict {
		options = append(options, "STRICT")
	}

	statement := fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		quoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(options) > 0 {
		statement += " " + strings.Join(options, ", ")
	}

	return statement + ";"
}

// The definition comes from sqlite_master, so dbdiff replays the text of the source.
type SQLiteCreateVirtualTableInstruction struct {
	Definition string
}

func (i *SQLiteCreateVirtualTableInstruction) String() string {
	return i.Definition + ";"
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
