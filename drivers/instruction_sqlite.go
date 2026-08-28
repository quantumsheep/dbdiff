package drivers

import (
	"fmt"
	"strings"
)

type SQLiteAlterTableInstruction struct {
	Name   string
	Action AlterTableAction
}

func (i *SQLiteAlterTableInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s %s;",
		QuoteIdentifier(i.Name), i.Action.TableActionClause())
}

func (i *SQLiteAlterTableInstruction) Comment() string {
	return objectComment("Modify", "table", i.Name)
}

type SQLiteAddColumnAction struct {
	Column *SQLiteColumn
}

func (a *SQLiteAddColumnAction) TableActionClause() string {
	return "ADD COLUMN " + a.Column.Definition()
}

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
			strings.Join(QuoteIdentifiers(i.PrimaryKey), ", "),
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
		QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(options) > 0 {
		statement += " " + strings.Join(options, ", ")
	}

	return statement + ";"
}

func (i *SQLiteCreateTableInstruction) Comment() string {
	return objectComment("Create", "table", i.Name)
}

type SQLiteCreateVirtualTableInstruction struct {
	Definition string
}

func (i *SQLiteCreateVirtualTableInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateVirtualTableInstruction) Comment() string {
	return definitionComment("Create", "virtual table", i.Definition, "TABLE")
}

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
		QuoteIdentifier(i.Name),
		QuoteIdentifier(i.TableName),
		strings.Join(i.Keys, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *SQLiteCreateIndexInstruction) Comment() string {
	return tableObjectComment("Create", "index", i.Name, i.TableName)
}

type SQLiteCreateTriggerInstruction struct {
	Definition string
}

func (i *SQLiteCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateTriggerInstruction) Comment() string {
	return tableDefinitionComment("Create", "trigger", i.Definition, "TRIGGER", "ON")
}

type SQLiteCreateViewInstruction struct {
	Definition string
}

func (i *SQLiteCreateViewInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateViewInstruction) Comment() string {
	return definitionComment("Create", "view", i.Definition, "VIEW")
}

type SQLiteDropTriggerInstruction struct {
	Name string
}

func (i *SQLiteDropTriggerInstruction) String() string {
	return "DROP TRIGGER " + QuoteIdentifier(i.Name) + ";"
}

func (i *SQLiteDropTriggerInstruction) Comment() string {
	return objectComment("Drop", "trigger", i.Name)
}

type SQLitePragmaForeignKeysInstruction struct {
	Enabled bool
}

func (i *SQLitePragmaForeignKeysInstruction) String() string {
	if i.Enabled {
		return "PRAGMA foreign_keys = ON;"
	}

	return "PRAGMA foreign_keys = OFF;"
}

func (i *SQLitePragmaForeignKeysInstruction) Comment() string {
	if i.Enabled {
		return "Turn the enforcement of the foreign keys on again"
	}

	return "Turn the enforcement of the foreign keys off for the recreation of a table"
}
