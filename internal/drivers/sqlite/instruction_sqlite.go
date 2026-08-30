package driverssqlite

import (
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteTableRecreationInstruction struct {
	driversshared.Instruction
	TableName string
}

func (i *SQLiteTableRecreationInstruction) Comment() string {
	return driversshared.ObjectComment("Recreate", "table", i.TableName)
}

type SQLiteAlterTableInstruction struct {
	Name   string
	Action driversshared.AlterTableAction
}

func (i *SQLiteAlterTableInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s %s;",
		driversshared.QuoteIdentifier(i.Name), i.Action.TableActionClause())
}

func (i *SQLiteAlterTableInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "table", i.Name)
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
	PrimaryKeyName     string
	PrimaryKeyConflict string

	// A key clause holds the quoted column name with its COLLATE clause and its DESC
	// keyword. An empty list keeps the plain names of PrimaryKey.
	PrimaryKeyKeys []string

	UniqueConstraints []*SQLiteUniqueConstraint
	ForeignKeys       []*SQLiteForeignKey
	CheckConstraints  []*SQLiteCheckConstraint
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
		keys := driversshared.QuoteIdentifiers(i.PrimaryKey)
		if len(i.PrimaryKeyKeys) == len(i.PrimaryKey) {
			keys = i.PrimaryKeyKeys
		}

		lines = append(lines, fmt.Sprintf("\t%sPRIMARY KEY (%s)%s",
			constraintNameClause(i.PrimaryKeyName),
			strings.Join(keys, ", "),
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
		driversshared.QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(options) > 0 {
		statement += " " + strings.Join(options, ", ")
	}

	return statement + ";"
}

func (i *SQLiteCreateTableInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "table", i.Name)
}

type SQLiteCreateVirtualTableInstruction struct {
	Definition string
}

func (i *SQLiteCreateVirtualTableInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateVirtualTableInstruction) Comment() string {
	return driversshared.DefinitionComment("Create", "virtual table", i.Definition, "TABLE")
}

type SQLiteIndexPredicateCondition struct {
	Expression string
}

func (c *SQLiteIndexPredicateCondition) ConditionClause() string {
	return c.Expression
}

type SQLiteCreateIndexInstruction struct {
	Unique    bool
	Name      string
	TableName string
	Keys      []string
	Condition driversshared.Condition
}

func (i *SQLiteCreateIndexInstruction) String() string {
	statement := "CREATE "

	if i.Unique {
		statement += "UNIQUE "
	}

	statement += fmt.Sprintf("INDEX %s ON %s (%s)",
		driversshared.QuoteIdentifier(i.Name),
		driversshared.QuoteIdentifier(i.TableName),
		strings.Join(i.Keys, ", "))

	if i.Condition != nil {
		statement += " WHERE " + i.Condition.ConditionClause()
	}

	return statement + ";"
}

func (i *SQLiteCreateIndexInstruction) Comment() string {
	return driversshared.TableObjectComment("Create", "index", i.Name, i.TableName)
}

type SQLiteCreateTriggerInstruction struct {
	Definition string
}

func (i *SQLiteCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateTriggerInstruction) Comment() string {
	return driversshared.TableDefinitionComment("Create", "trigger", i.Definition, "TRIGGER", "ON")
}

type SQLiteCreateViewInstruction struct {
	Definition string
}

func (i *SQLiteCreateViewInstruction) String() string {
	return i.Definition + ";"
}

func (i *SQLiteCreateViewInstruction) Comment() string {
	return driversshared.DefinitionComment("Create", "view", i.Definition, "VIEW")
}

type SQLiteDropTriggerInstruction struct {
	Name string
}

func (i *SQLiteDropTriggerInstruction) String() string {
	return "DROP TRIGGER " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *SQLiteDropTriggerInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "trigger", i.Name)
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
