package driverssqlite

import (
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
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
	// keyword. An empty list keeps the plain names of PrimaryKey. A list that is not empty
	// must hold one clause for each column of PrimaryKey.
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
		if len(i.PrimaryKeyKeys) > 0 {
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

type SQLiteCreateIndexInstruction struct {
	Unique    bool
	Name      string
	TableName string
	Keys      []string
	Where     string
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

	if i.Where != "" {
		statement += " WHERE " + i.Where
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

type SQLiteDeleteSequenceInstruction struct {
	TableName string
}

func (i *SQLiteDeleteSequenceInstruction) String() string {
	return "DELETE FROM sqlite_sequence WHERE name = " + driversshared.QuoteLiteral(i.TableName) + ";"
}

func (i *SQLiteDeleteSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "AUTOINCREMENT counter of the table", i.TableName)
}

// SQLite compares the table names without case, and sqlite_sequence holds the name of the
// table as the CREATE TABLE statement wrote it, so the condition uses COLLATE NOCASE.
type SQLiteCopySequenceInstruction struct {
	FromTableName string
	ToTableName   string
}

func (i *SQLiteCopySequenceInstruction) String() string {
	return fmt.Sprintf("INSERT INTO sqlite_sequence (name, seq) SELECT %s, seq FROM sqlite_sequence WHERE name = %s COLLATE NOCASE;",
		driversshared.QuoteLiteral(i.ToTableName), driversshared.QuoteLiteral(i.FromTableName))
}

func (i *SQLiteCopySequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "AUTOINCREMENT counter of the table", i.ToTableName)
}

const recreationSavepointName = "dbdiff_recreation"

// A SAVEPOINT statement outside a transaction starts a transaction, and inside a transaction
// it nests. The statement thus works in the sqlite3 shell and in the transaction of the
// migrate command, where a BEGIN statement fails.
type SQLiteSavepointInstruction struct {
	Release bool
}

func (i *SQLiteSavepointInstruction) String() string {
	if i.Release {
		return "RELEASE " + driversshared.QuoteIdentifier(recreationSavepointName) + ";"
	}

	return "SAVEPOINT " + driversshared.QuoteIdentifier(recreationSavepointName) + ";"
}

func (i *SQLiteSavepointInstruction) Comment() string {
	if i.Release {
		return "Commit the recreation of the tables"
	}

	return "Start the transaction of the recreation of the tables"
}

type foreignKeyCheckStep int

const (
	createForeignKeyCheckTable foreignKeyCheckStep = iota
	countForeignKeyViolations
	dropForeignKeyCheckTable
)

const (
	foreignKeyCheckTableName      = "dbdiff_foreign_key_check"
	foreignKeyCheckConstraintName = "a_foreign_key_is_broken"
)

// PRAGMA foreign_key_check gives the broken foreign keys as rows, and it causes no error.
// The CHECK constraint of the temporary table turns a count that is not zero into an error,
// which stops the script before the release of the savepoint. The count reads only the
// foreign keys of the recreated tables, as child or as parent, so an old broken foreign key
// of another table does not stop the script. SQLite compares table names without case.
type SQLiteForeignKeyCheckInstruction struct {
	Step foreignKeyCheckStep

	RecreatedTableNames []string
}

func (i *SQLiteForeignKeyCheckInstruction) String() string {
	tableName := driversshared.QuoteIdentifier(foreignKeyCheckTableName)

	switch i.Step {
	case countForeignKeyViolations:
		names := strings.Join(lo.Map(i.RecreatedTableNames, func(name string, _ int) string {
			return driversshared.QuoteLiteral(name)
		}), ", ")

		return fmt.Sprintf(`INSERT INTO %s ("violations") SELECT count(*) FROM pragma_foreign_key_check `+
			`WHERE "table" COLLATE NOCASE IN (%s) OR "parent" COLLATE NOCASE IN (%s);`, tableName, names, names)
	case dropForeignKeyCheckTable:
		return "DROP TABLE " + tableName + ";"
	}

	return fmt.Sprintf(`CREATE TEMP TABLE %s ("violations" INTEGER CONSTRAINT %s CHECK ("violations" = 0));`,
		tableName, driversshared.QuoteIdentifier(foreignKeyCheckConstraintName))
}

func (i *SQLiteForeignKeyCheckInstruction) Comment() string {
	return "Stop if a foreign key is broken"
}
