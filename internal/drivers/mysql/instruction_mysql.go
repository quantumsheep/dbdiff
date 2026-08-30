package driversmysql

import (
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func QuoteIdentifiers(names []string) []string {
	quoted := make([]string, len(names))

	for i, name := range names {
		quoted[i] = QuoteIdentifier(name)
	}

	return quoted
}

// MySQL reads a backslash of a string as an escape character, so the shared QuoteLiteral
// does not fit.
func QuoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)

	return "'" + strings.ReplaceAll(escaped, "'", "''") + "'"
}

type MySQLCreateTableInstruction struct {
	Name             string
	Columns          []*MySQLColumn
	PrimaryKey       []string
	CheckConstraints []*MySQLCheckConstraint
	ForeignKeys      []*MySQLForeignKey

	// An empty value keeps the default of the database out of the statement.
	Engine    string
	Collation string

	// Partition holds the whole clause, from the words PARTITION BY.
	Partition string
}

func (i *MySQLCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	if len(i.PrimaryKey) > 0 {
		lines = append(lines, "\tPRIMARY KEY ("+strings.Join(QuoteIdentifiers(i.PrimaryKey), ", ")+")")
	}

	for _, check := range i.CheckConstraints {
		lines = append(lines, "\t"+check.Clause())
	}

	for _, foreignKey := range i.ForeignKeys {
		lines = append(lines, "\t"+foreignKey.Clause())
	}

	statement := fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if i.Engine != "" {
		statement += " ENGINE = " + i.Engine
	}

	if i.Collation != "" {
		statement += " DEFAULT COLLATE = " + i.Collation
	}

	if i.Partition != "" {
		statement += "\n" + i.Partition
	}

	return statement + ";"
}

func (i *MySQLCreateTableInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "table", i.Name)
}

type MySQLDropTableInstruction struct {
	Name string
}

func (i *MySQLDropTableInstruction) String() string {
	return "DROP TABLE " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropTableInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "table", i.Name)
}

type MySQLAlterTableInstruction struct {
	Name   string
	Action driversshared.AlterTableAction
}

func (i *MySQLAlterTableInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s %s;",
		QuoteIdentifier(i.Name), i.Action.TableActionClause())
}

func (i *MySQLAlterTableInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "table", i.Name)
}

type MySQLAddColumnAction struct {
	Column *MySQLColumn
}

func (a *MySQLAddColumnAction) TableActionClause() string {
	return "ADD COLUMN " + a.Column.Definition()
}

type MySQLModifyColumnAction struct {
	Column *MySQLColumn
}

func (a *MySQLModifyColumnAction) TableActionClause() string {
	return "MODIFY COLUMN " + a.Column.Definition()
}

type MySQLDropColumnAction struct {
	ColumnName string
}

func (a *MySQLDropColumnAction) TableActionClause() string {
	return "DROP COLUMN " + QuoteIdentifier(a.ColumnName)
}

type MySQLRenameColumnAction struct {
	ColumnName    string
	NewColumnName string
}

func (a *MySQLRenameColumnAction) TableActionClause() string {
	return fmt.Sprintf("RENAME COLUMN %s TO %s",
		QuoteIdentifier(a.ColumnName), QuoteIdentifier(a.NewColumnName))
}

type MySQLAddPrimaryKeyAction struct {
	Columns []string
}

func (a *MySQLAddPrimaryKeyAction) TableActionClause() string {
	return "ADD PRIMARY KEY (" + strings.Join(QuoteIdentifiers(a.Columns), ", ") + ")"
}

type MySQLDropPrimaryKeyAction struct{}

func (a *MySQLDropPrimaryKeyAction) TableActionClause() string {
	return "DROP PRIMARY KEY"
}

type MySQLAddForeignKeyAction struct {
	ForeignKey *MySQLForeignKey
}

func (a *MySQLAddForeignKeyAction) TableActionClause() string {
	return "ADD " + a.ForeignKey.Clause()
}

type MySQLDropForeignKeyAction struct {
	Name string
}

func (a *MySQLDropForeignKeyAction) TableActionClause() string {
	return "DROP FOREIGN KEY " + QuoteIdentifier(a.Name)
}

type MySQLAddCheckConstraintAction struct {
	CheckConstraint *MySQLCheckConstraint
}

func (a *MySQLAddCheckConstraintAction) TableActionClause() string {
	return "ADD " + a.CheckConstraint.Clause()
}

type MySQLDropCheckConstraintAction struct {
	Name string
}

// MySQL knows DROP CHECK and MariaDB knows DROP CONSTRAINT. Both engines know
// DROP CONSTRAINT.
func (a *MySQLDropCheckConstraintAction) TableActionClause() string {
	return "DROP CONSTRAINT " + QuoteIdentifier(a.Name)
}

type MySQLEngineAction struct {
	Engine string
}

func (a *MySQLEngineAction) TableActionClause() string {
	return "ENGINE = " + a.Engine
}

type MySQLConvertToCharacterSetAction struct {
	CharacterSet string
	Collation    string
}

// A DEFAULT COLLATE action changes the new columns only, and the conversion changes the
// stored columns too.
func (a *MySQLConvertToCharacterSetAction) TableActionClause() string {
	return "CONVERT TO CHARACTER SET " + a.CharacterSet + " COLLATE " + a.Collation
}

func characterSetOfCollation(collation string) string {
	characterSet, _, _ := strings.Cut(collation, "_")

	return characterSet
}

type MySQLPartitionAction struct {
	Clause string
}

func (a *MySQLPartitionAction) TableActionClause() string {
	return a.Clause
}

type MySQLRemovePartitioningAction struct{}

func (a *MySQLRemovePartitioningAction) TableActionClause() string {
	return "REMOVE PARTITIONING"
}

type MySQLCreateIndexInstruction struct {
	// Kind holds "UNIQUE", "FULLTEXT", "SPATIAL", or the empty value.
	Kind      string
	Name      string
	TableName string
	Keys      []string
}

func (i *MySQLCreateIndexInstruction) String() string {
	statement := "CREATE "

	if i.Kind != "" {
		statement += i.Kind + " "
	}

	return statement + fmt.Sprintf("INDEX %s ON %s (%s);",
		QuoteIdentifier(i.Name),
		QuoteIdentifier(i.TableName),
		strings.Join(i.Keys, ", "))
}

func (i *MySQLCreateIndexInstruction) Comment() string {
	return driversshared.TableObjectComment("Create", "index", i.Name, i.TableName)
}

type MySQLDropIndexInstruction struct {
	Name      string
	TableName string
}

func (i *MySQLDropIndexInstruction) String() string {
	return fmt.Sprintf("DROP INDEX %s ON %s;",
		QuoteIdentifier(i.Name), QuoteIdentifier(i.TableName))
}

func (i *MySQLDropIndexInstruction) Comment() string {
	return driversshared.TableObjectComment("Drop", "index", i.Name, i.TableName)
}

type MySQLCreateTriggerInstruction struct {
	Name      string
	Timing    string
	Event     string
	TableName string
	Statement string
}

func (i *MySQLCreateTriggerInstruction) String() string {
	return fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s;",
		QuoteIdentifier(i.Name), i.Timing, i.Event, QuoteIdentifier(i.TableName), i.Statement)
}

func (i *MySQLCreateTriggerInstruction) Comment() string {
	return driversshared.TableObjectComment("Create", "trigger", i.Name, i.TableName)
}

type MySQLDropTriggerInstruction struct {
	Name string
}

func (i *MySQLDropTriggerInstruction) String() string {
	return "DROP TRIGGER " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropTriggerInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "trigger", i.Name)
}

type MySQLCreateViewInstruction struct {
	Name       string
	Definition string
	OrReplace  bool
}

func (i *MySQLCreateViewInstruction) String() string {
	statement := "CREATE "

	if i.OrReplace {
		statement += "OR REPLACE "
	}

	return statement + fmt.Sprintf("VIEW %s AS %s;", QuoteIdentifier(i.Name), i.Definition)
}

func (i *MySQLCreateViewInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "view", i.Name)
}

type MySQLDropViewInstruction struct {
	Name string
}

func (i *MySQLDropViewInstruction) String() string {
	return "DROP VIEW " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropViewInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "view", i.Name)
}

type MySQLCreateSequenceInstruction struct {
	Name      string
	Start     int64
	Minimum   int64
	Maximum   int64
	Increment int64
	Cache     int64
	Cycle     bool
}

func (i *MySQLCreateSequenceInstruction) String() string {
	cycle := "NOCYCLE"
	if i.Cycle {
		cycle = "CYCLE"
	}

	return fmt.Sprintf("CREATE SEQUENCE %s START WITH %d MINVALUE %d MAXVALUE %d INCREMENT BY %d CACHE %d %s;",
		QuoteIdentifier(i.Name), i.Start, i.Minimum, i.Maximum, i.Increment, i.Cache, cycle)
}

func (i *MySQLCreateSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "sequence", i.Name)
}

type MySQLDropSequenceInstruction struct {
	Name string
}

func (i *MySQLDropSequenceInstruction) String() string {
	return "DROP SEQUENCE " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "sequence", i.Name)
}

type MySQLCreateRoutineInstruction struct {
	Type       string
	Name       string
	Definition string
}

func (i *MySQLCreateRoutineInstruction) String() string {
	return i.Definition + ";"
}

func (i *MySQLCreateRoutineInstruction) Comment() string {
	return driversshared.ObjectComment("Create", strings.ToLower(i.Type), i.Name)
}

type MySQLDropRoutineInstruction struct {
	Type string
	Name string
}

func (i *MySQLDropRoutineInstruction) String() string {
	return "DROP " + i.Type + " " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropRoutineInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", strings.ToLower(i.Type), i.Name)
}

type MySQLCreateEventInstruction struct {
	Name       string
	Definition string
}

func (i *MySQLCreateEventInstruction) String() string {
	return i.Definition + ";"
}

func (i *MySQLCreateEventInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "event", i.Name)
}

type MySQLDropEventInstruction struct {
	Name string
}

func (i *MySQLDropEventInstruction) String() string {
	return "DROP EVENT " + QuoteIdentifier(i.Name) + ";"
}

func (i *MySQLDropEventInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "event", i.Name)
}

type MySQLGrantInstruction struct {
	Privileges []string

	// A column list narrows the grant to those columns of the one privilege of Privileges.
	Columns []string

	// An empty table name gives the privileges of the whole database, with ON *.
	TableName string

	Grantee         string
	WithGrantOption bool
}

func (i *MySQLGrantInstruction) String() string {
	target := "*"
	if i.TableName != "" {
		target = QuoteIdentifier(i.TableName)
	}

	privilegeClause := strings.Join(i.Privileges, ", ")
	if len(i.Columns) > 0 {
		privilegeClause = i.Privileges[0] + " (" + strings.Join(QuoteIdentifiers(i.Columns), ", ") + ")"
	}

	statement := fmt.Sprintf("GRANT %s ON %s TO %s", privilegeClause, target, i.Grantee)

	if i.WithGrantOption {
		statement += " WITH GRANT OPTION"
	}

	return statement + ";"
}

func (i *MySQLGrantInstruction) Comment() string {
	if i.TableName == "" {
		return "Grant the privileges of the database"
	}

	return driversshared.OwnedObjectComment("Grant", "privileges", "table", i.TableName)
}

type MySQLRevokeInstruction struct {
	Privileges []string

	// A column list narrows the revoke to those columns of the one privilege of Privileges.
	Columns []string

	TableName string
	Grantee   string
}

func (i *MySQLRevokeInstruction) String() string {
	target := "*"
	if i.TableName != "" {
		target = QuoteIdentifier(i.TableName)
	}

	privilegeClause := strings.Join(i.Privileges, ", ")
	if len(i.Columns) > 0 {
		privilegeClause = i.Privileges[0] + " (" + strings.Join(QuoteIdentifiers(i.Columns), ", ") + ")"
	}

	return fmt.Sprintf("REVOKE %s ON %s FROM %s;", privilegeClause, target, i.Grantee)
}

func (i *MySQLRevokeInstruction) Comment() string {
	if i.TableName == "" {
		return "Revoke the privileges of the database"
	}

	return driversshared.OwnedObjectComment("Revoke", "privileges", "table", i.TableName)
}

type MySQLSetForeignKeyChecksInstruction struct {
	Enabled bool
}

func (i *MySQLSetForeignKeyChecksInstruction) String() string {
	if i.Enabled {
		return "SET FOREIGN_KEY_CHECKS = 1;"
	}

	return "SET FOREIGN_KEY_CHECKS = 0;"
}

func (i *MySQLSetForeignKeyChecksInstruction) Comment() string {
	if i.Enabled {
		return "Turn the enforcement of the foreign keys on again"
	}

	return "Turn the enforcement of the foreign keys off for the creation order of the tables"
}
