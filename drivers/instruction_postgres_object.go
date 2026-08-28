package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresCreateExtensionInstruction struct {
	Name string
}

func (i *PostgresCreateExtensionInstruction) String() string {
	return "CREATE EXTENSION " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresCreateExtensionInstruction) Comment() string {
	return objectComment("Create", "extension", i.Name)
}

type PostgresAlterExtensionInstruction struct {
	Name       string
	NewVersion string
}

func (i *PostgresAlterExtensionInstruction) String() string {
	return fmt.Sprintf("ALTER EXTENSION %s UPDATE TO %s;",
		QuoteIdentifier(i.Name), quoteLiteral(i.NewVersion))
}

func (i *PostgresAlterExtensionInstruction) Comment() string {
	return objectComment("Modify", "extension", i.Name)
}

type PostgresDropExtensionInstruction struct {
	Name string
}

func (i *PostgresDropExtensionInstruction) String() string {
	return "DROP EXTENSION " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropExtensionInstruction) Comment() string {
	return objectComment("Drop", "extension", i.Name)
}

func sequenceCycleClause(cycle bool) string {
	if cycle {
		return "CYCLE"
	}

	return "NO CYCLE"
}

type PostgresCreateSequenceInstruction struct {
	Name      string
	DataType  string
	Increment int64
	Min       int64
	Max       int64
	Start     int64
	Cycle     bool
}

func (i *PostgresCreateSequenceInstruction) String() string {
	return fmt.Sprintf(
		"CREATE SEQUENCE %s AS %s INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d %s;",
		QuoteIdentifier(i.Name),
		i.DataType,
		i.Increment,
		i.Min,
		i.Max,
		i.Start,
		sequenceCycleClause(i.Cycle),
	)
}

func (i *PostgresCreateSequenceInstruction) Comment() string {
	return objectComment("Create", "sequence", i.Name)
}

type PostgresAlterSequenceInstruction struct {
	Name      string
	DataType  sql.NullString
	Increment sql.NullInt64
	Min       sql.NullInt64
	Max       sql.NullInt64
	Start     sql.NullInt64
	Cycle     sql.NullBool
	Restart   sql.NullInt64
}

func (i *PostgresAlterSequenceInstruction) String() string {
	var clauses []string

	if i.DataType.Valid {
		clauses = append(clauses, "AS "+i.DataType.String)
	}

	if i.Increment.Valid {
		clauses = append(clauses, fmt.Sprintf("INCREMENT BY %d", i.Increment.Int64))
	}

	if i.Min.Valid {
		clauses = append(clauses, fmt.Sprintf("MINVALUE %d", i.Min.Int64))
	}

	if i.Max.Valid {
		clauses = append(clauses, fmt.Sprintf("MAXVALUE %d", i.Max.Int64))
	}

	if i.Start.Valid {
		clauses = append(clauses, fmt.Sprintf("START WITH %d", i.Start.Int64))
	}

	if i.Cycle.Valid {
		clauses = append(clauses, sequenceCycleClause(i.Cycle.Bool))
	}

	if i.Restart.Valid {
		clauses = append(clauses, fmt.Sprintf("RESTART WITH %d", i.Restart.Int64))
	}

	return fmt.Sprintf("ALTER SEQUENCE %s %s;",
		QuoteIdentifier(i.Name), strings.Join(clauses, " "))
}

func (i *PostgresAlterSequenceInstruction) Comment() string {
	return objectComment("Modify", "sequence", i.Name)
}

type PostgresDropSequenceInstruction struct {
	Name string
}

func (i *PostgresDropSequenceInstruction) String() string {
	return "DROP SEQUENCE " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropSequenceInstruction) Comment() string {
	return objectComment("Drop", "sequence", i.Name)
}

type PostgresCreateStatisticsInstruction struct {
	Definition string
}

func (i *PostgresCreateStatisticsInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateStatisticsInstruction) Comment() string {
	return tableDefinitionComment("Create", "statistics object", i.Definition, "STATISTICS", "FROM")
}

type PostgresDropStatisticsInstruction struct {
	Name string
}

func (i *PostgresDropStatisticsInstruction) String() string {
	return "DROP STATISTICS " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropStatisticsInstruction) Comment() string {
	return objectComment("Drop", "statistics object", i.Name)
}

type PostgresGrantInstruction struct {
	Privileges []string
	ObjectType string
	ObjectName string
	Grantee    string
}

func (i *PostgresGrantInstruction) String() string {
	return fmt.Sprintf("GRANT %s ON %s %s TO %s;",
		joinPrivileges(i.Privileges), i.ObjectType,
		QuoteIdentifier(i.ObjectName), QuoteIdentifier(i.Grantee))
}

func (i *PostgresGrantInstruction) Comment() string {
	return ownedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
}

type PostgresRevokeInstruction struct {
	Privileges []string
	ObjectType string
	ObjectName string
	Grantee    string
}

func (i *PostgresRevokeInstruction) String() string {
	return fmt.Sprintf("REVOKE %s ON %s %s FROM %s;",
		joinPrivileges(i.Privileges), i.ObjectType,
		QuoteIdentifier(i.ObjectName), QuoteIdentifier(i.Grantee))
}

func (i *PostgresRevokeInstruction) Comment() string {
	return ownedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
}

type PostgresSetOwnerInstruction struct {
	ObjectType string
	ObjectName string
	Owner      string
}

func (i *PostgresSetOwnerInstruction) String() string {
	return fmt.Sprintf("ALTER %s %s OWNER TO %s;",
		i.ObjectType, QuoteIdentifier(i.ObjectName), QuoteIdentifier(i.Owner))
}

func (i *PostgresSetOwnerInstruction) Comment() string {
	return ownedObjectComment("Change", "owner", i.ObjectType, i.ObjectName)
}
