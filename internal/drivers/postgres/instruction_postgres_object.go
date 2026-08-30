package driverspostgres

import (
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresCreateExtensionInstruction struct {
	Name string
}

func (i *PostgresCreateExtensionInstruction) String() string {
	return "CREATE EXTENSION " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresCreateExtensionInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "extension", i.Name)
}

type PostgresAlterExtensionInstruction struct {
	Name       string
	NewVersion string
}

func (i *PostgresAlterExtensionInstruction) String() string {
	return fmt.Sprintf("ALTER EXTENSION %s UPDATE TO %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteLiteral(i.NewVersion))
}

func (i *PostgresAlterExtensionInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "extension", i.Name)
}

type PostgresDropExtensionInstruction struct {
	Name string
}

func (i *PostgresDropExtensionInstruction) String() string {
	return "DROP EXTENSION " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropExtensionInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "extension", i.Name)
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
	Cache     int64
	Cycle     bool
}

func (i *PostgresCreateSequenceInstruction) String() string {
	return fmt.Sprintf(
		"CREATE SEQUENCE %s AS %s INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d CACHE %d %s;",
		driversshared.QuoteIdentifier(i.Name),
		i.DataType,
		i.Increment,
		i.Min,
		i.Max,
		i.Start,
		i.Cache,
		sequenceCycleClause(i.Cycle),
	)
}

func (i *PostgresCreateSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "sequence", i.Name)
}

type PostgresCreateOwnedSequenceInstruction struct {
	Name       string
	TableName  string
	ColumnName string
}

func (i *PostgresCreateOwnedSequenceInstruction) String() string {
	return fmt.Sprintf("CREATE SEQUENCE %s OWNED BY %s.%s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.TableName), driversshared.QuoteIdentifier(i.ColumnName))
}

func (i *PostgresCreateOwnedSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "sequence", i.Name)
}

type PostgresAlterSequenceInstruction struct {
	Name      string
	DataType  sql.NullString
	Increment sql.NullInt64
	Min       sql.NullInt64
	Max       sql.NullInt64
	Start     sql.NullInt64
	Cache     sql.NullInt64
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

	if i.Cache.Valid {
		clauses = append(clauses, fmt.Sprintf("CACHE %d", i.Cache.Int64))
	}

	if i.Cycle.Valid {
		clauses = append(clauses, sequenceCycleClause(i.Cycle.Bool))
	}

	if i.Restart.Valid {
		clauses = append(clauses, fmt.Sprintf("RESTART WITH %d", i.Restart.Int64))
	}

	return fmt.Sprintf("ALTER SEQUENCE %s %s;",
		driversshared.QuoteIdentifier(i.Name), strings.Join(clauses, " "))
}

func (i *PostgresAlterSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "sequence", i.Name)
}

type PostgresDropSequenceInstruction struct {
	Name string
}

func (i *PostgresDropSequenceInstruction) String() string {
	return "DROP SEQUENCE " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "sequence", i.Name)
}

type PostgresCreateStatisticsInstruction struct {
	Definition string
}

func (i *PostgresCreateStatisticsInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateStatisticsInstruction) Comment() string {
	return driversshared.TableDefinitionComment("Create", "statistics object", i.Definition, "STATISTICS", "FROM")
}

type PostgresDropStatisticsInstruction struct {
	Name string
}

func (i *PostgresDropStatisticsInstruction) String() string {
	return "DROP STATISTICS " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropStatisticsInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "statistics object", i.Name)
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
		driversshared.QuoteIdentifier(i.ObjectName), driversshared.QuoteIdentifier(i.Grantee))
}

func (i *PostgresGrantInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
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
		driversshared.QuoteIdentifier(i.ObjectName), driversshared.QuoteIdentifier(i.Grantee))
}

func (i *PostgresRevokeInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
}

type PostgresSetOwnerInstruction struct {
	ObjectType string
	ObjectName string
	Owner      string
}

func (i *PostgresSetOwnerInstruction) String() string {
	return fmt.Sprintf("ALTER %s %s OWNER TO %s;",
		i.ObjectType, driversshared.QuoteIdentifier(i.ObjectName), driversshared.QuoteIdentifier(i.Owner))
}

func (i *PostgresSetOwnerInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "owner", i.ObjectType, i.ObjectName)
}
