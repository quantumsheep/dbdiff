package driverspostgres

import (
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
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
	DataType   string
	TableName  string
	ColumnName string
}

func (i *PostgresCreateOwnedSequenceInstruction) String() string {
	return fmt.Sprintf("CREATE SEQUENCE %s AS %s OWNED BY %s.%s;",
		driversshared.QuoteIdentifier(i.Name), i.DataType,
		driversshared.QuoteIdentifier(i.TableName), driversshared.QuoteIdentifier(i.ColumnName))
}

func (i *PostgresCreateOwnedSequenceInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "sequence", i.Name)
}

type PostgresAlterSequenceOwnerInstruction struct {
	Name       string
	TableName  string
	ColumnName string
}

func (i *PostgresAlterSequenceOwnerInstruction) String() string {
	owner := "NONE"
	if i.ColumnName != "" {
		owner = driversshared.QuoteIdentifier(i.TableName) + "." + driversshared.QuoteIdentifier(i.ColumnName)
	}

	return fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s;", driversshared.QuoteIdentifier(i.Name), owner)
}

func (i *PostgresAlterSequenceOwnerInstruction) Comment() string {
	return driversshared.ObjectComment("Modify the owner column of", "sequence", i.Name)
}

// A new sequence of a column that holds rows starts below the values of the column. The
// statement sets the next value above them and never below the start value. HAVING skips
// an empty table, so the sequence keeps its start value.
type PostgresRestartOwnedSequenceInstruction struct {
	TableName  string
	ColumnName string
}

func (i *PostgresRestartOwnedSequenceInstruction) String() string {
	sequence := fmt.Sprintf("pg_get_serial_sequence(%s, %s)",
		driversshared.QuoteLiteral(driversshared.QuoteIdentifier(i.TableName)), driversshared.QuoteLiteral(i.ColumnName))
	column := driversshared.QuoteIdentifier(i.ColumnName)

	return fmt.Sprintf(
		"SELECT setval(%[1]s, greatest(max(%[2]s) + 1, (SELECT seqstart FROM pg_sequence WHERE seqrelid = %[1]s::regclass)), false) "+
			"FROM %[3]s HAVING max(%[2]s) IS NOT NULL;",
		sequence, column, driversshared.QuoteIdentifier(i.TableName))
}

func (i *PostgresRestartOwnedSequenceInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Restart", "sequence", "column", i.ColumnName) +
		" of the table " + driversshared.QuoteIdentifier(i.TableName)
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
	Privileges      []string
	ObjectType      string
	ObjectName      string
	Arguments       string
	Column          string
	Grantee         string
	WithGrantOption bool
}

func (i *PostgresGrantInstruction) String() string {
	return fmt.Sprintf("GRANT %s ON %s %s TO %s%s;",
		privilegeList(i.Privileges, i.Column), i.ObjectType,
		quotePrivilegeObject(i.ObjectType, i.ObjectName, i.Arguments), quoteGrantee(i.Grantee),
		withGrantOptionClause(i.WithGrantOption))
}

func (i *PostgresGrantInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
}

type PostgresRevokeInstruction struct {
	Privileges     []string
	ObjectType     string
	ObjectName     string
	Arguments      string
	Column         string
	Grantee        string
	GrantOptionFor bool
}

func (i *PostgresRevokeInstruction) String() string {
	return fmt.Sprintf("REVOKE %s%s ON %s %s FROM %s;",
		grantOptionForClause(i.GrantOptionFor), privilegeList(i.Privileges, i.Column), i.ObjectType,
		quotePrivilegeObject(i.ObjectType, i.ObjectName, i.Arguments), quoteGrantee(i.Grantee))
}

func (i *PostgresRevokeInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "privileges", i.ObjectType, i.ObjectName)
}

type PostgresGrantDefaultPrivilegesInstruction struct {
	Role            string
	Schema          string
	Privileges      []string
	ObjectType      string
	Grantee         string
	WithGrantOption bool
}

func (i *PostgresGrantDefaultPrivilegesInstruction) String() string {
	return fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT %s ON %s TO %s%s;",
		driversshared.QuoteIdentifier(i.Role), driversshared.QuoteIdentifier(i.Schema),
		joinPrivileges(i.Privileges), i.ObjectType, quoteGrantee(i.Grantee),
		withGrantOptionClause(i.WithGrantOption))
}

func (i *PostgresGrantDefaultPrivilegesInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "default privileges", "schema", i.Schema)
}

type PostgresRevokeDefaultPrivilegesInstruction struct {
	Role           string
	Schema         string
	Privileges     []string
	ObjectType     string
	Grantee        string
	GrantOptionFor bool
}

func (i *PostgresRevokeDefaultPrivilegesInstruction) String() string {
	return fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s REVOKE %s%s ON %s FROM %s;",
		driversshared.QuoteIdentifier(i.Role), driversshared.QuoteIdentifier(i.Schema),
		grantOptionForClause(i.GrantOptionFor), joinPrivileges(i.Privileges), i.ObjectType,
		quoteGrantee(i.Grantee))
}

func (i *PostgresRevokeDefaultPrivilegesInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "default privileges", "schema", i.Schema)
}

type PostgresSetOwnerInstruction struct {
	ObjectType string
	ObjectName string
	Arguments  string
	Owner      string
}

func (i *PostgresSetOwnerInstruction) String() string {
	return fmt.Sprintf("ALTER %s %s OWNER TO %s;",
		i.ObjectType, quotePrivilegeObject(i.ObjectType, i.ObjectName, i.Arguments),
		driversshared.QuoteIdentifier(i.Owner))
}

func (i *PostgresSetOwnerInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "owner", i.ObjectType, i.ObjectName)
}

// Overloads of a routine share one name, so the argument types select the routine.
// ALTER AGGREGATE refuses an empty argument list, and the star names no argument.
func quotePrivilegeObject(objectType string, objectName string, arguments string) string {
	if !isRoutineObjectType(objectType) {
		return driversshared.QuoteIdentifier(objectName)
	}

	if objectType == "AGGREGATE" && arguments == "" {
		return driversshared.QuoteIdentifier(objectName) + "(*)"
	}

	return driversshared.QuoteIdentifier(objectName) + "(" + arguments + ")"
}

// A column privilege names the column after each privilege.
func privilegeList(privileges []string, column string) string {
	if column == "" {
		return joinPrivileges(privileges)
	}

	return joinPrivileges(lo.Map(privileges, func(privilege string, _ int) string {
		return privilege + " (" + driversshared.QuoteIdentifier(column) + ")"
	}))
}

// PUBLIC is a keyword, and the quoted name "PUBLIC" names a role.
func quoteGrantee(grantee string) string {
	if grantee == "public" {
		return "PUBLIC"
	}

	return driversshared.QuoteIdentifier(grantee)
}

func withGrantOptionClause(withGrantOption bool) string {
	if withGrantOption {
		return " WITH GRANT OPTION"
	}

	return ""
}

func grantOptionForClause(grantOptionFor bool) string {
	if grantOptionFor {
		return "GRANT OPTION FOR "
	}

	return ""
}
