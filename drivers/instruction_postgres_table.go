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
	Collation  string
	UsingCast  bool
}

func (a *PostgresAlterColumnTypeAction) TableActionClause() string {
	clause := fmt.Sprintf("ALTER COLUMN %s TYPE %s",
		quoteIdentifier(a.ColumnName), a.DataType)

	if a.Collation != "" {
		clause += " COLLATE " + quoteIdentifier(a.Collation)
	}

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

// ALTER COLUMN column_name ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY
// PostgreSQL refuses this action while the column accepts a null value, so the diff sets
// the NOT NULL flag of the column first.
type PostgresAddIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresAddIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s ADD GENERATED %s AS IDENTITY",
		quoteIdentifier(a.ColumnName), a.Identity)
}

// ALTER COLUMN column_name SET GENERATED { ALWAYS | BY DEFAULT }
type PostgresSetIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresSetIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET GENERATED %s",
		quoteIdentifier(a.ColumnName), a.Identity)
}

// ALTER COLUMN column_name DROP IDENTITY
// This action keeps the NOT NULL flag of the column. PostgreSQL refuses to remove that
// flag from an identity column, so the diff prints this action first.
type PostgresDropIdentityAction struct {
	ColumnName string
}

func (a *PostgresDropIdentityAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP IDENTITY"
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

	// PartitionKey holds the key of a partitioned table, for example RANGE (created). It
	// is empty for every other table.
	PartitionKey string

	// Comment holds the comment of the table. A separate COMMENT ON statement writes it,
	// because CREATE TABLE accepts no comment.
	Comment string

	// Inherits names the parent of a table of INHERITS. PostgreSQL merges a column that
	// the two tables both declare, so the statement keeps every column.
	Inherits []string
}

func (i *PostgresCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	for _, constraint := range i.Constraints {
		lines = append(lines, "\t"+constraint.Clause())
	}

	statement := fmt.Sprintf("CREATE TABLE %s (\n%s\n)",
		quoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(i.Inherits) > 0 {
		statement += " INHERITS (" + strings.Join(quoteIdentifiers(i.Inherits), ", ") + ")"
	}

	if i.PartitionKey != "" {
		statement += " PARTITION BY " + i.PartitionKey
	}

	return statement + ";"
}

// CREATE TABLE name PARTITION OF parent_name bound
// A partition takes the columns and the constraints of its parent, so the statement names
// neither of them.
type PostgresCreateTablePartitionInstruction struct {
	Name       string
	ParentName string
	Bound      string
}

func (i *PostgresCreateTablePartitionInstruction) String() string {
	return fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.ParentName), i.Bound)
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

// { ENABLE | DISABLE | FORCE | NO FORCE } ROW LEVEL SECURITY
type PostgresRowLevelSecurityAction struct {
	Mode string
}

func (a *PostgresRowLevelSecurityAction) TableActionClause() string {
	return a.Mode + " ROW LEVEL SECURITY"
}

// CREATE POLICY name ON table_name AS permissive FOR command TO role [, ...]
//
//	[ USING ( expression ) ] [ WITH CHECK ( expression ) ]
//
// The keyword PUBLIC names every role, so that name takes no quotes.
type PostgresCreatePolicyInstruction struct {
	Name       string
	TableName  string
	Permissive string
	Command    string
	Roles      []string
	Using      string
	WithCheck  string
}

func (i *PostgresCreatePolicyInstruction) String() string {
	statement := fmt.Sprintf("CREATE POLICY %s ON %s",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))

	if i.Permissive != "" {
		statement += " AS " + i.Permissive
	}

	if i.Command != "" {
		statement += " FOR " + i.Command
	}

	if len(i.Roles) > 0 {
		statement += " TO " + strings.Join(policyRoleNames(i.Roles), ", ")
	}

	if i.Using != "" {
		statement += " USING " + i.Using
	}

	if i.WithCheck != "" {
		statement += " WITH CHECK " + i.WithCheck
	}

	return statement + ";"
}

// policyRoleNames quotes each role name. The name public is the keyword PUBLIC, so it
// keeps no quotes.
func policyRoleNames(roles []string) []string {
	names := make([]string, 0, len(roles))

	for _, role := range roles {
		if strings.EqualFold(role, "public") {
			names = append(names, role)
			continue
		}

		names = append(names, quoteIdentifier(role))
	}

	return names
}

// DROP POLICY name ON table_name
type PostgresDropPolicyInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropPolicyInstruction) String() string {
	return fmt.Sprintf("DROP POLICY %s ON %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))
}

// COMMENT ON TABLE name IS comment
// An empty comment gives the keyword NULL, which removes the comment.
type PostgresCommentOnTableInstruction struct {
	Name    string
	Comment string
}

func (i *PostgresCommentOnTableInstruction) String() string {
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;",
		quoteIdentifier(i.Name), commentLiteral(i.Comment))
}

// COMMENT ON COLUMN table_name.column_name IS comment
type PostgresCommentOnColumnInstruction struct {
	TableName  string
	ColumnName string
	Comment    string
}

func (i *PostgresCommentOnColumnInstruction) String() string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;",
		quoteIdentifier(i.TableName), quoteIdentifier(i.ColumnName),
		commentLiteral(i.Comment))
}

// commentLiteral gives the text of a comment, or the keyword NULL for an empty comment.
func commentLiteral(comment string) string {
	if comment == "" {
		return sqlNullLiteral
	}

	return quoteLiteral(comment)
}

// CREATE MATERIALIZED VIEW name AS query
// The query comes from pg_matviews.definition, and it ends with a semicolon. String
// removes that semicolon and adds one, so every instruction ends the same way.
type PostgresCreateMaterializedViewInstruction struct {
	Name  string
	Query string
}

func (i *PostgresCreateMaterializedViewInstruction) String() string {
	return "CREATE MATERIALIZED VIEW " + quoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";") + ";"
}

// DROP MATERIALIZED VIEW name
type PostgresDropMaterializedViewInstruction struct {
	Name string
}

func (i *PostgresDropMaterializedViewInstruction) String() string {
	return "DROP MATERIALIZED VIEW " + quoteIdentifier(i.Name) + ";"
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
