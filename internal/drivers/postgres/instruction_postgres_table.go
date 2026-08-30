package driverspostgres

import (
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresAlterTableInstruction struct {
	Name    string
	Actions []driversshared.AlterTableAction
}

func (i *PostgresAlterTableInstruction) String() string {
	clauses := lo.Map(i.Actions, func(action driversshared.AlterTableAction, _ int) string {
		return action.TableActionClause()
	})

	return fmt.Sprintf("ALTER TABLE %s %s;",
		driversshared.QuoteIdentifier(i.Name), strings.Join(clauses, ", "))
}

func (i *PostgresAlterTableInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "table", i.Name)
}

type PostgresAddColumnAction struct {
	Column *PostgresColumn
}

func (a *PostgresAddColumnAction) TableActionClause() string {
	return "ADD COLUMN " + a.Column.Definition()
}

type PostgresAlterColumnTypeAction struct {
	ColumnName string
	DataType   string
	Collation  string
	UsingCast  bool
}

func (a *PostgresAlterColumnTypeAction) TableActionClause() string {
	clause := fmt.Sprintf("ALTER COLUMN %s TYPE %s",
		driversshared.QuoteIdentifier(a.ColumnName), a.DataType)

	if a.Collation != "" {
		clause += " COLLATE " + driversshared.QuoteIdentifier(a.Collation)
	}

	if a.UsingCast {
		clause += fmt.Sprintf(" USING %s::%s", driversshared.QuoteIdentifier(a.ColumnName), a.DataType)
	}

	return clause
}

type PostgresSetStorageAction struct {
	ColumnName string
	Storage    string
}

func (a *PostgresSetStorageAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET STORAGE %s",
		driversshared.QuoteIdentifier(a.ColumnName), a.Storage)
}

type PostgresSetStatisticsAction struct {
	ColumnName string
	Source     int64
}

func (a *PostgresSetStatisticsAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET STATISTICS %d",
		driversshared.QuoteIdentifier(a.ColumnName), a.Source)
}

type PostgresSetNotNullAction struct {
	ColumnName string
}

func (a *PostgresSetNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + driversshared.QuoteIdentifier(a.ColumnName) + " SET NOT NULL"
}

type PostgresDropNotNullAction struct {
	ColumnName string
}

func (a *PostgresDropNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + driversshared.QuoteIdentifier(a.ColumnName) + " DROP NOT NULL"
}

type PostgresSetDefaultAction struct {
	ColumnName string
	Expression string
}

func (a *PostgresSetDefaultAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s",
		driversshared.QuoteIdentifier(a.ColumnName), a.Expression)
}

type PostgresDropDefaultAction struct {
	ColumnName string
}

func (a *PostgresDropDefaultAction) TableActionClause() string {
	return "ALTER COLUMN " + driversshared.QuoteIdentifier(a.ColumnName) + " DROP DEFAULT"
}

type PostgresAddIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresAddIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s ADD GENERATED %s AS IDENTITY",
		driversshared.QuoteIdentifier(a.ColumnName), a.Identity)
}

type PostgresSetIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresSetIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET GENERATED %s",
		driversshared.QuoteIdentifier(a.ColumnName), a.Identity)
}

type PostgresDetachPartitionInstruction struct {
	ParentName    string
	PartitionName string
}

func (i *PostgresDetachPartitionInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s;",
		driversshared.QuoteIdentifier(i.ParentName), driversshared.QuoteIdentifier(i.PartitionName))
}

func (i *PostgresDetachPartitionInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Detach", "partition", "table", i.ParentName)
}

type PostgresAttachPartitionInstruction struct {
	ParentName    string
	PartitionName string
	Bound         string
}

func (i *PostgresAttachPartitionInstruction) String() string {
	return fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s %s;",
		driversshared.QuoteIdentifier(i.ParentName), driversshared.QuoteIdentifier(i.PartitionName), i.Bound)
}

func (i *PostgresAttachPartitionInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Attach", "partition", "table", i.ParentName)
}

type PostgresInheritAction struct {
	ParentName string
}

func (a *PostgresInheritAction) TableActionClause() string {
	return "INHERIT " + driversshared.QuoteIdentifier(a.ParentName)
}

type PostgresNoInheritAction struct {
	ParentName string
}

func (a *PostgresNoInheritAction) TableActionClause() string {
	return "NO INHERIT " + driversshared.QuoteIdentifier(a.ParentName)
}

type PostgresSetIdentityOptionsAction struct {
	ColumnName string
	Options    string
}

// PostgreSQL takes one SET for each sequence option of the identity.
func (a *PostgresSetIdentityOptionsAction) TableActionClause() string {
	clauses := lo.Map(splitSequenceOptions(a.Options), func(option string, _ int) string {
		return "SET " + option
	})

	return fmt.Sprintf("ALTER COLUMN %s %s",
		driversshared.QuoteIdentifier(a.ColumnName), strings.Join(clauses, " "))
}

func splitSequenceOptions(options string) []string {
	var parts []string
	var part []string

	for _, token := range strings.Fields(options) {
		lastIsNo := len(part) > 0 && strings.EqualFold(part[len(part)-1], "NO")

		if isSequenceOptionStart(token) && len(part) > 0 && !lastIsNo {
			parts = append(parts, strings.Join(part, " "))
			part = nil
		}

		part = append(part, token)
	}

	if len(part) > 0 {
		parts = append(parts, strings.Join(part, " "))
	}

	return parts
}

func isSequenceOptionStart(token string) bool {
	switch strings.ToUpper(token) {
	case "START", "INCREMENT", "MINVALUE", "MAXVALUE", "CACHE", "CYCLE", "NO", "RESTART":
		return true
	}

	return false
}

type PostgresDropIdentityAction struct {
	ColumnName string
}

func (a *PostgresDropIdentityAction) TableActionClause() string {
	return "ALTER COLUMN " + driversshared.QuoteIdentifier(a.ColumnName) + " DROP IDENTITY"
}

type PostgresAddConstraintAction struct {
	Constraint *PostgresConstraint
}

func (a *PostgresAddConstraintAction) TableActionClause() string {
	return "ADD " + a.Constraint.Clause()
}

type PostgresDropConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropConstraintAction) TableActionClause() string {
	return "DROP CONSTRAINT " + driversshared.QuoteIdentifier(a.ConstraintName)
}

type PostgresCreateTableInstruction struct {
	Name              string
	Columns           []*PostgresColumn
	Constraints       []*PostgresConstraint
	PartitionKey      string
	Inherits          []string
	Unlogged          bool
	StorageParameters []string
}

func (i *PostgresCreateTableInstruction) String() string {
	var lines []string

	for _, column := range i.Columns {
		lines = append(lines, "\t"+column.Definition())
	}

	for _, constraint := range i.Constraints {
		lines = append(lines, "\t"+constraint.Clause())
	}

	keyword := "CREATE TABLE"
	if i.Unlogged {
		keyword = "CREATE UNLOGGED TABLE"
	}

	statement := fmt.Sprintf("%s %s (\n%s\n)",
		keyword, driversshared.QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(i.Inherits) > 0 {
		statement += " INHERITS (" + strings.Join(driversshared.QuoteIdentifiers(i.Inherits), ", ") + ")"
	}

	if len(i.StorageParameters) > 0 {
		statement += " WITH (" + strings.Join(i.StorageParameters, ", ") + ")"
	}

	if i.PartitionKey != "" {
		statement += " PARTITION BY " + i.PartitionKey
	}

	return statement + ";"
}

func (i *PostgresCreateTableInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "table", i.Name)
}

type PostgresCreateTablePartitionInstruction struct {
	Name       string
	ParentName string
	Bound      string
}

func (i *PostgresCreateTablePartitionInstruction) String() string {
	return fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.ParentName), i.Bound)
}

func (i *PostgresCreateTablePartitionInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "table", i.Name)
}

type PostgresCreateIndexInstruction struct {
	Definition string
}

func (i *PostgresCreateIndexInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateIndexInstruction) Comment() string {
	return driversshared.TableDefinitionComment("Create", "index", i.Definition, "INDEX", "ON")
}

type PostgresCreateTriggerInstruction struct {
	Definition string
}

func (i *PostgresCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateTriggerInstruction) Comment() string {
	return driversshared.TableDefinitionComment("Create", "trigger", i.Definition, "TRIGGER", "ON")
}

type PostgresDropTriggerInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropTriggerInstruction) String() string {
	return fmt.Sprintf("DROP TRIGGER %s ON %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.TableName))
}

func (i *PostgresDropTriggerInstruction) Comment() string {
	return driversshared.TableObjectComment("Drop", "trigger", i.Name, i.TableName)
}

type PostgresTriggerEnableAction struct {
	Mode string

	TriggerName string
}

func (a *PostgresTriggerEnableAction) TableActionClause() string {
	return a.Mode + " TRIGGER " + driversshared.QuoteIdentifier(a.TriggerName)
}

type PostgresSetStorageParametersAction struct {
	Parameters []string
}

func (a *PostgresSetStorageParametersAction) TableActionClause() string {
	return "SET (" + strings.Join(a.Parameters, ", ") + ")"
}

type PostgresResetStorageParametersAction struct {
	Names []string
}

func (a *PostgresResetStorageParametersAction) TableActionClause() string {
	return "RESET (" + strings.Join(a.Names, ", ") + ")"
}

type PostgresSetPersistenceAction struct {
	Persistence string
}

func (a *PostgresSetPersistenceAction) TableActionClause() string {
	return "SET " + a.Persistence
}

type PostgresReplicaIdentityAction struct {
	Mode      string
	IndexName string
}

func (a *PostgresReplicaIdentityAction) TableActionClause() string {
	if a.IndexName != "" {
		return "REPLICA IDENTITY " + a.Mode + " " + driversshared.QuoteIdentifier(a.IndexName)
	}

	return "REPLICA IDENTITY " + a.Mode
}

type PostgresRowLevelSecurityAction struct {
	Mode string
}

func (a *PostgresRowLevelSecurityAction) TableActionClause() string {
	return a.Mode + " ROW LEVEL SECURITY"
}

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
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.TableName))

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

func (i *PostgresCreatePolicyInstruction) Comment() string {
	return driversshared.TableObjectComment("Create", "policy", i.Name, i.TableName)
}

func policyRoleNames(roles []string) []string {
	names := make([]string, 0, len(roles))

	for _, role := range roles {
		if strings.EqualFold(role, "public") {
			names = append(names, role)
			continue
		}

		names = append(names, driversshared.QuoteIdentifier(role))
	}

	return names
}

type PostgresDropPolicyInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropPolicyInstruction) String() string {
	return fmt.Sprintf("DROP POLICY %s ON %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.TableName))
}

func (i *PostgresDropPolicyInstruction) Comment() string {
	return driversshared.TableObjectComment("Drop", "policy", i.Name, i.TableName)
}

// The definition of pg_rules ends with a semicolon, so String adds no second one.
type PostgresCreateRuleInstruction struct {
	Definition string
}

func (i *PostgresCreateRuleInstruction) String() string {
	return i.Definition
}

func (i *PostgresCreateRuleInstruction) Comment() string {
	return driversshared.TableDefinitionComment("Create", "rule", i.Definition, "RULE", "TO")
}

type PostgresDropRuleInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropRuleInstruction) String() string {
	return fmt.Sprintf("DROP RULE %s ON %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.TableName))
}

func (i *PostgresDropRuleInstruction) Comment() string {
	return driversshared.TableObjectComment("Drop", "rule", i.Name, i.TableName)
}

type PostgresCommentOnTableInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnTableInstruction) String() string {
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;",
		driversshared.QuoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnTableInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "table", i.Name)
}

type PostgresCommentOnColumnInstruction struct {
	TableName  string
	ColumnName string
	Text       string
}

func (i *PostgresCommentOnColumnInstruction) String() string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;",
		driversshared.QuoteIdentifier(i.TableName), driversshared.QuoteIdentifier(i.ColumnName),
		commentLiteral(i.Text))
}

func (i *PostgresCommentOnColumnInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "table", i.TableName)
}

type PostgresCommentOnViewInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnViewInstruction) String() string {
	return fmt.Sprintf("COMMENT ON VIEW %s IS %s;",
		driversshared.QuoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnViewInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "view", i.Name)
}

type PostgresCommentOnMaterializedViewInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnMaterializedViewInstruction) String() string {
	return fmt.Sprintf("COMMENT ON MATERIALIZED VIEW %s IS %s;",
		driversshared.QuoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnMaterializedViewInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "materialized view", i.Name)
}

type PostgresCommentOnIndexInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnIndexInstruction) String() string {
	return fmt.Sprintf("COMMENT ON INDEX %s IS %s;",
		driversshared.QuoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnIndexInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "index", i.Name)
}

func commentLiteral(comment string) string {
	if comment == "" {
		return driversshared.SQLNullLiteral
	}

	return driversshared.QuoteLiteral(comment)
}

type PostgresCreateMaterializedViewInstruction struct {
	Name  string
	Query string
}

func (i *PostgresCreateMaterializedViewInstruction) String() string {
	return "CREATE MATERIALIZED VIEW " + driversshared.QuoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";") + ";"
}

func (i *PostgresCreateMaterializedViewInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "materialized view", i.Name)
}

type PostgresDropMaterializedViewInstruction struct {
	Name string
}

func (i *PostgresDropMaterializedViewInstruction) String() string {
	return "DROP MATERIALIZED VIEW " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropMaterializedViewInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "materialized view", i.Name)
}

type PostgresCreateViewInstruction struct {
	Name        string
	Query       string
	CheckOption string
}

func (i *PostgresCreateViewInstruction) String() string {
	statement := "CREATE VIEW " + driversshared.QuoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";")

	if i.CheckOption != "" {
		statement += " WITH " + i.CheckOption + " CHECK OPTION"
	}

	return statement + ";"
}

func (i *PostgresCreateViewInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "view", i.Name)
}

type PostgresInsertOverridingInstruction struct {
	TableName   string
	ColumnNames []string
	Expressions []string
}

func (i *PostgresInsertOverridingInstruction) String() string {
	return fmt.Sprintf("INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE VALUES (%s);",
		driversshared.QuoteIdentifier(i.TableName),
		strings.Join(driversshared.QuoteIdentifiers(i.ColumnNames), ", "),
		strings.Join(i.Expressions, ", "))
}

func (i *PostgresInsertOverridingInstruction) Comment() string {
	return driversshared.OwnedObjectComment("Change", "rows", "table", i.TableName)
}
