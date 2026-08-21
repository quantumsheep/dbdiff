package drivers

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

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

func (i *PostgresAlterTableInstruction) Comment() string {
	return objectComment("Modify", "table", i.Name)
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
		quoteIdentifier(a.ColumnName), a.DataType)

	if a.Collation != "" {
		clause += " COLLATE " + quoteIdentifier(a.Collation)
	}

	if a.UsingCast {
		clause += fmt.Sprintf(" USING %s::%s", quoteIdentifier(a.ColumnName), a.DataType)
	}

	return clause
}

type PostgresSetStorageAction struct {
	ColumnName string
	Storage    string
}

func (a *PostgresSetStorageAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET STORAGE %s",
		quoteIdentifier(a.ColumnName), a.Storage)
}

type PostgresSetStatisticsAction struct {
	ColumnName string
	Target     int64
}

func (a *PostgresSetStatisticsAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET STATISTICS %d",
		quoteIdentifier(a.ColumnName), a.Target)
}

type PostgresSetNotNullAction struct {
	ColumnName string
}

func (a *PostgresSetNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " SET NOT NULL"
}

type PostgresDropNotNullAction struct {
	ColumnName string
}

func (a *PostgresDropNotNullAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP NOT NULL"
}

type PostgresSetDefaultAction struct {
	ColumnName string
	Expression string
}

func (a *PostgresSetDefaultAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET DEFAULT %s",
		quoteIdentifier(a.ColumnName), a.Expression)
}

type PostgresDropDefaultAction struct {
	ColumnName string
}

func (a *PostgresDropDefaultAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP DEFAULT"
}

type PostgresAddIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresAddIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s ADD GENERATED %s AS IDENTITY",
		quoteIdentifier(a.ColumnName), a.Identity)
}

type PostgresSetIdentityAction struct {
	ColumnName string
	Identity   string
}

func (a *PostgresSetIdentityAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET GENERATED %s",
		quoteIdentifier(a.ColumnName), a.Identity)
}

type PostgresSetIdentityOptionsAction struct {
	ColumnName string
	Options    string
}

func (a *PostgresSetIdentityOptionsAction) TableActionClause() string {
	return fmt.Sprintf("ALTER COLUMN %s SET %s",
		quoteIdentifier(a.ColumnName), a.Options)
}

type PostgresDropIdentityAction struct {
	ColumnName string
}

func (a *PostgresDropIdentityAction) TableActionClause() string {
	return "ALTER COLUMN " + quoteIdentifier(a.ColumnName) + " DROP IDENTITY"
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
	return "DROP CONSTRAINT " + quoteIdentifier(a.ConstraintName)
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
		keyword, quoteIdentifier(i.Name), strings.Join(lines, ",\n"))

	if len(i.Inherits) > 0 {
		statement += " INHERITS (" + strings.Join(quoteIdentifiers(i.Inherits), ", ") + ")"
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
	return objectComment("Create", "table", i.Name)
}

type PostgresCreateTablePartitionInstruction struct {
	Name       string
	ParentName string
	Bound      string
}

func (i *PostgresCreateTablePartitionInstruction) String() string {
	return fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.ParentName), i.Bound)
}

func (i *PostgresCreateTablePartitionInstruction) Comment() string {
	return objectComment("Create", "table", i.Name)
}

type PostgresCreateIndexInstruction struct {
	Definition string
}

func (i *PostgresCreateIndexInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateIndexInstruction) Comment() string {
	return tableDefinitionComment("Create", "index", i.Definition, "INDEX", "ON")
}

type PostgresCreateTriggerInstruction struct {
	Definition string
}

func (i *PostgresCreateTriggerInstruction) String() string {
	return i.Definition + ";"
}

func (i *PostgresCreateTriggerInstruction) Comment() string {
	return tableDefinitionComment("Create", "trigger", i.Definition, "TRIGGER", "ON")
}

type PostgresDropTriggerInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropTriggerInstruction) String() string {
	return fmt.Sprintf("DROP TRIGGER %s ON %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))
}

func (i *PostgresDropTriggerInstruction) Comment() string {
	return tableObjectComment("Drop", "trigger", i.Name, i.TableName)
}

type PostgresTriggerEnableAction struct {
	Mode string

	TriggerName string
}

func (a *PostgresTriggerEnableAction) TableActionClause() string {
	return a.Mode + " TRIGGER " + quoteIdentifier(a.TriggerName)
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
		return "REPLICA IDENTITY " + a.Mode + " " + quoteIdentifier(a.IndexName)
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

func (i *PostgresCreatePolicyInstruction) Comment() string {
	return tableObjectComment("Create", "policy", i.Name, i.TableName)
}

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

type PostgresDropPolicyInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropPolicyInstruction) String() string {
	return fmt.Sprintf("DROP POLICY %s ON %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))
}

func (i *PostgresDropPolicyInstruction) Comment() string {
	return tableObjectComment("Drop", "policy", i.Name, i.TableName)
}

// The definition of pg_rules ends with a semicolon, so String adds no second one.
type PostgresCreateRuleInstruction struct {
	Definition string
}

func (i *PostgresCreateRuleInstruction) String() string {
	return i.Definition
}

func (i *PostgresCreateRuleInstruction) Comment() string {
	return tableDefinitionComment("Create", "rule", i.Definition, "RULE", "TO")
}

type PostgresDropRuleInstruction struct {
	Name      string
	TableName string
}

func (i *PostgresDropRuleInstruction) String() string {
	return fmt.Sprintf("DROP RULE %s ON %s;",
		quoteIdentifier(i.Name), quoteIdentifier(i.TableName))
}

func (i *PostgresDropRuleInstruction) Comment() string {
	return tableObjectComment("Drop", "rule", i.Name, i.TableName)
}

type PostgresCommentOnTableInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnTableInstruction) String() string {
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;",
		quoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnTableInstruction) Comment() string {
	return objectComment("Modify", "table", i.Name)
}

type PostgresCommentOnColumnInstruction struct {
	TableName  string
	ColumnName string
	Text       string
}

func (i *PostgresCommentOnColumnInstruction) String() string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;",
		quoteIdentifier(i.TableName), quoteIdentifier(i.ColumnName),
		commentLiteral(i.Text))
}

func (i *PostgresCommentOnColumnInstruction) Comment() string {
	return objectComment("Modify", "table", i.TableName)
}

func commentLiteral(comment string) string {
	if comment == "" {
		return sqlNullLiteral
	}

	return quoteLiteral(comment)
}

type PostgresCreateMaterializedViewInstruction struct {
	Name  string
	Query string
}

func (i *PostgresCreateMaterializedViewInstruction) String() string {
	return "CREATE MATERIALIZED VIEW " + quoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";") + ";"
}

func (i *PostgresCreateMaterializedViewInstruction) Comment() string {
	return objectComment("Create", "materialized view", i.Name)
}

type PostgresDropMaterializedViewInstruction struct {
	Name string
}

func (i *PostgresDropMaterializedViewInstruction) String() string {
	return "DROP MATERIALIZED VIEW " + quoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropMaterializedViewInstruction) Comment() string {
	return objectComment("Drop", "materialized view", i.Name)
}

type PostgresCreateViewInstruction struct {
	Name        string
	Query       string
	CheckOption string
}

func (i *PostgresCreateViewInstruction) String() string {
	statement := "CREATE VIEW " + quoteIdentifier(i.Name) + " AS " +
		strings.TrimSuffix(i.Query, ";")

	if i.CheckOption != "" {
		statement += " WITH " + i.CheckOption + " CHECK OPTION"
	}

	return statement + ";"
}

func (i *PostgresCreateViewInstruction) Comment() string {
	return objectComment("Create", "view", i.Name)
}
