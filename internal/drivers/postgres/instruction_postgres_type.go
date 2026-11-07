package driverspostgres

import (
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresCreateEnumTypeInstruction struct {
	Name   string
	Labels []string
}

func (i *PostgresCreateEnumTypeInstruction) String() string {
	labels := lo.Map(i.Labels, func(label string, _ int) string {
		return driversshared.QuoteLiteral(label)
	})

	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
		driversshared.QuoteIdentifier(i.Name), strings.Join(labels, ", "))
}

func (i *PostgresCreateEnumTypeInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "type", i.Name)
}

// If Before and After are empty, PostgreSQL adds the value at the end.
type PostgresAlterTypeAddValueInstruction struct {
	Name   string
	Value  string
	Before string
	After  string
}

func (i *PostgresAlterTypeAddValueInstruction) String() string {
	position := ""

	if i.Before != "" {
		position = " BEFORE " + driversshared.QuoteLiteral(i.Before)
	} else if i.After != "" {
		position = " AFTER " + driversshared.QuoteLiteral(i.After)
	}

	return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s%s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteLiteral(i.Value), position)
}

func (i *PostgresAlterTypeAddValueInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "type", i.Name)
}

// PostgreSQL accepts ALTER TYPE ... ADD VALUE in a transaction, and it refuses the new
// value in that same transaction.
func (i *PostgresAlterTypeAddValueInstruction) SeparateMigrationFile() {}

type PostgresDropTypeInstruction struct {
	Name string
}

func (i *PostgresDropTypeInstruction) String() string {
	return "DROP TYPE " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropTypeInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "type", i.Name)
}

// ObjectType is TYPE or DOMAIN, because PostgreSQL refuses ALTER TYPE on a domain.
type PostgresRenameTypeInstruction struct {
	ObjectType string
	Name       string
	NewName    string
}

func (i *PostgresRenameTypeInstruction) String() string {
	return fmt.Sprintf("ALTER %s %s RENAME TO %s;",
		i.ObjectType, driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.NewName))
}

func (i *PostgresRenameTypeInstruction) Comment() string {
	return driversshared.ObjectComment("Rename", strings.ToLower(i.ObjectType), i.Name)
}

type PostgresAlterTypeAddAttributeInstruction struct {
	Name      string
	Attribute *PostgresCompositeTypeAttribute
}

func (i *PostgresAlterTypeAddAttributeInstruction) String() string {
	return fmt.Sprintf("ALTER TYPE %s ADD ATTRIBUTE %s %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.Attribute.Name), i.Attribute.Type)
}

func (i *PostgresAlterTypeAddAttributeInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "type", i.Name)
}

type PostgresAlterTypeDropAttributeInstruction struct {
	Name          string
	AttributeName string
}

func (i *PostgresAlterTypeDropAttributeInstruction) String() string {
	return fmt.Sprintf("ALTER TYPE %s DROP ATTRIBUTE %s;",
		driversshared.QuoteIdentifier(i.Name), driversshared.QuoteIdentifier(i.AttributeName))
}

func (i *PostgresAlterTypeDropAttributeInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "type", i.Name)
}

type PostgresCreateCompositeTypeInstruction struct {
	Name       string
	Attributes []*PostgresCompositeTypeAttribute
}

func (i *PostgresCreateCompositeTypeInstruction) String() string {
	lines := lo.Map(i.Attributes, func(attribute *PostgresCompositeTypeAttribute, _ int) string {
		return fmt.Sprintf("\t%s %s", driversshared.QuoteIdentifier(attribute.Name), attribute.Type)
	})

	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n);",
		driversshared.QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))
}

func (i *PostgresCreateCompositeTypeInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "type", i.Name)
}

type PostgresCreateDomainInstruction struct {
	Name        string
	BaseType    string
	Default     sql.NullString
	NotNull     bool
	Constraints []*PostgresDomainConstraint
}

func (i *PostgresCreateDomainInstruction) String() string {
	statement := fmt.Sprintf("CREATE DOMAIN %s AS %s", driversshared.QuoteIdentifier(i.Name), i.BaseType)

	if i.Default.Valid {
		statement += " DEFAULT " + i.Default.String
	}

	if i.NotNull {
		statement += " NOT NULL"
	}

	clauses := lo.Map(i.Constraints, func(constraint *PostgresDomainConstraint, _ int) string {
		return fmt.Sprintf("CONSTRAINT %s %s",
			driversshared.QuoteIdentifier(constraint.Name), constraint.Def)
	})

	if len(clauses) > 0 {
		statement += " " + strings.Join(clauses, " ")
	}

	return statement + ";"
}

func (i *PostgresCreateDomainInstruction) Comment() string {
	return driversshared.ObjectComment("Create", "domain", i.Name)
}

type PostgresDropDomainInstruction struct {
	Name string
}

func (i *PostgresDropDomainInstruction) String() string {
	return "DROP DOMAIN " + driversshared.QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropDomainInstruction) Comment() string {
	return driversshared.ObjectComment("Drop", "domain", i.Name)
}

type PostgresAlterDomainInstruction struct {
	Name   string
	Action driversshared.AlterDomainAction
}

func (i *PostgresAlterDomainInstruction) String() string {
	return fmt.Sprintf("ALTER DOMAIN %s %s;",
		driversshared.QuoteIdentifier(i.Name), i.Action.DomainActionClause())
}

func (i *PostgresAlterDomainInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "domain", i.Name)
}

type PostgresSetDomainDefaultAction struct {
	Expression string
}

func (a *PostgresSetDomainDefaultAction) DomainActionClause() string {
	return "SET DEFAULT " + a.Expression
}

type PostgresDropDomainDefaultAction struct{}

func (a *PostgresDropDomainDefaultAction) DomainActionClause() string {
	return "DROP DEFAULT"
}

type PostgresSetDomainNotNullAction struct{}

func (a *PostgresSetDomainNotNullAction) DomainActionClause() string {
	return "SET NOT NULL"
}

type PostgresDropDomainNotNullAction struct{}

func (a *PostgresDropDomainNotNullAction) DomainActionClause() string {
	return "DROP NOT NULL"
}

type PostgresAddDomainConstraintAction struct {
	ConstraintName string
	Definition     string
}

func (a *PostgresAddDomainConstraintAction) DomainActionClause() string {
	return fmt.Sprintf("ADD CONSTRAINT %s %s",
		driversshared.QuoteIdentifier(a.ConstraintName), a.Definition)
}

type PostgresDropDomainConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropDomainConstraintAction) DomainActionClause() string {
	return "DROP CONSTRAINT " + driversshared.QuoteIdentifier(a.ConstraintName)
}

type PostgresCommentOnTypeInstruction struct {
	Name string
	Text string
}

func (i *PostgresCommentOnTypeInstruction) String() string {
	return fmt.Sprintf("COMMENT ON TYPE %s IS %s;",
		driversshared.QuoteIdentifier(i.Name), commentLiteral(i.Text))
}

func (i *PostgresCommentOnTypeInstruction) Comment() string {
	return driversshared.ObjectComment("Modify", "type", i.Name)
}
