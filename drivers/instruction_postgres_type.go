package drivers

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/samber/lo"
)

type PostgresCreateEnumTypeInstruction struct {
	Name   string
	Labels []string
}

func (i *PostgresCreateEnumTypeInstruction) String() string {
	labels := lo.Map(i.Labels, func(label string, _ int) string {
		return quoteLiteral(label)
	})

	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
		QuoteIdentifier(i.Name), strings.Join(labels, ", "))
}

func (i *PostgresCreateEnumTypeInstruction) Comment() string {
	return objectComment("Create", "type", i.Name)
}

type PostgresAlterTypeAddValueInstruction struct {
	Name  string
	Value string
}

func (i *PostgresAlterTypeAddValueInstruction) String() string {
	return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s;",
		QuoteIdentifier(i.Name), quoteLiteral(i.Value))
}

func (i *PostgresAlterTypeAddValueInstruction) Comment() string {
	return objectComment("Modify", "type", i.Name)
}

type PostgresDropTypeInstruction struct {
	Name string
}

func (i *PostgresDropTypeInstruction) String() string {
	return "DROP TYPE " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropTypeInstruction) Comment() string {
	return objectComment("Drop", "type", i.Name)
}

type PostgresCreateCompositeTypeInstruction struct {
	Name       string
	Attributes []*PostgresCompositeTypeAttribute
}

func (i *PostgresCreateCompositeTypeInstruction) String() string {
	lines := lo.Map(i.Attributes, func(attribute *PostgresCompositeTypeAttribute, _ int) string {
		return fmt.Sprintf("\t%s %s", QuoteIdentifier(attribute.Name), attribute.Type)
	})

	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n);",
		QuoteIdentifier(i.Name), strings.Join(lines, ",\n"))
}

func (i *PostgresCreateCompositeTypeInstruction) Comment() string {
	return objectComment("Create", "type", i.Name)
}

type PostgresCreateDomainInstruction struct {
	Name        string
	BaseType    string
	Default     sql.NullString
	NotNull     bool
	Constraints []*PostgresDomainConstraint
}

func (i *PostgresCreateDomainInstruction) String() string {
	statement := fmt.Sprintf("CREATE DOMAIN %s AS %s", QuoteIdentifier(i.Name), i.BaseType)

	if i.Default.Valid {
		statement += " DEFAULT " + i.Default.String
	}

	if i.NotNull {
		statement += " NOT NULL"
	}

	clauses := lo.Map(i.Constraints, func(constraint *PostgresDomainConstraint, _ int) string {
		return fmt.Sprintf("CONSTRAINT %s %s",
			QuoteIdentifier(constraint.Name), constraint.Def)
	})

	if len(clauses) > 0 {
		statement += " " + strings.Join(clauses, " ")
	}

	return statement + ";"
}

func (i *PostgresCreateDomainInstruction) Comment() string {
	return objectComment("Create", "domain", i.Name)
}

type PostgresDropDomainInstruction struct {
	Name string
}

func (i *PostgresDropDomainInstruction) String() string {
	return "DROP DOMAIN " + QuoteIdentifier(i.Name) + ";"
}

func (i *PostgresDropDomainInstruction) Comment() string {
	return objectComment("Drop", "domain", i.Name)
}

type PostgresAlterDomainInstruction struct {
	Name   string
	Action AlterDomainAction
}

func (i *PostgresAlterDomainInstruction) String() string {
	return fmt.Sprintf("ALTER DOMAIN %s %s;",
		QuoteIdentifier(i.Name), i.Action.DomainActionClause())
}

func (i *PostgresAlterDomainInstruction) Comment() string {
	return objectComment("Modify", "domain", i.Name)
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
		QuoteIdentifier(a.ConstraintName), a.Definition)
}

type PostgresDropDomainConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropDomainConstraintAction) DomainActionClause() string {
	return "DROP CONSTRAINT " + QuoteIdentifier(a.ConstraintName)
}
