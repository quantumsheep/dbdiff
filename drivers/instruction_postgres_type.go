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
		quoteIdentifier(i.Name), strings.Join(labels, ", "))
}

type PostgresAlterTypeAddValueInstruction struct {
	Name  string
	Value string
}

func (i *PostgresAlterTypeAddValueInstruction) String() string {
	return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s;",
		quoteIdentifier(i.Name), quoteLiteral(i.Value))
}

type PostgresDropTypeInstruction struct {
	Name string
}

func (i *PostgresDropTypeInstruction) String() string {
	return "DROP TYPE " + quoteIdentifier(i.Name) + ";"
}

type PostgresCreateCompositeTypeInstruction struct {
	Name       string
	Attributes []*PostgresCompositeTypeAttribute
}

func (i *PostgresCreateCompositeTypeInstruction) String() string {
	lines := lo.Map(i.Attributes, func(attribute *PostgresCompositeTypeAttribute, _ int) string {
		return fmt.Sprintf("\t%s %s", quoteIdentifier(attribute.Name), attribute.Type)
	})

	return fmt.Sprintf("CREATE TYPE %s AS (\n%s\n);",
		quoteIdentifier(i.Name), strings.Join(lines, ",\n"))
}

type PostgresCreateDomainInstruction struct {
	Name        string
	BaseType    string
	Default     sql.NullString
	NotNull     bool
	Constraints []*PostgresDomainConstraint
}

func (i *PostgresCreateDomainInstruction) String() string {
	statement := fmt.Sprintf("CREATE DOMAIN %s AS %s", quoteIdentifier(i.Name), i.BaseType)

	if i.Default.Valid {
		statement += " DEFAULT " + i.Default.String
	}

	if i.NotNull {
		statement += " NOT NULL"
	}

	clauses := lo.Map(i.Constraints, func(constraint *PostgresDomainConstraint, _ int) string {
		return fmt.Sprintf("CONSTRAINT %s %s",
			quoteIdentifier(constraint.Name), constraint.Def)
	})

	if len(clauses) > 0 {
		statement += " " + strings.Join(clauses, " ")
	}

	return statement + ";"
}

type PostgresDropDomainInstruction struct {
	Name string
}

func (i *PostgresDropDomainInstruction) String() string {
	return "DROP DOMAIN " + quoteIdentifier(i.Name) + ";"
}

type PostgresAlterDomainInstruction struct {
	Name   string
	Action AlterDomainAction
}

func (i *PostgresAlterDomainInstruction) String() string {
	return fmt.Sprintf("ALTER DOMAIN %s %s;",
		quoteIdentifier(i.Name), i.Action.DomainActionClause())
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
		quoteIdentifier(a.ConstraintName), a.Definition)
}

type PostgresDropDomainConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropDomainConstraintAction) DomainActionClause() string {
	return "DROP CONSTRAINT " + quoteIdentifier(a.ConstraintName)
}
