package drivers

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// CREATE TYPE name AS ENUM ( label [, ...] )
type PostgresCreateEnumTypeInstruction struct {
	Name   string
	Values []string
}

func (i *PostgresCreateEnumTypeInstruction) String() string {
	labels := lo.Map(i.Values, func(value string, _ int) string {
		return quoteLiteral(value)
	})

	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
		quoteIdentifier(i.Name), strings.Join(labels, ", "))
}

// ALTER TYPE name ADD VALUE new_enum_value
type PostgresAlterTypeAddValueInstruction struct {
	Name  string
	Value string
}

func (i *PostgresAlterTypeAddValueInstruction) String() string {
	return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s;",
		quoteIdentifier(i.Name), quoteLiteral(i.Value))
}

// DROP TYPE name
// The enum type and the composite type both use this instruction.
type PostgresDropTypeInstruction struct {
	Name string
}

func (i *PostgresDropTypeInstruction) String() string {
	return "DROP TYPE " + quoteIdentifier(i.Name) + ";"
}

// CREATE TYPE name AS ( attribute_name data_type [, ...] )
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

// CREATE DOMAIN name AS data_type [ DEFAULT expression ] [ NOT NULL ]
//
//	[ CONSTRAINT constraint_name definition ] [ ... ]
type PostgresCreateDomainInstruction struct {
	Name        string
	BaseType    string
	Default     sql.NullString
	NotNull     bool
	Constraints []*PostgresDomainConstraint
}

func (i *PostgresCreateDomainInstruction) String() string {
	var statement strings.Builder

	fmt.Fprintf(&statement, "CREATE DOMAIN %s AS %s", quoteIdentifier(i.Name), i.BaseType)

	if i.Default.Valid {
		fmt.Fprintf(&statement, " DEFAULT %s", i.Default.String)
	}

	if i.NotNull {
		fmt.Fprint(&statement, " NOT NULL")
	}

	for _, constraint := range i.Constraints {
		fmt.Fprintf(&statement, " CONSTRAINT %s %s",
			quoteIdentifier(constraint.Name), constraint.Def)
	}

	fmt.Fprint(&statement, ";")

	return statement.String()
}

// DROP DOMAIN name
type PostgresDropDomainInstruction struct {
	Name string
}

func (i *PostgresDropDomainInstruction) String() string {
	return "DROP DOMAIN " + quoteIdentifier(i.Name) + ";"
}

// ALTER DOMAIN name action
type PostgresAlterDomainInstruction struct {
	Name   string
	Action AlterDomainAction
}

func (i *PostgresAlterDomainInstruction) String() string {
	return fmt.Sprintf("ALTER DOMAIN %s %s;",
		quoteIdentifier(i.Name), i.Action.DomainActionClause())
}

// SET DEFAULT expression
type PostgresSetDomainDefaultAction struct {
	Expression string
}

func (a *PostgresSetDomainDefaultAction) DomainActionClause() string {
	return "SET DEFAULT " + a.Expression
}

// DROP DEFAULT
type PostgresDropDomainDefaultAction struct{}

func (a *PostgresDropDomainDefaultAction) DomainActionClause() string {
	return "DROP DEFAULT"
}

// SET NOT NULL
type PostgresSetDomainNotNullAction struct{}

func (a *PostgresSetDomainNotNullAction) DomainActionClause() string {
	return "SET NOT NULL"
}

// DROP NOT NULL
type PostgresDropDomainNotNullAction struct{}

func (a *PostgresDropDomainNotNullAction) DomainActionClause() string {
	return "DROP NOT NULL"
}

// ADD CONSTRAINT constraint_name definition
type PostgresAddDomainConstraintAction struct {
	ConstraintName string
	Definition     string
}

func (a *PostgresAddDomainConstraintAction) DomainActionClause() string {
	return fmt.Sprintf("ADD CONSTRAINT %s %s",
		quoteIdentifier(a.ConstraintName), a.Definition)
}

// DROP CONSTRAINT constraint_name
type PostgresDropDomainConstraintAction struct {
	ConstraintName string
}

func (a *PostgresDropDomainConstraintAction) DomainActionClause() string {
	return "DROP CONSTRAINT " + quoteIdentifier(a.ConstraintName)
}
