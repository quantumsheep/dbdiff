package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresDomainConstraint struct {
	Name string
	Def  string
}

type PostgresDomain struct {
	Name        string
	BaseType    string
	NotNull     bool
	Default     sql.NullString
	Constraints []*PostgresDomainConstraint
}

func (d *PostgresDomain) ConstraintByName(name string) (*PostgresDomainConstraint, bool) {
	for _, constraint := range d.Constraints {
		if constraint.Name == name {
			return constraint, true
		}
	}

	return nil, false
}

func (d *PostgresDomain) String() string {
	var statement strings.Builder

	fmt.Fprintf(&statement, "CREATE DOMAIN %s AS %s", quoteIdentifier(d.Name), d.BaseType)

	if d.Default.Valid {
		fmt.Fprintf(&statement, " DEFAULT %s", d.Default.String)
	}

	if d.NotNull {
		fmt.Fprint(&statement, " NOT NULL")
	}

	for _, constraint := range d.Constraints {
		fmt.Fprintf(&statement, " CONSTRAINT %s %s", quoteIdentifier(constraint.Name), constraint.Def)
	}

	fmt.Fprint(&statement, ";")

	return statement.String()
}

func (d *PostgresDomain) StringDrop() string {
	return fmt.Sprintf("DROP DOMAIN %s;", quoteIdentifier(d.Name))
}

// PostgreSQL changes no base type of a domain, so a new base type needs a recreation.
func (d *PostgresDomain) Diff(other *PostgresDomain) string {
	var diff strings.Builder

	if d.BaseType != other.BaseType {
		fmt.Fprintf(&diff, "%s\n", other.StringDrop())
		fmt.Fprintf(&diff, "%s\n", d.String())

		return strings.TrimSpace(diff.String())
	}

	if d.Default != other.Default {
		if d.Default.Valid {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s SET DEFAULT %s;\n", quoteIdentifier(d.Name), d.Default.String)
		} else {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s DROP DEFAULT;\n", quoteIdentifier(d.Name))
		}
	}

	if d.NotNull != other.NotNull {
		if d.NotNull {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s SET NOT NULL;\n", quoteIdentifier(d.Name))
		} else {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s DROP NOT NULL;\n", quoteIdentifier(d.Name))
		}
	}

	for _, sourceConstraint := range d.Constraints {
		targetConstraint, found := other.ConstraintByName(sourceConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s ADD CONSTRAINT %s %s;\n", quoteIdentifier(d.Name), quoteIdentifier(sourceConstraint.Name), sourceConstraint.Def)
			continue
		}

		if sourceConstraint.Def != targetConstraint.Def {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s DROP CONSTRAINT %s;\n", quoteIdentifier(d.Name), quoteIdentifier(targetConstraint.Name))
			fmt.Fprintf(&diff, "ALTER DOMAIN %s ADD CONSTRAINT %s %s;\n", quoteIdentifier(d.Name), quoteIdentifier(sourceConstraint.Name), sourceConstraint.Def)
		}
	}

	for _, targetConstraint := range other.Constraints {
		_, found := d.ConstraintByName(targetConstraint.Name)
		if !found {
			fmt.Fprintf(&diff, "ALTER DOMAIN %s DROP CONSTRAINT %s;\n", quoteIdentifier(d.Name), quoteIdentifier(targetConstraint.Name))
		}
	}

	return strings.TrimSpace(diff.String())
}
