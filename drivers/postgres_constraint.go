package drivers

import "fmt"

type PostgresConstraint struct {
	Name string
	Type string
	Def  string
}

func (c *PostgresConstraint) Clause() string {
	return fmt.Sprintf("CONSTRAINT %s %s", quoteIdentifier(c.Name), c.Def)
}
