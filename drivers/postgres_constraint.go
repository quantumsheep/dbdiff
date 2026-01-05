package drivers

import "fmt"

type PostgresConstraint struct {
	Name string
	Type string // p (primary), u (unique), c (check), f (foreign)
	Def  string
}

func (c *PostgresConstraint) String() string {
	return fmt.Sprintf("CONSTRAINT \"%s\" %s", c.Name, c.Def)
}
