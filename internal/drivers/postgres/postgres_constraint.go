package driverspostgres

import (
	"fmt"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresConstraint struct {
	Name string
	Type string
	Def  string
}

func (c *PostgresConstraint) IsForeignKey() bool {
	return c.Type == "f"
}

func (c *PostgresConstraint) Clause() string {
	return fmt.Sprintf("CONSTRAINT %s %s", driversshared.QuoteIdentifier(c.Name), c.Def)
}
