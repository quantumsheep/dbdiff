package drivers

import "fmt"

type PostgresExtension struct {
	Name    string
	Version string
}

func (e *PostgresExtension) String() string {
	return fmt.Sprintf("CREATE EXTENSION %s;", quoteIdentifier(e.Name))
}

func (e *PostgresExtension) StringUpdate() string {
	return fmt.Sprintf("ALTER EXTENSION %s UPDATE TO %s;", quoteIdentifier(e.Name), quoteLiteral(e.Version))
}

func (e *PostgresExtension) StringDrop() string {
	return fmt.Sprintf("DROP EXTENSION %s;", quoteIdentifier(e.Name))
}
