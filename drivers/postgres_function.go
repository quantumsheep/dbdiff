package drivers

import "fmt"

type PostgresFunction struct {
	Name      string
	Arguments string
	Def       string
}

// Signature identifies the function. PostgreSQL accepts several functions with one name
// and different arguments.
func (f *PostgresFunction) Signature() string {
	return fmt.Sprintf("%s(%s)", f.Name, f.Arguments)
}

func (f *PostgresFunction) String() string {
	return f.Def + ";"
}

func (f *PostgresFunction) StringDrop() string {
	return fmt.Sprintf("DROP FUNCTION %s(%s);", quoteIdentifier(f.Name), f.Arguments)
}
