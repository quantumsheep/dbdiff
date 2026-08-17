package drivers

import "fmt"

type PostgresFunction struct {
	Name       string
	Arguments  string
	ReturnType string
	Def        string
}

func (f *PostgresFunction) Signature() string {
	return fmt.Sprintf("%s(%s)", f.Name, f.Arguments)
}

func (f *PostgresFunction) CreateInstruction() *PostgresCreateFunctionInstruction {
	return &PostgresCreateFunctionInstruction{Definition: f.Def}
}

func (f *PostgresFunction) DropInstruction() *PostgresDropFunctionInstruction {
	return &PostgresDropFunctionInstruction{
		Name:      f.Name,
		Arguments: f.Arguments,
	}
}
