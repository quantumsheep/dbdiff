package drivers

type PostgresIndex struct {
	Name string
	Def  string
}

func (i *PostgresIndex) CreateInstruction() *PostgresCreateIndexInstruction {
	return &PostgresCreateIndexInstruction{Definition: i.Def}
}
