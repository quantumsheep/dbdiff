package drivers

type PostgresTrigger struct {
	Name string
	Def  string
}

func (t *PostgresTrigger) CreateInstruction() *PostgresCreateTriggerInstruction {
	return &PostgresCreateTriggerInstruction{Definition: t.Def}
}
