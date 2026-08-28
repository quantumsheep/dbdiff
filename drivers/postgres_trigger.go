package drivers

type PostgresTrigger struct {
	Name       string
	Def        string
	EnableMode string
}

func (t *PostgresTrigger) CreateInstruction() *PostgresCreateTriggerInstruction {
	return &PostgresCreateTriggerInstruction{Definition: t.Def}
}

// A CREATE TRIGGER statement accepts no mode, and PostgreSQL builds every trigger with
// ENABLE, so that mode needs no statement.
func (t *PostgresTrigger) EnableInstructions(tableName string) []Instruction {
	if t.EnableMode == "" || t.EnableMode == "ENABLE" {
		return nil
	}

	return []Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []AlterTableAction{&PostgresTriggerEnableAction{
			Mode:        t.EnableMode,
			TriggerName: t.Name,
		}},
	}}
}
