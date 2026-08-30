package driverspostgres

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

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
func (t *PostgresTrigger) EnableInstructions(tableName string) []driversshared.Instruction {
	if t.EnableMode == "" || t.EnableMode == "ENABLE" {
		return nil
	}

	return []driversshared.Instruction{&PostgresAlterTableInstruction{
		Name: tableName,
		Actions: []driversshared.AlterTableAction{&PostgresTriggerEnableAction{
			Mode:        t.EnableMode,
			TriggerName: t.Name,
		}},
	}}
}
