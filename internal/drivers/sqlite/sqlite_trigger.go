package driverssqlite

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteTrigger struct {
	Name string
	SQL  string
}

func diffTriggerRules() driversshared.DiffRules[*SQLiteTrigger] {
	return driversshared.DiffRules[*SQLiteTrigger]{
		Key: func(trigger *SQLiteTrigger) string {
			return trigger.Name
		},
		Create: func(trigger *SQLiteTrigger) []driversshared.Instruction {
			return []driversshared.Instruction{&SQLiteCreateTriggerInstruction{
				Definition: trigger.SQL,
			}}
		},
		Change: func(target *SQLiteTrigger, source *SQLiteTrigger) ([]driversshared.Instruction, error) {
			if target.SQL == source.SQL {
				return nil, nil
			}

			return []driversshared.Instruction{
				&SQLiteDropTriggerInstruction{
					Name: source.Name,
				},
				&SQLiteCreateTriggerInstruction{
					Definition: target.SQL,
				},
			}, nil
		},
		Drop: func(trigger *SQLiteTrigger) []driversshared.Instruction {
			return []driversshared.Instruction{&SQLiteDropTriggerInstruction{
				Name: trigger.Name,
			}}
		},
	}
}
