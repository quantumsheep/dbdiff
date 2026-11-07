package driverssqlite

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteTrigger struct {
	Name string
	SQL  string
}

func diffTriggerRules() driversshared.DiffRules[*SQLiteTrigger] {
	return driversshared.ReplaceRules(
		func(trigger *SQLiteTrigger) string {
			return identifierKey(trigger.Name)
		},
		func(desired *SQLiteTrigger, current *SQLiteTrigger) bool {
			return desired.SQL == current.SQL
		},
		func(trigger *SQLiteTrigger) driversshared.Instruction {
			return &SQLiteCreateTriggerInstruction{
				Definition: trigger.SQL,
			}
		},
		func(trigger *SQLiteTrigger) driversshared.Instruction {
			return &SQLiteDropTriggerInstruction{
				Name: trigger.Name,
			}
		})
}
