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
			return trigger.Name
		},
		func(target *SQLiteTrigger, source *SQLiteTrigger) bool {
			return target.SQL == source.SQL
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
