package driversmysql

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLTrigger struct {
	Table string
	Name  string

	// Timing holds BEFORE or AFTER, and Event holds INSERT, UPDATE, or DELETE.
	Timing string
	Event  string

	Statement string
}

func (t *MySQLTrigger) Equal(other *MySQLTrigger) bool {
	return *t == *other
}

func (t *MySQLTrigger) CreateInstruction() *MySQLCreateTriggerInstruction {
	return &MySQLCreateTriggerInstruction{
		Name:      t.Name,
		Timing:    t.Timing,
		Event:     t.Event,
		TableName: t.Table,
		Statement: t.Statement,
	}
}

func diffTriggerRules() driversshared.DiffRules[*MySQLTrigger] {
	return driversshared.ReplaceRules(
		func(trigger *MySQLTrigger) string {
			return trigger.Name
		},
		(*MySQLTrigger).Equal,
		func(trigger *MySQLTrigger) driversshared.Instruction {
			return trigger.CreateInstruction()
		},
		func(trigger *MySQLTrigger) driversshared.Instruction {
			return &MySQLDropTriggerInstruction{
				Name: trigger.Name,
			}
		})
}
