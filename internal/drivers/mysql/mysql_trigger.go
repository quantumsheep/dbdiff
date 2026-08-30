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
	return driversshared.DiffRules[*MySQLTrigger]{
		Key: func(trigger *MySQLTrigger) string {
			return trigger.Name
		},
		Create: func(trigger *MySQLTrigger) []driversshared.Instruction {
			return []driversshared.Instruction{trigger.CreateInstruction()}
		},
		Change: func(target *MySQLTrigger, source *MySQLTrigger) ([]driversshared.Instruction, error) {
			if target.Equal(source) {
				return nil, nil
			}

			return []driversshared.Instruction{
				&MySQLDropTriggerInstruction{
					Name: source.Name,
				},
				target.CreateInstruction(),
			}, nil
		},
		Drop: func(trigger *MySQLTrigger) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropTriggerInstruction{
				Name: trigger.Name,
			}}
		},
	}
}
