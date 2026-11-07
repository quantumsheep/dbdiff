package driversmysql

import (
	"context"
	"database/sql"

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

// The order of ACTION_ORDER keeps two triggers of one timing and one event in their
// activation order, so the creation builds that order again without a FOLLOWS clause.
func (d *MySQLDriver) GetTableTriggers(ctx context.Context, db *sql.DB, tableName string) ([]*MySQLTrigger, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
		ORDER BY ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORDER;
	`, func(rows *sql.Rows) (*MySQLTrigger, error) {
		trigger := &MySQLTrigger{
			Table: tableName,
		}

		err := rows.Scan(&trigger.Name, &trigger.Timing, &trigger.Event, &trigger.Statement)

		return trigger, err
	}, tableName)
}
