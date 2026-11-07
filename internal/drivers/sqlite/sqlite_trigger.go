package driverssqlite

import (
	"context"
	"database/sql"

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

func (d *SQLiteDriver) GetTriggers(ctx context.Context, db *sql.DB, objectName string) ([]*SQLiteTrigger, error) {
	return driversshared.QueryAll(ctx, db, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE ORDER BY name", func(rows *sql.Rows) (*SQLiteTrigger, error) {
		trigger := &SQLiteTrigger{}

		err := rows.Scan(&trigger.Name, &trigger.SQL)

		return trigger, err
	}, objectName)
}
