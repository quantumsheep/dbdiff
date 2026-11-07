package driverssqlite

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteView struct {
	Name     string
	SQL      string
	Triggers []*SQLiteTrigger
}

func (v *SQLiteView) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{&SQLiteCreateViewInstruction{Definition: v.SQL}}

	return append(instructions, v.TriggerInstructions()...)
}

func (v *SQLiteView) TriggerInstructions() []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, trigger := range v.Triggers {
		instructions = append(instructions, &SQLiteCreateTriggerInstruction{
			Definition: trigger.SQL,
		})
	}

	return instructions
}

// A DROP VIEW statement removes every trigger of the view.
func (v *SQLiteView) Diff(other *SQLiteView) ([]driversshared.Instruction, error) {
	if v.SQL != other.SQL {
		instructions := []driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: other.Name},
			&SQLiteCreateViewInstruction{Definition: v.SQL},
		}

		return append(instructions, v.TriggerInstructions()...), nil
	}

	return v.DiffTriggers(other)
}

func (v *SQLiteView) DiffTriggers(other *SQLiteView) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffByKey(v.Triggers, other.Triggers, diffTriggerRules())
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (d *SQLiteDriver) GetViews(ctx context.Context, db *sql.DB) ([]*SQLiteView, error) {
	views, err := driversshared.QueryAll(ctx, db, "SELECT name, sql FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%' ORDER BY name", func(rows *sql.Rows) (*SQLiteView, error) {
		view := &SQLiteView{}

		err := rows.Scan(&view.Name, &view.SQL)

		return view, err
	})
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		view.Triggers, err = d.GetTriggers(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}
	}

	return views, nil
}
