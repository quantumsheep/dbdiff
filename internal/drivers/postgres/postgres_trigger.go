package driverspostgres

import (
	"context"
	"database/sql"

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

func getTableTriggers(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	// pg_get_triggerdef writes no mode, so the query reads tgenabled apart. The value O names
	// the mode of every new trigger.
	//
	// A partition takes each row trigger of its parent, and tgparentid names that parent
	// trigger. Without this filter a second CREATE TRIGGER statement of that trigger fails.
	triggerRows, err := db.QueryContext(ctx, `
			SELECT
				tgname,
				regexp_replace(
					pg_get_triggerdef(oid),
					' ON ' || quote_ident(current_schema()) || '\.',
					' ON '
				),
				CASE tgenabled
					WHEN 'O' THEN 'ENABLE'
					WHEN 'D' THEN 'DISABLE'
					WHEN 'R' THEN 'ENABLE REPLICA'
					WHEN 'A' THEN 'ENABLE ALWAYS'
					ELSE 'ENABLE'
				END
			FROM pg_trigger
			WHERE tgrelid = $1::regclass AND tgisinternal = false AND tgparentid = 0
			ORDER BY tgname
		`, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	defer func() { _ = triggerRows.Close() }()

	for triggerRows.Next() {
		trigger := &PostgresTrigger{}

		err := triggerRows.Scan(&trigger.Name, &trigger.Def, &trigger.EnableMode)
		if err != nil {
			return err
		}

		table.Triggers = append(table.Triggers, trigger)
	}

	return triggerRows.Err()
}
