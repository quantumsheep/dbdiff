package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLEvent struct {
	Name string

	// Definition holds the whole CREATE statement, without the DEFINER clause.
	Definition string
}

func (e *MySQLEvent) CreateInstruction() *MySQLCreateEventInstruction {
	return &MySQLCreateEventInstruction{
		Name:       e.Name,
		Definition: e.Definition,
	}
}

func (d *MySQLDriver) DiffEvents(ctx context.Context) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, d.GetEvents,
		driversshared.ReplaceRules(
			func(event *MySQLEvent) string {
				return event.Name
			},
			func(desired *MySQLEvent, current *MySQLEvent) bool {
				return stripStartsClause(desired.Definition) == stripStartsClause(current.Definition)
			},
			func(event *MySQLEvent) driversshared.Instruction {
				return event.CreateInstruction()
			},
			func(event *MySQLEvent) driversshared.Instruction {
				return &MySQLDropEventInstruction{
					Name: event.Name,
				}
			},
		))
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (d *MySQLDriver) GetEvents(ctx context.Context, db *sql.DB) ([]*MySQLEvent, error) {
	names, err := driversshared.QueryAll(ctx, db, `
		SELECT EVENT_NAME
		FROM information_schema.EVENTS
		WHERE EVENT_SCHEMA = DATABASE()
		ORDER BY EVENT_NAME;
	`, func(rows *sql.Rows) (string, error) {
		var name string

		err := rows.Scan(&name)

		return name, err
	})
	if err != nil {
		return nil, err
	}

	var events []*MySQLEvent

	for _, name := range names {
		definition, err := d.GetEventDefinition(ctx, db, name)
		if err != nil {
			return nil, err
		}

		events = append(events, &MySQLEvent{
			Name:       name,
			Definition: definition,
		})
	}

	return events, nil
}

func (d *MySQLDriver) GetEventDefinition(ctx context.Context, db *sql.DB, name string) (string, error) {
	row := db.QueryRowContext(ctx, "SHOW CREATE EVENT "+QuoteIdentifier(name)+";")

	var eventName, sqlMode, timeZone, characterSet, collation, databaseCollation string
	var definition sql.NullString

	err := row.Scan(&eventName, &sqlMode, &timeZone, &definition, &characterSet, &collation, &databaseCollation)
	if err != nil {
		return "", err
	}

	if !definition.Valid {
		return "", fmt.Errorf("the event %q gives no definition. Connect as a user with the EVENT right", name)
	}

	return stripDefinerClause(definition.String, "EVENT"), nil
}

// MySQL writes the creation time as the STARTS clause of an event without one, so two
// databases give two different texts for one event. The comparison reads the text without
// the clause.
func stripStartsClause(definition string) string {
	const startsPrefix = " STARTS '"

	position := strings.Index(definition, startsPrefix)
	if position < 0 {
		return definition
	}

	rest := definition[position+len(startsPrefix):]

	end := strings.IndexByte(rest, '\'')
	if end < 0 {
		return definition
	}

	return definition[:position] + rest[end+1:]
}
