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
	targetEvents, err := d.GetEvents(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceEvents, err := d.GetEvents(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetEvents, sourceEvents, driversshared.DiffRules[*MySQLEvent]{
		Key: func(event *MySQLEvent) string {
			return event.Name
		},
		Create: func(event *MySQLEvent) []driversshared.Instruction {
			return []driversshared.Instruction{event.CreateInstruction()}
		},
		Change: func(target *MySQLEvent, source *MySQLEvent) ([]driversshared.Instruction, error) {
			if stripStartsClause(target.Definition) == stripStartsClause(source.Definition) {
				return nil, nil
			}

			return []driversshared.Instruction{
				&MySQLDropEventInstruction{
					Name: source.Name,
				},
				target.CreateInstruction(),
			}, nil
		},
		Drop: func(event *MySQLEvent) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropEventInstruction{
				Name: event.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (d *MySQLDriver) GetEvents(ctx context.Context, db *sql.DB) ([]*MySQLEvent, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT EVENT_NAME
		FROM information_schema.EVENTS
		WHERE EVENT_SCHEMA = DATABASE()
		ORDER BY EVENT_NAME;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var names []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}

		names = append(names, name)
	}

	err = rows.Err()
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
