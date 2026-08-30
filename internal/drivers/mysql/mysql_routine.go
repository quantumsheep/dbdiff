package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLRoutine struct {
	// Type holds PROCEDURE or FUNCTION.
	Type string
	Name string

	// Definition holds the whole CREATE statement, without the DEFINER clause.
	Definition string
}

func (r *MySQLRoutine) CreateInstruction() *MySQLCreateRoutineInstruction {
	return &MySQLCreateRoutineInstruction{
		Type:       r.Type,
		Name:       r.Name,
		Definition: r.Definition,
	}
}

func (d *MySQLDriver) DiffRoutines(ctx context.Context) ([]driversshared.Instruction, error) {
	targetRoutines, err := d.GetRoutines(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceRoutines, err := d.GetRoutines(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetRoutines, sourceRoutines, driversshared.DiffRules[*MySQLRoutine]{
		Key: func(routine *MySQLRoutine) string {
			return routine.Type + " " + routine.Name
		},
		Create: func(routine *MySQLRoutine) []driversshared.Instruction {
			return []driversshared.Instruction{routine.CreateInstruction()}
		},
		Change: func(target *MySQLRoutine, source *MySQLRoutine) ([]driversshared.Instruction, error) {
			if target.Definition == source.Definition {
				return nil, nil
			}

			return []driversshared.Instruction{
				&MySQLDropRoutineInstruction{
					Type: source.Type,
					Name: source.Name,
				},
				target.CreateInstruction(),
			}, nil
		},
		Drop: func(routine *MySQLRoutine) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropRoutineInstruction{
				Type: routine.Type,
				Name: routine.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (d *MySQLDriver) GetRoutines(ctx context.Context, db *sql.DB) ([]*MySQLRoutine, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT ROUTINE_TYPE, ROUTINE_NAME
		FROM information_schema.ROUTINES
		WHERE ROUTINE_SCHEMA = DATABASE()
		ORDER BY ROUTINE_TYPE, ROUTINE_NAME;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	type routineRow struct {
		Type string
		Name string
	}

	var routineRows []routineRow

	for rows.Next() {
		var row routineRow

		err := rows.Scan(&row.Type, &row.Name)
		if err != nil {
			return nil, err
		}

		routineRows = append(routineRows, row)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	var routines []*MySQLRoutine

	for _, row := range routineRows {
		definition, err := d.GetRoutineDefinition(ctx, db, row.Type, row.Name)
		if err != nil {
			return nil, err
		}

		routines = append(routines, &MySQLRoutine{
			Type:       row.Type,
			Name:       row.Name,
			Definition: definition,
		})
	}

	return routines, nil
}

// The ROUTINES catalog table gives the body without the parameter list, so the definition
// comes from SHOW CREATE.
func (d *MySQLDriver) GetRoutineDefinition(ctx context.Context, db *sql.DB, routineType string, name string) (string, error) {
	row := db.QueryRowContext(ctx,
		fmt.Sprintf("SHOW CREATE %s %s;", routineType, QuoteIdentifier(name)))

	var routineName, sqlMode, characterSet, collation, databaseCollation string
	var definition sql.NullString

	err := row.Scan(&routineName, &sqlMode, &definition, &characterSet, &collation, &databaseCollation)
	if err != nil {
		return "", err
	}

	// The text is NULL when the connection misses the right on the routine.
	if !definition.Valid {
		return "", fmt.Errorf("the %s %q gives no definition. Connect as a user with a right on it",
			strings.ToLower(routineType), name)
	}

	return stripDefinerClause(definition.String, routineType), nil
}

// The DEFINER holds the user of the creation, and that user differs between two servers.
// The server keeps the terminator of a statement that arrives in a batch, so the removal
// of the final semicolon keeps the two texts comparable.
func stripDefinerClause(definition string, keyword string) string {
	definition = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(definition), ";"))

	if !strings.HasPrefix(definition, "CREATE DEFINER=") {
		return definition
	}

	position := strings.Index(definition, " "+keyword+" ")
	if position < 0 {
		return definition
	}

	return "CREATE" + definition[position:]
}
