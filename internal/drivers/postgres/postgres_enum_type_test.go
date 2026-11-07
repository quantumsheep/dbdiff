package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresEnumType(t *testing.T) {
	t.Run("ModifyTypeComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TYPE mood AS ENUM ('sad', 'happy');`
		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`COMMENT ON TYPE mood IS 'the state of a person';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnTypeInstruction{
				Name: "mood",
				Text: "the state of a person",
			},
		})
	})

	t.Run("CreateEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad", "ok"},
			},
		})
	})

	t.Run("AddEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnDesired(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "happy",
			},
		})
	})

	t.Run("InsertEnumValueBeforeAndBetweenExistingValues", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE mood AS ENUM ('sad', 'happy');
			CREATE TABLE people (current_mood mood);
		`)
		driver.ExecOnDesired(`
			CREATE TYPE mood AS ENUM ('angry', 'sad', 'ok', 'fine', 'happy', 'joyful');
			CREATE TABLE people (current_mood mood);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTypeAddValueInstruction{
				Name:   "mood",
				Value:  "angry",
				Before: "sad",
			},
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "ok",
				After: "sad",
			},
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "fine",
				After: "ok",
			},
			&PostgresAlterTypeAddValueInstruction{
				Name:  "mood",
				Value: "joyful",
			},
		})
	})

	t.Run("RemoveEnumValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad', 'ok');`)
		driver.ExecOnDesired(`CREATE TYPE mood AS ENUM ('sad');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad"},
			},
		})
	})

	t.Run("ReorderAnEnumThatAColumnUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
			CREATE TABLE people (id INT, current_mood mood DEFAULT 'ok', past_moods mood[]);
			CREATE VIEW sad_people AS SELECT id FROM people WHERE current_mood = 'sad';
			INSERT INTO people VALUES (1, 'sad', '{sad,happy}'), (2, 'happy', NULL);
		`)
		driver.ExecOnDesired(`
			CREATE TYPE mood AS ENUM ('happy', 'sad');
			CREATE TABLE people (id INT, current_mood mood DEFAULT 'happy', past_moods mood[]);
			CREATE VIEW sad_people AS SELECT id FROM people WHERE current_mood = 'sad';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{
				Name: "sad_people",
			},
			&PostgresRenameTypeInstruction{
				ObjectType: "TYPE",
				Name:       "mood",
				NewName:    "dbdiff_old_mood",
			},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"happy", "sad"},
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "current_mood",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName:       "current_mood",
						DataType:         `"mood"`,
						UsingCast:        true,
						IntermediateType: "text",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName:       "past_moods",
						DataType:         `"mood"[]`,
						UsingCast:        true,
						IntermediateType: "text[]",
					},
				},
			},
			&PostgresDropTypeInstruction{
				Name: "dbdiff_old_mood",
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "current_mood",
						Expression: "'happy'::mood",
					},
				},
			},
			&PostgresCreateViewInstruction{
				Name:  "sad_people",
				Query: " SELECT id\n   FROM people\n  WHERE (current_mood = 'sad'::mood);",
			},
		})

		rows := driver.FetchAllFromCurrent("people", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "current_mood": "sad", "past_moods": "{sad,happy}"},
			{"id": int64(2), "current_mood": "happy", "past_moods": nil},
		}, rows)
	})

	t.Run("ReorderAnEnumThatAViewReturns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE people (id INT, current_mood mood);
			CREATE VIEW moods AS SELECT id, current_mood, 'ok'::mood AS fixed_mood FROM people;
		`
		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');` + schema)
		driver.ExecOnCurrent(`INSERT INTO people VALUES (1, 'sad'), (2, 'happy');`)
		driver.ExecOnDesired(`CREATE TYPE mood AS ENUM ('happy', 'ok', 'sad');` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{
				Name: "moods",
			},
			&PostgresRenameTypeInstruction{
				ObjectType: "TYPE",
				Name:       "mood",
				NewName:    "dbdiff_old_mood",
			},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"happy", "ok", "sad"},
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName:       "current_mood",
						DataType:         `"mood"`,
						UsingCast:        true,
						IntermediateType: "text",
					},
				},
			},
			&PostgresDropTypeInstruction{
				Name: "dbdiff_old_mood",
			},
			&PostgresCreateViewInstruction{
				Name:  "moods",
				Query: " SELECT id,\n    current_mood,\n    'ok'::mood AS fixed_mood\n   FROM people;",
			},
		})

		rows := driver.FetchAllFromCurrent("moods", "ORDER BY current_mood")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "current_mood": "happy", "fixed_mood": "ok"},
			{"id": int64(1), "current_mood": "sad", "fixed_mood": "ok"},
		}, rows)
	})

	t.Run("ReorderAnEnumThatAMaterializedViewReturns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE people (id INT, current_mood mood);
			CREATE MATERIALIZED VIEW moods AS SELECT * FROM people;
		`
		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad', 'happy');` + schema)
		driver.ExecOnCurrent(`INSERT INTO people VALUES (1, 'sad'), (2, 'happy');`)
		driver.ExecOnDesired(`CREATE TYPE mood AS ENUM ('happy', 'sad');` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropMaterializedViewInstruction{
				Name: "moods",
			},
			&PostgresRenameTypeInstruction{
				ObjectType: "TYPE",
				Name:       "mood",
				NewName:    "dbdiff_old_mood",
			},
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"happy", "sad"},
			},
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName:       "current_mood",
						DataType:         `"mood"`,
						UsingCast:        true,
						IntermediateType: "text",
					},
				},
			},
			&PostgresDropTypeInstruction{
				Name: "dbdiff_old_mood",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "moods",
				Query: " SELECT id,\n    current_mood\n   FROM people;",
			},
		})

		rows := driver.FetchAllFromCurrent("moods", "ORDER BY current_mood")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "current_mood": "happy"},
			{"id": int64(1), "current_mood": "sad"},
		}, rows)
	})

	t.Run("ReorderAnEnumThatAFunctionUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE mood AS ENUM ('sad', 'happy');
			CREATE FUNCTION is_sad(value mood) RETURNS boolean LANGUAGE sql AS 'SELECT value = ''sad''';
		`)
		driver.ExecOnDesired(`
			CREATE TYPE mood AS ENUM ('happy', 'sad');
			CREATE FUNCTION is_sad(value mood) RETURNS boolean LANGUAGE sql AS 'SELECT value = ''sad''';
		`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the function is_sad(mood) uses the type "mood". `+
			`dbdiff must drop and create this type again, and it cannot drop and create this object again`)
	})

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
		})
	})
}
