package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
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

	t.Run("DropEnumType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE mood AS ENUM ('sad');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "mood"},
		})
	})
}
