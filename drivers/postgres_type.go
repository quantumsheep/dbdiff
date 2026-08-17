package drivers

import (
	"slices"
)

type PostgresType struct {
	Name   string
	Values []string
}

func (t *PostgresType) CreateInstruction() *PostgresCreateEnumTypeInstruction {
	return &PostgresCreateEnumTypeInstruction{Name: t.Name, Values: t.Values}
}

func (t *PostgresType) StartsWith(other *PostgresType) bool {
	if len(other.Values) > len(t.Values) {
		return false
	}

	return slices.Equal(t.Values[:len(other.Values)], other.Values)
}

// PostgreSQL adds a value to an enum, but it removes none and it moves none.
func (t *PostgresType) Diff(other *PostgresType) []Instruction {
	if slices.Equal(t.Values, other.Values) {
		return nil
	}

	if t.StartsWith(other) {
		var instructions []Instruction

		for _, value := range t.Values[len(other.Values):] {
			instructions = append(instructions, &PostgresAlterTypeAddValueInstruction{
				Name:  t.Name,
				Value: value,
			})
		}

		return instructions
	}

	return []Instruction{
		&PostgresDropTypeInstruction{Name: t.Name},
		t.CreateInstruction(),
	}
}
