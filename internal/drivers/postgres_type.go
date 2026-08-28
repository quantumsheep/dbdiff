package drivers

import (
	"slices"
)

type PostgresType struct {
	Name    string
	Values  []string
	Comment string
}

func (t *PostgresType) CreateInstruction() *PostgresCreateEnumTypeInstruction {
	return &PostgresCreateEnumTypeInstruction{
		Name:   t.Name,
		Labels: t.Values,
	}
}

// CREATE TYPE accepts no comment, so the comment takes its own statement.
func (t *PostgresType) Instructions() []Instruction {
	instructions := []Instruction{t.CreateInstruction()}

	if t.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}

func (t *PostgresType) StartsWith(other *PostgresType) bool {
	if len(other.Values) > len(t.Values) {
		return false
	}

	return slices.Equal(t.Values[:len(other.Values)], other.Values)
}

// PostgreSQL adds a value to an enum, but it removes none and it moves none.
func (t *PostgresType) Diff(other *PostgresType) []Instruction {
	if !slices.Equal(t.Values, other.Values) && !t.StartsWith(other) {
		instructions := []Instruction{&PostgresDropTypeInstruction{Name: t.Name}}

		return append(instructions, t.Instructions()...)
	}

	var instructions []Instruction

	for _, value := range t.Values[len(other.Values):] {
		instructions = append(instructions, &PostgresAlterTypeAddValueInstruction{
			Name:  t.Name,
			Value: value,
		})
	}

	if t.Comment != other.Comment {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}
