package driverspostgres

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresIndex struct {
	Name    string
	Def     string
	Comment string
}

func (i *PostgresIndex) CreateInstruction() *PostgresCreateIndexInstruction {
	return &PostgresCreateIndexInstruction{Definition: i.Def}
}

// CREATE INDEX accepts no comment, so the comment takes its own statement.
func (i *PostgresIndex) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{i.CreateInstruction()}

	if i.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnIndexInstruction{
			Name: i.Name,
			Text: i.Comment,
		})
	}

	return instructions
}
