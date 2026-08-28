package drivers

type PostgresIndex struct {
	Name    string
	Def     string
	Comment string
}

func (i *PostgresIndex) CreateInstruction() *PostgresCreateIndexInstruction {
	return &PostgresCreateIndexInstruction{Definition: i.Def}
}

// CREATE INDEX accepts no comment, so the comment takes its own statement.
func (i *PostgresIndex) Instructions() []Instruction {
	instructions := []Instruction{i.CreateInstruction()}

	if i.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnIndexInstruction{
			Name: i.Name,
			Text: i.Comment,
		})
	}

	return instructions
}
