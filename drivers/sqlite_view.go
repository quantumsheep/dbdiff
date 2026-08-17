package drivers

type SQLiteView struct {
	Name string
	SQL  string
}

func (v *SQLiteView) Diff(other *SQLiteView) ([]Instruction, error) {
	if v.SQL == other.SQL {
		return nil, nil
	}

	instructions := []Instruction{
		&SQLDropViewInstruction{Name: other.Name},
		&SQLiteCreateViewInstruction{Definition: v.SQL},
	}

	return instructions, nil
}
