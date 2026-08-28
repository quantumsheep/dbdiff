package drivers

type SQLiteView struct {
	Name     string
	SQL      string
	Triggers []*SQLiteTrigger
}

func (v *SQLiteView) TriggerByName(name string) (*SQLiteTrigger, bool) {
	for _, trigger := range v.Triggers {
		if trigger.Name == name {
			return trigger, true
		}
	}

	return nil, false
}

func (v *SQLiteView) Instructions() []Instruction {
	instructions := []Instruction{&SQLiteCreateViewInstruction{Definition: v.SQL}}

	return append(instructions, v.TriggerInstructions()...)
}

func (v *SQLiteView) TriggerInstructions() []Instruction {
	var instructions []Instruction

	for _, trigger := range v.Triggers {
		instructions = append(instructions, &SQLiteCreateTriggerInstruction{
			Definition: trigger.SQL,
		})
	}

	return instructions
}

// A DROP VIEW statement removes every trigger of the view.
func (v *SQLiteView) Diff(other *SQLiteView) ([]Instruction, error) {
	if v.SQL != other.SQL {
		instructions := []Instruction{
			&SQLDropViewInstruction{Name: other.Name},
			&SQLiteCreateViewInstruction{Definition: v.SQL},
		}

		return append(instructions, v.TriggerInstructions()...), nil
	}

	return v.DiffTriggers(other), nil
}

func (v *SQLiteView) DiffTriggers(other *SQLiteView) []Instruction {
	var instructions []Instruction

	for _, targetTrigger := range v.Triggers {
		sourceTrigger, found := other.TriggerByName(targetTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteCreateTriggerInstruction{
				Definition: targetTrigger.SQL,
			})

			continue
		}

		if targetTrigger.SQL != sourceTrigger.SQL {
			instructions = append(instructions,
				&SQLiteDropTriggerInstruction{Name: sourceTrigger.Name},
				&SQLiteCreateTriggerInstruction{Definition: targetTrigger.SQL})
		}
	}

	for _, sourceTrigger := range other.Triggers {
		_, found := v.TriggerByName(sourceTrigger.Name)
		if !found {
			instructions = append(instructions, &SQLiteDropTriggerInstruction{
				Name: sourceTrigger.Name,
			})
		}
	}

	return instructions
}
