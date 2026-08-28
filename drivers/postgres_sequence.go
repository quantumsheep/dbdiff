package drivers

import (
	"database/sql"
)

type PostgresSequence struct {
	Name      string
	DataType  string
	Start     int64
	Min       int64
	Max       int64
	Increment int64
	Cache     int64
	Cycle     bool

	CurrentValue sql.NullInt64
}

func (s *PostgresSequence) CreateInstruction() *PostgresCreateSequenceInstruction {
	return &PostgresCreateSequenceInstruction{
		Name:      s.Name,
		DataType:  s.DataType,
		Increment: s.Increment,
		Min:       s.Min,
		Max:       s.Max,
		Start:     s.Start,
		Cache:     s.Cache,
		Cycle:     s.Cycle,
	}
}

// Separate statements can fail, because a new minimum above the current value is invalid.
// A restart changes data, so the RESTART WITH clause comes only when the current value
// falls outside the new range.
func (s *PostgresSequence) Diff(other *PostgresSequence) []Instruction {
	instruction := &PostgresAlterSequenceInstruction{Name: s.Name}

	changed := false

	if s.DataType != other.DataType {
		instruction.DataType = sql.NullString{
			String: s.DataType,
			Valid:  true,
		}
		changed = true
	}

	if s.Increment != other.Increment {
		instruction.Increment = sql.NullInt64{
			Int64: s.Increment,
			Valid: true,
		}
		changed = true
	}

	if s.Min != other.Min {
		instruction.Min = sql.NullInt64{
			Int64: s.Min,
			Valid: true,
		}
		changed = true
	}

	if s.Max != other.Max {
		instruction.Max = sql.NullInt64{
			Int64: s.Max,
			Valid: true,
		}
		changed = true
	}

	if s.Start != other.Start {
		instruction.Start = sql.NullInt64{
			Int64: s.Start,
			Valid: true,
		}
		changed = true
	}

	if s.Cache != other.Cache {
		instruction.Cache = sql.NullInt64{
			Int64: s.Cache,
			Valid: true,
		}
		changed = true
	}

	if s.Cycle != other.Cycle {
		instruction.Cycle = sql.NullBool{
			Bool:  s.Cycle,
			Valid: true,
		}
		changed = true
	}

	if !changed {
		return nil
	}

	if other.CurrentValue.Valid {
		currentValue := other.CurrentValue.Int64
		if currentValue < s.Min || currentValue > s.Max {
			instruction.Restart = sql.NullInt64{
				Int64: s.RestartValue(),
				Valid: true,
			}
		}
	}

	return []Instruction{instruction}
}

func (s *PostgresSequence) RestartValue() int64 {
	value := s.Start
	if value < s.Min {
		value = s.Min
	}

	if value > s.Max {
		value = s.Max
	}

	return value
}
