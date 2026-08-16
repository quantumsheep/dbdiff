package drivers

import (
	"database/sql"
	"fmt"
	"strings"
)

type PostgresSequence struct {
	Name      string
	DataType  string
	Start     int64
	Min       int64
	Max       int64
	Increment int64
	Cycle     bool

	// CurrentValue holds the last value of the sequence. The value is not valid before the
	// first call of nextval.
	CurrentValue sql.NullInt64
}

func (s *PostgresSequence) StringCycle() string {
	if s.Cycle {
		return "CYCLE"
	}

	return "NO CYCLE"
}

func (s *PostgresSequence) String() string {
	return fmt.Sprintf(
		"CREATE SEQUENCE %s AS %s INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d %s;",
		quoteIdentifier(s.Name),
		s.DataType,
		s.Increment,
		s.Min,
		s.Max,
		s.Start,
		s.StringCycle(),
	)
}

// Diff returns one ALTER SEQUENCE statement with every attribute that changes. Separate
// statements can fail, because a new minimum above the current value is invalid.
//
// PostgreSQL also refuses the new minimum or the new maximum alone when the current value
// of the target sequence falls outside the new range. Diff adds a RESTART WITH clause only
// in that case, because a restart changes the current value of the sequence.
func (s *PostgresSequence) Diff(other *PostgresSequence) string {
	var changes []string

	if s.DataType != other.DataType {
		changes = append(changes, "AS "+s.DataType)
	}

	if s.Increment != other.Increment {
		changes = append(changes, fmt.Sprintf("INCREMENT BY %d", s.Increment))
	}

	if s.Min != other.Min {
		changes = append(changes, fmt.Sprintf("MINVALUE %d", s.Min))
	}

	if s.Max != other.Max {
		changes = append(changes, fmt.Sprintf("MAXVALUE %d", s.Max))
	}

	if s.Start != other.Start {
		changes = append(changes, fmt.Sprintf("START WITH %d", s.Start))
	}

	if s.Cycle != other.Cycle {
		changes = append(changes, s.StringCycle())
	}

	if len(changes) == 0 {
		return ""
	}

	if other.CurrentValue.Valid {
		currentValue := other.CurrentValue.Int64

		if currentValue < s.Min || currentValue > s.Max {
			changes = append(changes, fmt.Sprintf("RESTART WITH %d", s.RestartValue()))
		}
	}

	return fmt.Sprintf("ALTER SEQUENCE %s %s;", quoteIdentifier(s.Name), strings.Join(changes, " "))
}

// RestartValue returns the value of a RESTART WITH clause. It returns the start value of
// the sequence when that value fits the new range. It clamps the value to the nearest
// bound otherwise.
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
