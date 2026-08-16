package drivers

import (
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
// statements can fail, because a new minimum below the current value is invalid.
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

	return fmt.Sprintf("ALTER SEQUENCE %s %s;", quoteIdentifier(s.Name), strings.Join(changes, " "))
}
