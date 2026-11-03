package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
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
func (s *PostgresSequence) Diff(other *PostgresSequence) []driversshared.Instruction {
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

	return []driversshared.Instruction{instruction}
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

func (d *PostgresDriver) DiffSequences(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresSequence]{
		Get: d.GetSequences,
		Key: func(sequence *PostgresSequence) string {
			return sequence.Name
		},
		Create: func(sequence *PostgresSequence) []driversshared.Instruction {
			return []driversshared.Instruction{sequence.CreateInstruction()}
		},
		Change: func(target *PostgresSequence, source *PostgresSequence) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresSequence) driversshared.Instruction {
			return &PostgresDropSequenceInstruction{Name: source.Name}
		},
	})
}

func (d *PostgresDriver) GetSequences(ctx context.Context, db *sql.DB) ([]*PostgresSequence, error) {
	// A serial column and an identity column own their sequence, and the table creates it.
	// s.last_value is NULL until the first call of nextval.
	return driversshared.QueryAll(ctx, db, `
		SELECT s.sequencename, s.data_type::text, s.start_value, s.min_value, s.max_value, s.increment_by, s.cache_size, s.cycle, s.last_value
		FROM pg_sequences s
		JOIN pg_class c ON c.relname = s.sequencename
			AND c.relkind = 'S'
			AND c.relnamespace = current_schema()::regnamespace
		WHERE s.schemaname = current_schema()
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype IN ('a', 'e', 'i')
		)
		ORDER BY s.sequencename
	`, func(rows *sql.Rows) (*PostgresSequence, error) {
		sequence := &PostgresSequence{}

		err := rows.Scan(
			&sequence.Name,
			&sequence.DataType,
			&sequence.Start,
			&sequence.Min,
			&sequence.Max,
			&sequence.Increment,
			&sequence.Cache,
			&sequence.Cycle,
			&sequence.CurrentValue,
		)

		return sequence, err
	})
}
