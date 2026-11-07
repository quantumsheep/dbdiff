package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
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

	OwnerTable  string
	OwnerColumn string

	// The word serial of the owner column creates and drops this sequence.
	Serial bool
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

// A sequence can change between a serial sequence and a sequence with its own options, and
// the key match then compares the options of the two.
func (d *PostgresDriver) DiffSequences(ctx context.Context) (*SectionDiff, error) {
	rules := driversshared.DiffRules[*PostgresSequence]{
		Key: func(sequence *PostgresSequence) string {
			return sequence.Name
		},
		Create: func(sequence *PostgresSequence) []driversshared.Instruction {
			if sequence.Serial {
				return nil
			}

			return []driversshared.Instruction{sequence.CreateInstruction()}
		},
		Change: func(desired *PostgresSequence, current *PostgresSequence) ([]driversshared.Instruction, error) {
			return desired.Diff(current), nil
		},
		Drop: func(current *PostgresSequence) []driversshared.Instruction {
			if current.Serial {
				return nil
			}

			return []driversshared.Instruction{&PostgresDropSequenceInstruction{Name: current.Name}}
		},
	}

	additions, removals, err := driversshared.DiffSides(
		ctx,
		d.DesiredDatabaseConnection,
		d.CurrentDatabaseConnection,
		d.GetSequences,
		rules,
	)
	if err != nil {
		return nil, err
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
}

// An OWNED BY clause names a column, so this section comes after the tables. A DROP TABLE
// statement and a DROP COLUMN statement also drop each sequence that the column owns, so the
// early removals release a sequence before its owner changes. The word serial creates and
// drops its sequence only when the other side holds no sequence of that name.
func (d *PostgresDriver) DiffSequenceOwners(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction

	desiredSequences, err := d.GetSequences(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentSequences, err := d.GetSequences(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	desiredSequenceByName := lo.KeyBy(desiredSequences, func(sequence *PostgresSequence) string {
		return sequence.Name
	})
	currentSequenceByName := lo.KeyBy(currentSequences, func(sequence *PostgresSequence) string {
		return sequence.Name
	})

	for _, currentSequence := range currentSequences {
		desiredSequence, found := desiredSequenceByName[currentSequence.Name]
		if currentSequence.OwnerColumn == "" || (currentSequence.Serial && !found) ||
			(found && desiredSequence.hasEqualOwner(currentSequence)) {
			continue
		}

		earlyRemovals = append(earlyRemovals, &PostgresAlterSequenceOwnerInstruction{
			Name: currentSequence.Name,
		})
	}

	for _, desiredSequence := range desiredSequences {
		currentSequence, found := currentSequenceByName[desiredSequence.Name]
		if desiredSequence.OwnerColumn == "" || (desiredSequence.Serial && !found) ||
			(found && desiredSequence.hasEqualOwner(currentSequence)) {
			continue
		}

		additions = append(additions, &PostgresAlterSequenceOwnerInstruction{
			Name:       desiredSequence.Name,
			TableName:  desiredSequence.OwnerTable,
			ColumnName: desiredSequence.OwnerColumn,
		})
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

func (s *PostgresSequence) hasEqualOwner(other *PostgresSequence) bool {
	return s.OwnerTable == other.OwnerTable && s.OwnerColumn == other.OwnerColumn
}

func (d *PostgresDriver) GetSequences(ctx context.Context, db *sql.DB) ([]*PostgresSequence, error) {
	// An identity column owns its sequence, and the table creates it.
	// s.last_value is NULL until the first call of nextval.
	return driversshared.QueryAll(ctx, db, `
		SELECT
			s.sequencename,
			s.data_type::text,
			s.start_value,
			s.min_value,
			s.max_value,
			s.increment_by,
			s.cache_size,
			s.cycle,
			s.last_value,
			coalesce(owner_table.relname, ''),
			coalesce(owner_column.attname, ''),
			coalesce(`+serialSequenceCondition+`, false)
		FROM pg_sequences s
		JOIN pg_class sequence_class ON sequence_class.relname = s.sequencename
			AND sequence_class.relkind = 'S'
			AND sequence_class.relnamespace = current_schema()::regnamespace
		JOIN pg_sequence sequence_options ON sequence_options.seqrelid = sequence_class.oid
		LEFT JOIN pg_depend owner ON owner.classid = 'pg_class'::regclass
			AND owner.objid = sequence_class.oid
			AND owner.refclassid = 'pg_class'::regclass
			AND owner.deptype = 'a'
		LEFT JOIN pg_class owner_table ON owner_table.oid = owner.refobjid
		LEFT JOIN pg_attribute owner_column ON owner_column.attrelid = owner.refobjid
			AND owner_column.attnum = owner.refobjsubid
		WHERE s.schemaname = current_schema()
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = sequence_class.oid AND d.deptype IN ('e', 'i')
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
			&sequence.OwnerTable,
			&sequence.OwnerColumn,
			&sequence.Serial,
		)

		return sequence, err
	})
}
