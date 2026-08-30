package driversmysql

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// MariaDB holds sequences, and MySQL holds none.
type MySQLSequence struct {
	Name      string
	Start     int64
	Minimum   int64
	Maximum   int64
	Increment int64
	Cache     int64
	Cycle     bool
}

func (s *MySQLSequence) Equal(other *MySQLSequence) bool {
	return *s == *other
}

func (s *MySQLSequence) CreateInstruction() *MySQLCreateSequenceInstruction {
	return &MySQLCreateSequenceInstruction{
		Name:      s.Name,
		Start:     s.Start,
		Minimum:   s.Minimum,
		Maximum:   s.Maximum,
		Increment: s.Increment,
		Cache:     s.Cache,
		Cycle:     s.Cycle,
	}
}

func (d *MySQLDriver) DiffSequences(ctx context.Context) ([]driversshared.Instruction, error) {
	targetSequences, err := d.GetSequences(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceSequences, err := d.GetSequences(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	additions, removals, err := driversshared.DiffByKey(targetSequences, sourceSequences, driversshared.DiffRules[*MySQLSequence]{
		Key: func(sequence *MySQLSequence) string {
			return sequence.Name
		},
		Create: func(sequence *MySQLSequence) []driversshared.Instruction {
			return []driversshared.Instruction{sequence.CreateInstruction()}
		},
		Change: func(target *MySQLSequence, source *MySQLSequence) ([]driversshared.Instruction, error) {
			if target.Equal(source) {
				return nil, nil
			}

			return []driversshared.Instruction{
				&MySQLDropSequenceInstruction{
					Name: source.Name,
				},
				target.CreateInstruction(),
			}, nil
		},
		Drop: func(sequence *MySQLSequence) []driversshared.Instruction {
			return []driversshared.Instruction{&MySQLDropSequenceInstruction{
				Name: sequence.Name,
			}}
		},
	})
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

func (d *MySQLDriver) GetSequences(ctx context.Context, db *sql.DB) ([]*MySQLSequence, error) {
	if !d.detailsByConnection[db].mariadb {
		return nil, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'SEQUENCE'
		ORDER BY TABLE_NAME;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var names []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}

		names = append(names, name)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	var sequences []*MySQLSequence

	for _, name := range names {
		sequence, err := d.GetSequence(ctx, db, name)
		if err != nil {
			return nil, err
		}

		sequences = append(sequences, sequence)
	}

	return sequences, nil
}

// The read skips next_not_cached_value and cycle_count, because those two columns hold
// the runtime state of the sequence.
func (d *MySQLDriver) GetSequence(ctx context.Context, db *sql.DB, name string) (*MySQLSequence, error) {
	row := db.QueryRowContext(ctx,
		"SELECT start_value, minimum_value, maximum_value, increment, cache_size, cycle_option FROM "+
			QuoteIdentifier(name)+";")

	sequence := &MySQLSequence{
		Name: name,
	}

	err := row.Scan(&sequence.Start, &sequence.Minimum, &sequence.Maximum,
		&sequence.Increment, &sequence.Cache, &sequence.Cycle)
	if err != nil {
		return nil, err
	}

	return sequence, nil
}
