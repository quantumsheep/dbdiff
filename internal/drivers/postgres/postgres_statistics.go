package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// PostgreSQL holds no action that changes the columns of such an object, so a new
// definition prints a DROP statement and a CREATE statement.
type PostgresStatistics struct {
	Name string

	// Def holds the text of pg_get_statisticsobjdef, with no semicolon.
	Def string
}

func (s *PostgresStatistics) CreateInstruction() *PostgresCreateStatisticsInstruction {
	return &PostgresCreateStatisticsInstruction{Definition: s.Def}
}

func (s *PostgresStatistics) DropInstruction() *PostgresDropStatisticsInstruction {
	return &PostgresDropStatisticsInstruction{Name: s.Name}
}

// An extended statistics object names a table, so this section comes after the tables.
func (d *PostgresDriver) DiffStatistics(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresStatistics]{
		Get: d.GetStatistics,
		Key: func(statistics *PostgresStatistics) string {
			return statistics.Name
		},
		Create: func(statistics *PostgresStatistics) []driversshared.Instruction {
			return []driversshared.Instruction{statistics.CreateInstruction()}
		},
		Change: func(target *PostgresStatistics, source *PostgresStatistics) []driversshared.Instruction {
			if target.Def == source.Def {
				return nil
			}

			return []driversshared.Instruction{source.DropInstruction(), target.CreateInstruction()}
		},
		Drop: func(source *PostgresStatistics) driversshared.Instruction {
			return source.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetStatistics(ctx context.Context, db *sql.DB) ([]*PostgresStatistics, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			stxname,
			regexp_replace(
				pg_get_statisticsobjdef(oid),
				'\m' || quote_ident(current_schema()) || '\.',
				'',
				'g'
			)
		FROM pg_statistic_ext
		WHERE stxnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = pg_statistic_ext.oid AND d.deptype = 'e'
		)
		ORDER BY stxname
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var statistics []*PostgresStatistics

	for rows.Next() {
		object := &PostgresStatistics{}

		err := rows.Scan(&object.Name, &object.Def)
		if err != nil {
			return nil, err
		}

		statistics = append(statistics, object)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return statistics, nil
}
