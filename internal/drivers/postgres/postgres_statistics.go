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
func (d *PostgresDriver) DiffStatistics(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresStatistics]{
		Get: d.GetStatistics,
		Key: func(statistics *PostgresStatistics) string {
			return statistics.Name
		},
		Create: func(statistics *PostgresStatistics) []driversshared.Instruction {
			return []driversshared.Instruction{statistics.CreateInstruction()}
		},
		Change: func(desired *PostgresStatistics, current *PostgresStatistics) []driversshared.Instruction {
			if desired.Def == current.Def {
				return nil
			}

			return []driversshared.Instruction{current.DropInstruction(), desired.CreateInstruction()}
		},
		Drop: func(current *PostgresStatistics) driversshared.Instruction {
			return current.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetStatistics(ctx context.Context, db *sql.DB) ([]*PostgresStatistics, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			stxname,
			regexp_replace(
				pg_get_statisticsobjdef(oid),
				'^CREATE STATISTICS ' || `+currentSchemaPrefixPattern+`,
				'CREATE STATISTICS '
			)
		FROM pg_statistic_ext
		WHERE stxnamespace = quote_ident(current_schema())::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = pg_statistic_ext.oid AND d.deptype = 'e'
		)
		ORDER BY stxname
	`, func(rows *sql.Rows) (*PostgresStatistics, error) {
		object := &PostgresStatistics{}

		err := rows.Scan(&object.Name, &object.Def)

		return object, err
	})
}
