package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresIndex struct {
	Name    string
	Def     string
	Comment string
}

func (i *PostgresIndex) CreateInstruction() *PostgresCreateIndexInstruction {
	return &PostgresCreateIndexInstruction{Definition: i.Def}
}

// CREATE INDEX accepts no comment, so the comment takes its own statement.
func (i *PostgresIndex) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{i.CreateInstruction()}

	if i.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnIndexInstruction{
			Name: i.Name,
			Text: i.Comment,
		})
	}

	return instructions
}

func getTableIndexes(ctx context.Context, db *sql.DB, table *PostgresTable) error {
	// The query removes the keyword ONLY. PostgreSQL writes it for the index of a partitioned
	// table, and a statement that holds it builds no index on the partitions.
	indexRows, err := db.QueryContext(ctx, `
			SELECT
				indexname,
				regexp_replace(
					indexdef,
					' ON (ONLY )?' || quote_ident(current_schema()) || '\.',
					' ON '
				),
				coalesce((
					SELECT obj_description(c.oid, 'pg_class')
					FROM pg_class c
					WHERE c.relnamespace = current_schema()::regnamespace
					AND c.relname = pg_indexes.indexname
				), '')
			FROM pg_indexes
			WHERE schemaname = current_schema() AND tablename = $1
			AND indexname NOT IN (
				SELECT conname FROM pg_constraint WHERE conrelid = $2::regclass
			)
			AND NOT EXISTS (
				SELECT 1
				FROM pg_inherits i
				JOIN pg_class child ON child.oid = i.inhrelid
				WHERE child.relname = pg_indexes.indexname
				AND child.relnamespace = current_schema()::regnamespace
			)
			ORDER BY indexname
		`, table.Name, driversshared.QuoteIdentifier(table.Name))
	if err != nil {
		return err
	}

	defer func() { _ = indexRows.Close() }()

	for indexRows.Next() {
		index := &PostgresIndex{}

		err := indexRows.Scan(&index.Name, &index.Def, &index.Comment)
		if err != nil {
			return err
		}

		table.Indexes = append(table.Indexes, index)
	}

	return indexRows.Err()
}
