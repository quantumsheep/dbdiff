package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// A materialized view reads a table, a view, or a second materialized view, so the diff
// keeps the order of the dependency.
type PostgresMaterializedView struct {
	Name            string
	Def             string
	Columns         []*PostgresViewColumn
	DependencyNames []string
	Indexes         []*PostgresIndex
	Comment         string
}

func (v *PostgresMaterializedView) HasEqualColumns(other *PostgresMaterializedView) bool {
	if len(v.Columns) != len(other.Columns) {
		return false
	}

	for index, column := range v.Columns {
		if *column != *other.Columns[index] {
			return false
		}
	}

	return true
}

func (v *PostgresMaterializedView) CreateInstruction() *PostgresCreateMaterializedViewInstruction {
	return &PostgresCreateMaterializedViewInstruction{
		Name:  v.Name,
		Query: v.Def,
	}
}

func (v *PostgresMaterializedView) DropInstruction() *PostgresDropMaterializedViewInstruction {
	return &PostgresDropMaterializedViewInstruction{Name: v.Name}
}

func (v *PostgresMaterializedView) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{v.CreateInstruction()}

	// CREATE MATERIALIZED VIEW accepts no comment, so the comment takes its own statement.
	if v.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnMaterializedViewInstruction{
			Name: v.Name,
			Text: v.Comment,
		})
	}

	for _, index := range v.Indexes {
		instructions = append(instructions, index.Instructions()...)
	}

	return instructions
}

func (v *PostgresMaterializedView) IndexByName(name string) (*PostgresIndex, bool) {
	for _, index := range v.Indexes {
		if index.Name == name {
			return index, true
		}
	}

	return nil, false
}

func (d *PostgresDriver) GetMaterializedViews(ctx context.Context, db *sql.DB) ([]*PostgresMaterializedView, error) {
	// Every query skips the objects that an extension owns (deptype 'e') and ends with a
	// stable ORDER BY, because PostgreSQL gives no stable row order.
	views, err := driversshared.QueryAll(ctx, db, `
		SELECT m.matviewname, m.definition,
			coalesce(obj_description(c.oid, 'pg_class'), '')
		FROM pg_matviews m
		JOIN pg_class c ON c.relname = m.matviewname
			AND c.relnamespace = current_schema()::regnamespace
		WHERE m.schemaname = current_schema()
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		ORDER BY m.matviewname
	`, func(rows *sql.Rows) (*PostgresMaterializedView, error) {
		view := &PostgresMaterializedView{}

		err := rows.Scan(&view.Name, &view.Def, &view.Comment)

		return view, err
	})
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		columns, err := d.GetViewColumns(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}

		view.Columns = columns

		dependencyNames, err := d.GetViewDependencyNames(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}

		view.DependencyNames = dependencyNames

		indexes, err := d.GetMaterializedViewIndexes(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}

		view.Indexes = indexes
	}

	return views, nil
}

func (d *PostgresDriver) GetMaterializedViewIndexes(ctx context.Context, db *sql.DB, viewName string) ([]*PostgresIndex, error) {
	// Every query that reads a definition text removes the current_schema() prefix. Without
	// this step the diff drops and recreates an object that did not change.
	return driversshared.QueryAll(ctx, db, `
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
		ORDER BY indexname
	`, func(rows *sql.Rows) (*PostgresIndex, error) {
		index := &PostgresIndex{}

		err := rows.Scan(&index.Name, &index.Def, &index.Comment)

		return index, err
	}, viewName)
}

func diffMaterializedViewIndexes(targetView *PostgresMaterializedView, sourceView *PostgresMaterializedView) []driversshared.Instruction {
	var instructions []driversshared.Instruction

	for _, targetIndex := range targetView.Indexes {
		sourceIndex, found := sourceView.IndexByName(targetIndex.Name)
		if !found {
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Def != sourceIndex.Def {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
			instructions = append(instructions, targetIndex.Instructions()...)
			continue
		}

		if targetIndex.Comment != sourceIndex.Comment {
			instructions = append(instructions, &PostgresCommentOnIndexInstruction{
				Name: targetIndex.Name,
				Text: targetIndex.Comment,
			})
		}
	}

	for _, sourceIndex := range sourceView.Indexes {
		_, found := targetView.IndexByName(sourceIndex.Name)
		if !found {
			instructions = append(instructions, &driversshared.SQLDropIndexInstruction{Name: sourceIndex.Name})
		}
	}

	return instructions
}
