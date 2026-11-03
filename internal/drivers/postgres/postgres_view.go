package driverspostgres

import (
	"context"
	"database/sql"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// A type change of a column keeps the definition text of the view equal, so the diff
// compares the columns too.
type PostgresViewColumn struct {
	Table  string
	Column string
	Type   string
}

type PostgresView struct {
	Name    string
	Def     string
	Columns []*PostgresViewColumn

	// The query text of the view holds no check option, so the diff compares this field too.
	CheckOption string

	Comment string
}

func (v *PostgresView) HasEqualColumns(other *PostgresView) bool {
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

func (v *PostgresView) CreateInstruction() *PostgresCreateViewInstruction {
	return &PostgresCreateViewInstruction{
		Name:        v.Name,
		Query:       v.Def,
		CheckOption: v.CheckOption,
	}
}

// CREATE VIEW accepts no comment, so the comment takes its own statement.
func (v *PostgresView) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{v.CreateInstruction()}

	if v.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnViewInstruction{
			Name: v.Name,
			Text: v.Comment,
		})
	}

	return instructions
}

// A view can read a materialized view, and a materialized view can read a view, so one
// section order per kind cannot hold the dependency. A relation joins the two kinds into
// one dependency order.
type PostgresRelation struct {
	View             *PostgresView
	MaterializedView *PostgresMaterializedView
}

func (r *PostgresRelation) Name() string {
	if r.View != nil {
		return r.View.Name
	}

	return r.MaterializedView.Name
}

func (r *PostgresRelation) Columns() []*PostgresViewColumn {
	if r.View != nil {
		return r.View.Columns
	}

	return r.MaterializedView.Columns
}

// Two independent relations keep the order that the queries give.
func sortRelationsByDependency(views []*PostgresView, materializedViews []*PostgresMaterializedView) []*PostgresRelation {
	relations := make([]*PostgresRelation, 0, len(views)+len(materializedViews))

	for _, view := range views {
		relations = append(relations, &PostgresRelation{
			View: view,
		})
	}

	for _, view := range materializedViews {
		relations = append(relations, &PostgresRelation{
			MaterializedView: view,
		})
	}

	relationByName := make(map[string]*PostgresRelation, len(relations))

	for _, relation := range relations {
		relationByName[relation.Name()] = relation
	}

	sorted := make([]*PostgresRelation, 0, len(relations))
	visited := make(map[string]bool, len(relations))

	var visit func(relation *PostgresRelation)

	visit = func(relation *PostgresRelation) {
		if visited[relation.Name()] {
			return
		}

		visited[relation.Name()] = true

		for _, column := range relation.Columns() {
			dependency, isRelation := relationByName[column.Table]
			if isRelation {
				visit(dependency)
			}
		}

		sorted = append(sorted, relation)
	}

	for _, relation := range relations {
		visit(relation)
	}

	return sorted
}

func (d *PostgresDriver) GetViews(ctx context.Context, db *sql.DB) ([]*PostgresView, error) {
	// information_schema.views gives no definition of a view that the current role does not
	// own, and it hides a view without a privilege, so the query reads pg_class.
	views, err := driversshared.QueryAll(ctx, db, `
		SELECT c.relname, pg_get_viewdef(c.oid),
			coalesce((
				SELECT upper(substring(option FROM '^check_option=(.*)$'))
				FROM unnest(c.reloptions) AS option
				WHERE option LIKE 'check_option=%'
			), ''),
			coalesce(obj_description(c.oid, 'pg_class'), '')
		FROM pg_class c
		WHERE c.relkind = 'v'
		AND c.relnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype = 'e'
		)
		ORDER BY c.relname
	`, func(rows *sql.Rows) (*PostgresView, error) {
		view := &PostgresView{}

		err := rows.Scan(&view.Name, &view.Def, &view.CheckOption, &view.Comment)

		return view, err
	})
	if err != nil {
		return nil, err
	}

	for _, view := range views {
		view.Columns, err = d.GetViewColumns(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}
	}

	return sortViewsByDependency(views), nil
}

// The type of each column comes with it, because a type change makes the view invalid.
func (d *PostgresDriver) GetViewColumns(ctx context.Context, db *sql.DB, viewName string) ([]*PostgresViewColumn, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT DISTINCT
			source_class.relname,
			source_attribute.attname,
			format_type(source_attribute.atttypid, source_attribute.atttypmod)
		FROM pg_depend d
		JOIN pg_rewrite view_rule ON view_rule.oid = d.objid
		JOIN pg_class view_class ON view_class.oid = view_rule.ev_class
		JOIN pg_class source_class ON source_class.oid = d.refobjid
		JOIN pg_attribute source_attribute
			ON source_attribute.attrelid = d.refobjid
			AND source_attribute.attnum = d.refobjsubid
		WHERE d.classid = 'pg_rewrite'::regclass
		AND d.refclassid = 'pg_class'::regclass
		AND d.refobjsubid > 0
		AND view_class.relnamespace = current_schema()::regnamespace
		AND view_class.relname = $1
		AND view_class.oid <> d.refobjid
		ORDER BY source_class.relname, source_attribute.attname
	`, func(rows *sql.Rows) (*PostgresViewColumn, error) {
		column := &PostgresViewColumn{}

		err := rows.Scan(&column.Table, &column.Column, &column.Type)

		return column, err
	}, viewName)
}

// A view can read a materialized view, and a materialized view can read a view, so the
// two kinds share one section and one dependency order.
func (d *PostgresDriver) DiffViewsAndMaterializedViews(ctx context.Context) (*driversshared.SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction

	targetViews, err := d.GetViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceViews, err := d.GetViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetMaterializedViews, err := d.GetMaterializedViews(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceMaterializedViews, err := d.GetMaterializedViews(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	targetRelations := sortRelationsByDependency(targetViews, targetMaterializedViews)
	sourceRelations := sortRelationsByDependency(sourceViews, sourceMaterializedViews)

	for _, relation := range targetRelations {
		if relation.View != nil {
			targetView := relation.View

			sourceView, found := lo.Find(sourceViews, func(view *PostgresView) bool {
				return view.Name == targetView.Name
			})
			if !found {
				additions = append(additions, targetView.Instructions()...)
				continue
			}

			// A type change of a column that the view reads keeps the definition text equal.
			if targetView.Def != sourceView.Def || targetView.CheckOption != sourceView.CheckOption ||
				!targetView.HasEqualColumns(sourceView) {
				d.markRecreated(targetView.Name)
				additions = append(additions, targetView.Instructions()...)
				continue
			}

			if targetView.Comment != sourceView.Comment {
				additions = append(additions, &PostgresCommentOnViewInstruction{
					Name: targetView.Name,
					Text: targetView.Comment,
				})
			}

			continue
		}

		targetView := relation.MaterializedView

		sourceView, found := lo.Find(sourceMaterializedViews, func(view *PostgresMaterializedView) bool {
			return view.Name == targetView.Name
		})
		if !found {
			additions = append(additions, targetView.Instructions()...)
			continue
		}

		// A DROP statement of the view removes every index of it.
		if targetView.Def != sourceView.Def || !targetView.HasEqualColumns(sourceView) {
			d.markRecreated(targetView.Name)
			additions = append(additions, targetView.Instructions()...)
			continue
		}

		if targetView.Comment != sourceView.Comment {
			additions = append(additions, &PostgresCommentOnMaterializedViewInstruction{
				Name: targetView.Name,
				Text: targetView.Comment,
			})
		}

		additions = append(additions, diffMaterializedViewIndexes(targetView, sourceView)...)
	}

	// PostgreSQL refuses a DROP statement while another view still reads the object, so
	// a backward walk drops each dependent view first.
	for _, relation := range slices.Backward(sourceRelations) {
		if relation.View != nil {
			sourceView := relation.View

			targetView, found := lo.Find(targetViews, func(view *PostgresView) bool {
				return view.Name == sourceView.Name
			})
			if !found {
				earlyRemovals = append(earlyRemovals, &driversshared.SQLDropViewInstruction{Name: sourceView.Name})
				continue
			}

			if targetView.Def != sourceView.Def || targetView.CheckOption != sourceView.CheckOption ||
				!targetView.HasEqualColumns(sourceView) {
				earlyRemovals = append(earlyRemovals, &driversshared.SQLDropViewInstruction{Name: sourceView.Name})
			}

			continue
		}

		sourceView := relation.MaterializedView

		targetView, found := lo.Find(targetMaterializedViews, func(view *PostgresMaterializedView) bool {
			return view.Name == sourceView.Name
		})
		if !found {
			earlyRemovals = append(earlyRemovals, sourceView.DropInstruction())
			continue
		}

		if targetView.Def != sourceView.Def || !targetView.HasEqualColumns(sourceView) {
			earlyRemovals = append(earlyRemovals, sourceView.DropInstruction())
		}
	}

	return &driversshared.SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}

// Two independent views keep the order that the query gives.
func sortViewsByDependency(views []*PostgresView) []*PostgresView {
	viewByName := make(map[string]*PostgresView, len(views))

	for _, view := range views {
		viewByName[view.Name] = view
	}

	sorted := make([]*PostgresView, 0, len(views))
	visited := make(map[string]bool, len(views))

	var visit func(view *PostgresView)

	visit = func(view *PostgresView) {
		if visited[view.Name] {
			return
		}

		visited[view.Name] = true

		for _, column := range view.Columns {
			dependency, isView := viewByName[column.Table]
			if isView {
				visit(dependency)
			}
		}

		sorted = append(sorted, view)
	}

	for _, view := range views {
		visit(view)
	}

	return sorted
}
