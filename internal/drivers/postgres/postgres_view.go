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

	// A view that names no column of a relation, for example with count(*), has no column
	// of it in Columns. Thus the dependency order reads this field.
	DependencyNames []string

	// The query text of the view holds no check option, so the diff compares this field too.
	CheckOption string

	Comment string
}

func hasEqualViewColumns(first []*PostgresViewColumn, second []*PostgresViewColumn) bool {
	return slices.EqualFunc(first, second, func(firstColumn *PostgresViewColumn, secondColumn *PostgresViewColumn) bool {
		return *firstColumn == *secondColumn
	})
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

func (r *PostgresRelation) DependencyNames() []string {
	if r.View != nil {
		return r.View.DependencyNames
	}

	return r.MaterializedView.DependencyNames
}

// A view and a materialized view can have the same definition, so the comparison checks the
// kind too. A type change of a column that the relation reads keeps the definition text
// equal, so the comparison checks the columns too.
func (r *PostgresRelation) HasEqualDefinition(other *PostgresRelation) bool {
	if r.View != nil {
		return other.View != nil &&
			r.View.Def == other.View.Def &&
			r.View.CheckOption == other.View.CheckOption &&
			hasEqualViewColumns(r.View.Columns, other.View.Columns)
	}

	return other.MaterializedView != nil &&
		r.MaterializedView.Def == other.MaterializedView.Def &&
		hasEqualViewColumns(r.MaterializedView.Columns, other.MaterializedView.Columns)
}

func (r *PostgresRelation) DropInstruction() driversshared.Instruction {
	if r.View != nil {
		return &driversshared.SQLDropViewInstruction{Name: r.View.Name}
	}

	return r.MaterializedView.DropInstruction()
}

func (r *PostgresRelation) Instructions() []driversshared.Instruction {
	if r.View != nil {
		return r.View.Instructions()
	}

	return r.MaterializedView.Instructions()
}

// Two independent relations keep the order that the queries give.
func sortRelationsByDependency(
	views []*PostgresView,
	materializedViews []*PostgresMaterializedView,
) []*PostgresRelation {
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

	return sortByDependency(relations, (*PostgresRelation).Name, (*PostgresRelation).DependencyNames)
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

		view.DependencyNames, err = d.GetViewDependencyNames(ctx, db, view.Name)
		if err != nil {
			return nil, err
		}
	}

	return views, nil
}

// The type of each column comes with it, because a type change makes the view invalid.
func (d *PostgresDriver) GetViewColumns(
	ctx context.Context,
	db *sql.DB,
	viewName string,
) ([]*PostgresViewColumn, error) {
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

// The query reads each relation that the view reads, also a relation of which the view
// names no column.
func (d *PostgresDriver) GetViewDependencyNames(ctx context.Context, db *sql.DB, viewName string) ([]string, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT DISTINCT source_class.relname
		FROM pg_depend d
		JOIN pg_rewrite view_rule ON view_rule.oid = d.objid
		JOIN pg_class view_class ON view_class.oid = view_rule.ev_class
		JOIN pg_class source_class ON source_class.oid = d.refobjid
		WHERE d.classid = 'pg_rewrite'::regclass
		AND d.refclassid = 'pg_class'::regclass
		AND view_class.relnamespace = current_schema()::regnamespace
		AND view_class.relname = $1
		AND view_class.oid <> d.refobjid
		ORDER BY source_class.relname
	`, func(rows *sql.Rows) (string, error) {
		var name string

		err := rows.Scan(&name)

		return name, err
	}, viewName)
}

// A view can read a materialized view, and a materialized view can read a view, so the
// two kinds share one section and one dependency order.
func (d *PostgresDriver) DiffViewsAndMaterializedViews(ctx context.Context) (*SectionDiff, error) {
	var earlyRemovals []driversshared.Instruction
	var additions []driversshared.Instruction

	desiredViews, err := d.GetViews(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentViews, err := d.GetViews(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	desiredMaterializedViews, err := d.GetMaterializedViews(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentMaterializedViews, err := d.GetMaterializedViews(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	desiredRelations := sortRelationsByDependency(desiredViews, desiredMaterializedViews)
	currentRelations := sortRelationsByDependency(currentViews, currentMaterializedViews)

	desiredRelationByName := lo.KeyBy(desiredRelations, (*PostgresRelation).Name)
	currentRelationByName := lo.KeyBy(currentRelations, (*PostgresRelation).Name)

	// PostgreSQL refuses to drop a relation while another relation reads it. CASCADE can
	// drop objects that the diff does not see. Thus the diff also drops and creates again
	// each relation that reads a dropped relation. DiffTables marks each relation that reads
	// a rebuilt column. The dependency order puts each relation after the relations that it
	// reads, so one pass finds every indirect reader too.
	droppedNames := map[string]bool{}

	for _, currentRelation := range currentRelations {
		desiredRelation, found := desiredRelationByName[currentRelation.Name()]
		readsADroppedRelation := lo.SomeBy(currentRelation.DependencyNames(), func(name string) bool {
			return droppedNames[name]
		})
		if !found || readsADroppedRelation || d.recreatedObjectNames[currentRelation.Name()] ||
			!desiredRelation.HasEqualDefinition(currentRelation) {
			droppedNames[currentRelation.Name()] = true
		}
	}

	for _, desiredRelation := range desiredRelations {
		currentRelation, found := currentRelationByName[desiredRelation.Name()]
		if !found {
			additions = append(additions, desiredRelation.Instructions()...)
			continue
		}

		// A DROP statement of a materialized view removes every index of it, and the
		// create instructions give back the indexes.
		if droppedNames[desiredRelation.Name()] {
			d.markRecreated(desiredRelation.Name())
			additions = append(additions, desiredRelation.Instructions()...)
			continue
		}

		if desiredRelation.View != nil {
			if desiredRelation.View.Comment != currentRelation.View.Comment {
				additions = append(additions, &PostgresCommentOnViewInstruction{
					Name: desiredRelation.View.Name,
					Text: desiredRelation.View.Comment,
				})
			}

			continue
		}

		if desiredRelation.MaterializedView.Comment != currentRelation.MaterializedView.Comment {
			additions = append(additions, &PostgresCommentOnMaterializedViewInstruction{
				Name: desiredRelation.MaterializedView.Name,
				Text: desiredRelation.MaterializedView.Comment,
			})
		}

		additions = append(additions, diffMaterializedViewIndexes(desiredRelation.MaterializedView, currentRelation.MaterializedView)...)
	}

	// A backward walk drops each relation before the relations that it reads.
	for _, currentRelation := range slices.Backward(currentRelations) {
		if droppedNames[currentRelation.Name()] {
			earlyRemovals = append(earlyRemovals, currentRelation.DropInstruction())
		}
	}

	return &SectionDiff{
		EarlyRemovals: earlyRemovals,
		Additions:     additions,
	}, nil
}
