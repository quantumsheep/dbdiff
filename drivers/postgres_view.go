package drivers

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
