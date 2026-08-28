package drivers

// A materialized view reads a table, a view, or a second materialized view, so the diff
// keeps the order of the dependency.
type PostgresMaterializedView struct {
	Name    string
	Def     string
	Columns []*PostgresViewColumn
	Indexes []*PostgresIndex
	Comment string
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

func (v *PostgresMaterializedView) Instructions() []Instruction {
	instructions := []Instruction{v.CreateInstruction()}

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

func sortMaterializedViewsByDependency(views []*PostgresMaterializedView) []*PostgresMaterializedView {
	viewByName := make(map[string]*PostgresMaterializedView, len(views))

	for _, view := range views {
		viewByName[view.Name] = view
	}

	sorted := make([]*PostgresMaterializedView, 0, len(views))
	visited := make(map[string]bool, len(views))

	var visit func(view *PostgresMaterializedView)

	visit = func(view *PostgresMaterializedView) {
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
