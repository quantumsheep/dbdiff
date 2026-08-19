package drivers

// A PostgresViewColumn is one column that a view reads. The diff compares these columns,
// because a type change of such a column keeps the definition text of the view equal.
type PostgresViewColumn struct {
	Table  string
	Column string
	Type   string
}

type PostgresView struct {
	Name    string
	Def     string
	Columns []*PostgresViewColumn

	// CheckOption holds LOCAL or CASCADED. The query text of the view holds none of it, so
	// the diff compares this field beside the text.
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
