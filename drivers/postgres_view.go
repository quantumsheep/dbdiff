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
