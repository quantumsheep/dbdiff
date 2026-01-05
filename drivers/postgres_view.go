package drivers

type PostgresView struct {
	Name string
	Def  string
}

func (v *PostgresView) String() string {
	return "CREATE VIEW \"" + v.Name + "\" AS " + v.Def
}
