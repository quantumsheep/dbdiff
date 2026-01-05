package drivers

type PostgresIndex struct {
	Name string
	Def  string
}

func (i *PostgresIndex) String() string {
	return i.Def + ";"
}
