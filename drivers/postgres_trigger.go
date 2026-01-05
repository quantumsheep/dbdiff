package drivers

type PostgresTrigger struct {
	Name string
	Def  string
}

func (t *PostgresTrigger) String() string {
	return t.Def + ";"
}
