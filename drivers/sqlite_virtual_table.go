package drivers

// SQLite holds no ALTER statement for a virtual table, so a new definition prints a DROP
// statement and a CREATE statement. The module builds the shadow tables.
type SQLiteVirtualTable struct {
	Name string

	// The statement holds no semicolon.
	SQL string
}

func (t *SQLiteVirtualTable) CreateInstruction() *SQLiteCreateVirtualTableInstruction {
	return &SQLiteCreateVirtualTableInstruction{Definition: t.SQL}
}

func (t *SQLiteVirtualTable) DropInstruction() *SQLDropTableInstruction {
	return &SQLDropTableInstruction{Name: t.Name}
}
