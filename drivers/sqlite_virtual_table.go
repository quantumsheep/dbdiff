package drivers

// A SQLiteVirtualTable is a table that a module builds, for example fts4. SQLite holds no
// ALTER statement for such a table, so a new definition prints a DROP statement and a
// CREATE statement. The module builds the shadow tables of the table, and the diff names
// none of them.
type SQLiteVirtualTable struct {
	Name string

	// SQL holds the CREATE VIRTUAL TABLE statement of sqlite_master, with no semicolon.
	SQL string
}

func (t *SQLiteVirtualTable) CreateInstruction() *SQLiteCreateVirtualTableInstruction {
	return &SQLiteCreateVirtualTableInstruction{Definition: t.SQL}
}

func (t *SQLiteVirtualTable) DropInstruction() *SQLDropTableInstruction {
	return &SQLDropTableInstruction{Name: t.Name}
}
