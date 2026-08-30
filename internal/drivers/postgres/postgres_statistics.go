package driverspostgres

// PostgreSQL holds no action that changes the columns of such an object, so a new
// definition prints a DROP statement and a CREATE statement.
type PostgresStatistics struct {
	Name string

	// Def holds the text of pg_get_statisticsobjdef, with no semicolon.
	Def string
}

func (s *PostgresStatistics) CreateInstruction() *PostgresCreateStatisticsInstruction {
	return &PostgresCreateStatisticsInstruction{Definition: s.Def}
}

func (s *PostgresStatistics) DropInstruction() *PostgresDropStatisticsInstruction {
	return &PostgresDropStatisticsInstruction{Name: s.Name}
}
