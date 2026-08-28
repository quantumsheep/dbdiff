package drivers

type PostgresExtension struct {
	Name    string
	Version string
}

func (e *PostgresExtension) CreateInstruction() *PostgresCreateExtensionInstruction {
	return &PostgresCreateExtensionInstruction{Name: e.Name}
}

func (e *PostgresExtension) UpdateInstruction() *PostgresAlterExtensionInstruction {
	return &PostgresAlterExtensionInstruction{
		Name:       e.Name,
		NewVersion: e.Version,
	}
}

func (e *PostgresExtension) DropInstruction() *PostgresDropExtensionInstruction {
	return &PostgresDropExtensionInstruction{Name: e.Name}
}
