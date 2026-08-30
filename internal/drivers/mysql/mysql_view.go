package driversmysql

import (
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLView struct {
	Name       string
	Definition string
}

func (v *MySQLView) Instructions() []driversshared.Instruction {
	return []driversshared.Instruction{&MySQLCreateViewInstruction{
		Name:       v.Name,
		Definition: v.Definition,
	}}
}

func (v *MySQLView) Diff(other *MySQLView) ([]driversshared.Instruction, error) {
	if v.Definition == other.Definition {
		return nil, nil
	}

	return []driversshared.Instruction{&MySQLCreateViewInstruction{
		Name:       v.Name,
		Definition: v.Definition,
		OrReplace:  true,
	}}, nil
}
