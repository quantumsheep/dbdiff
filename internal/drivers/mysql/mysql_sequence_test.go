package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMariaDBSequence(t *testing.T) {
	t.Run("Sequences", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers START WITH 1000 INCREMENT BY 10;")
		driver.ExecOnCurrent("CREATE SEQUENCE old_numbers;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     1000,
				Minimum:   1,
				Maximum:   9223372036854775806,
				Increment: 10,
				Cache:     1000,
			},
			&MySQLDropSequenceInstruction{
				Name: "old_numbers",
			},
		})
	})

	t.Run("ChangeSequence", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers INCREMENT BY 5;")
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers INCREMENT BY 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropSequenceInstruction{
				Name: "order_numbers",
			},
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     1,
				Minimum:   1,
				Maximum:   9223372036854775806,
				Increment: 5,
				Cache:     1000,
			},
		})
	})

	t.Run("ChangeSequenceOptions", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 20 CYCLE;")
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 500 CACHE 10 NOCYCLE;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropSequenceInstruction{
				Name: "order_numbers",
			},
			&MySQLCreateSequenceInstruction{
				Name:      "order_numbers",
				Start:     5,
				Minimum:   5,
				Maximum:   500,
				Increment: 1,
				Cache:     20,
				Cycle:     true,
			},
		})
	})
}
