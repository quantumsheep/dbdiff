package driversmysql

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
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
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers INCREMENT BY 1 CACHE 0;")
		driver.ExecOnCurrent("SELECT NEXTVAL(order_numbers);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterSequenceInstruction{
				Name: "order_numbers",
				Increment: sql.NullInt64{
					Int64: 5,
					Valid: true,
				},
				Cache: sql.NullInt64{
					Int64: 1000,
					Valid: true,
				},
			},
		})

		var nextValue int64

		err := driver.currentConnection.QueryRowContext(t.Context(), "SELECT NEXTVAL(order_numbers);").Scan(&nextValue)
		require.NoError(t, err)
		require.Equal(t, int64(2), nextValue)
	})

	t.Run("ChangeSequenceOptions", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE SEQUENCE order_numbers START WITH 7 MINVALUE 5 MAXVALUE 500 CACHE 20 CYCLE;")
		driver.ExecOnCurrent("CREATE SEQUENCE order_numbers MINVALUE 5 MAXVALUE 600 CACHE 10 NOCYCLE;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterSequenceInstruction{
				Name: "order_numbers",
				Start: sql.NullInt64{
					Int64: 7,
					Valid: true,
				},
				Maximum: sql.NullInt64{
					Int64: 500,
					Valid: true,
				},
				Cache: sql.NullInt64{
					Int64: 20,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})
}
