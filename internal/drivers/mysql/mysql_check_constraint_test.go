package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLCheckConstraint(t *testing.T) {
	t.Run("AddCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "chk_users_age",
						Expression: "(`age` > 0)",
					},
				},
			},
		})
	})

	t.Run("RemoveCheckConstraint", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropCheckConstraintAction{
					Name: "chk_users_age",
				},
			},
		})
	})
}

func TestMariaDBCheckConstraint(t *testing.T) {
	t.Run("CheckConstraint", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL,
				age int,
				PRIMARY KEY (id),
				CONSTRAINT chk_users_age CHECK (age > 0)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "chk_users_age",
						Expression: "`age` > 0",
					},
				},
			},
		})
	})

	t.Run("ChangeTheExpressionOfAColumnCheck", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 1), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "a",
						Type: "int(11)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "a",
						Expression:  "`a` > 1",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "a": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("RemoveAColumnCheck", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, a int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "a",
						Type: "int(11)",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "a": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})
}
