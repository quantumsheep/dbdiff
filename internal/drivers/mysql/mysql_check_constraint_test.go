package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
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
}
