package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLIndex(t *testing.T) {
	t.Run("RenameColumnOfAFunctionalIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.GuessRenames = true

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, name varchar(20), PRIMARY KEY (id), KEY upper_name ((upper(name))));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, nm varchar(20), PRIMARY KEY (id), KEY upper_name ((upper(nm))));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'alice');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "upper_name",
				TableName: "users",
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "nm",
					NewColumnName: "name",
				},
			},
			&MySQLCreateIndexInstruction{
				Name:      "upper_name",
				TableName: "users",
				Keys:      []string{"(upper(`name`))"},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("CreateIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"`name`"},
			},
		})
	})

	t.Run("DropIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
			},
		})
	})

	t.Run("ChangeIndexToUnique", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"`name`"},
			},
		})
	})

	t.Run("IndexWithPrefixLengthAndDescendingKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));
			CREATE INDEX idx_articles ON articles (title(10), score DESC);
		`)
		driver.ExecOnCurrent("CREATE TABLE articles (id int NOT NULL, title text, score int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Name:      "idx_articles",
				TableName: "articles",
				Keys:      []string{"`title`(10)", "`score` DESC"},
			},
		})
	})
}
