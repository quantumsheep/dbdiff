package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLIndex(t *testing.T) {
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
