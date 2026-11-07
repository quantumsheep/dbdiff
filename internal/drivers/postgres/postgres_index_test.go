package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresIndex(t *testing.T) {
	t.Run("ModifyIndexComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT, email TEXT);
			CREATE INDEX users_email ON users (email);
		`
		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`COMMENT ON INDEX users_email IS 'the lookup key';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnIndexInstruction{
				Name: "users_email",
				Text: "the lookup key",
			},
		})
	})

	t.Run("Indexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
		})
	})

	t.Run("EqualIndexes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (name TEXT); CREATE INDEX idx_name ON users(name);`
		driver.ExecOnCurrent(schema)
		driver.ExecOnDesired(schema)
		driver.RequireInstructions(nil)
	})

	t.Run("DropColumnDropsItsIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, email TEXT); CREATE INDEX idx_email ON users(email);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnKeepsAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT); CREATE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_name ON users USING btree (name)"},
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnAndModifyAnotherIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, email TEXT, name TEXT); CREATE INDEX idx_email ON users(email); CREATE INDEX idx_name ON users(name);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT); CREATE UNIQUE INDEX idx_name ON users(name);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_name"},
			&PostgresCreateIndexInstruction{Definition: "CREATE UNIQUE INDEX idx_name ON users USING btree (name)"},
			&driversshared.SQLDropIndexInstruction{Name: "idx_email"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})
}
