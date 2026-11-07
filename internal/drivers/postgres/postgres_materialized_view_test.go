package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresMaterializedView(t *testing.T) {
	t.Run("CreateMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, active BOOLEAN);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW active_users AS SELECT id FROM users WHERE active;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "active_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})
	})

	t.Run("DropMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "all_users"},
		})
	})

	t.Run("ModifyMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW selected_users AS SELECT id FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT, active BOOLEAN);
			CREATE MATERIALIZED VIEW selected_users AS SELECT id FROM users WHERE active;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropMaterializedViewInstruction{Name: "selected_users"},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "selected_users",
				Query: " SELECT id\n   FROM users\n  WHERE active;",
			},
		})
	})

	t.Run("CreateMaterializedViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW first_view AS SELECT id FROM users;
			CREATE MATERIALIZED VIEW second_view AS SELECT id FROM first_view;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "first_view",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "second_view",
				Query: " SELECT id\n   FROM first_view;",
			},
		})
	})

	t.Run("CreateMaterializedViewWithIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE UNIQUE INDEX all_users_id ON all_users (id);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX all_users_id ON all_users USING btree (id)",
			},
		})
	})

	t.Run("ModifyMaterializedViewComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
		`
		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`COMMENT ON MATERIALIZED VIEW all_users IS 'every person';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnMaterializedViewInstruction{
				Name: "all_users",
				Text: "every person",
			},
		})
	})
}
