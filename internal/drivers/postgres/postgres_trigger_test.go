package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresTrigger(t *testing.T) {
	t.Run("IndexesAndTriggersKeepTheNameOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (name TEXT, email TEXT);`)
		driver.ExecOnDesired(`
			CREATE TABLE users (name TEXT, email TEXT);
			CREATE INDEX idx_b_name ON users(name);
			CREATE INDEX idx_a_email ON users(email);
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER trg_b_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch();
			CREATE TRIGGER trg_a_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch();
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION touch()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END; $function$",
			},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_a_email ON users USING btree (email)"},
			&PostgresCreateIndexInstruction{Definition: "CREATE INDEX idx_b_name ON users USING btree (name)"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_a_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
			&PostgresCreateTriggerInstruction{Definition: "CREATE TRIGGER trg_b_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch()"},
		})
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE TABLE users (updated_at TIMESTAMP);`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`
			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
		})
	})

	t.Run("CreateADisabledTrigger", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`CREATE TABLE users (updated_at TIMESTAMP);`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`
			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
			ALTER TABLE users DISABLE TRIGGER set_timestamp;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "DISABLE",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("AlterTriggerEnableMode", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`
		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`ALTER TABLE users DISABLE TRIGGER set_timestamp;`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`ALTER TABLE users ENABLE ALWAYS TRIGGER set_timestamp;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE ALWAYS",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("AlterTriggerDefinitionAndEnableMode", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
		`
		driver.ExecOnCurrent(setup)
		driver.ExecOnCurrent(`
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`)

		driver.ExecOnDesired(setup)
		driver.ExecOnDesired(`
			CREATE TRIGGER set_timestamp BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
			ALTER TABLE users ENABLE REPLICA TRIGGER set_timestamp;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTriggerInstruction{
				Name:      "set_timestamp",
				TableName: "users",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER set_timestamp BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresTriggerEnableAction{
						Mode:        "ENABLE REPLICA",
						TriggerName: "set_timestamp",
					},
				},
			},
		})
	})

	t.Run("EqualTriggers", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE OR REPLACE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = NOW();
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;

			CREATE TABLE users (updated_at TIMESTAMP);
			CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();
		`
		driver.ExecOnCurrent(schema)
		driver.ExecOnDesired(schema)
		driver.RequireInstructions(nil)
	})
}
