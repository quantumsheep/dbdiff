package driverspostgres

import (
	"fmt"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresRule(t *testing.T) {
	t.Run("CreateRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE base (a INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE no_delete AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})
	})

	t.Run("CreateARuleThatWritesIntoANewView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE base (a INT);
			CREATE TABLE audit (a INT);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE TABLE audit (a INT);
			CREATE VIEW audit_entries AS SELECT a FROM audit;
			CREATE RULE log_delete AS ON DELETE TO base DO ALSO INSERT INTO audit_entries (a) VALUES (OLD.a);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "audit_entries",
				Query: " SELECT a\n   FROM audit;",
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE log_delete AS\n    ON DELETE TO base DO  INSERT INTO audit_entries (a)\n  VALUES (old.a);",
			},
		})
	})

	t.Run("DropRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE base (a INT);
			CREATE RULE no_delete AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.ExecOnDesired(`CREATE TABLE base (a INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropRuleInstruction{
				Name:      "no_delete",
				TableName: "base",
			},
		})
	})

	t.Run("ModifyRule", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE base (a INT);
			CREATE RULE guard AS ON UPDATE TO base DO INSTEAD NOTHING;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE RULE guard AS ON DELETE TO base DO INSTEAD NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropRuleInstruction{
				Name:      "guard",
				TableName: "base",
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE guard AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
			},
		})
	})

	t.Run("ViewRuleIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE base (a INT);
			CREATE VIEW v AS SELECT a FROM base;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE VIEW v AS SELECT a FROM base;
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateRuleThatNamesASecondTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE TABLE zlog (a INT);
			CREATE RULE log_insert AS ON INSERT TO base DO ALSO INSERT INTO zlog VALUES (NEW.a);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "base",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "zlog",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateRuleInstruction{
				Definition: "CREATE RULE log_insert AS\n    ON INSERT TO base DO  INSERT INTO zlog (a)\n  VALUES (new.a);",
			},
		})
	})

	t.Run("KeepTheSchemaNameInAStringLiteral", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE base (a INT, note TEXT);`)

		driver.ExecOnDesired(`CREATE TABLE base (a INT, note TEXT);`)
		driver.ExecOnDesired(fmt.Sprintf(
			`CREATE RULE add_note AS ON INSERT TO base DO ALSO UPDATE base SET note = '%s.base' WHERE a = 0;`,
			driver.desiredSchema))
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateRuleInstruction{
				Definition: fmt.Sprintf("CREATE RULE add_note AS\n    ON INSERT TO base DO  UPDATE base SET note = '%s.base'::text\n  WHERE (base.a = 0);",
					driver.desiredSchema),
			},
		})
	})
}
