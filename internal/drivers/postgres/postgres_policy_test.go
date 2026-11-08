package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresPolicy(t *testing.T) {
	t.Run("ModifyPolicy", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (secret);
			CREATE POLICY docs_old ON docs FOR DELETE USING (true);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (NOT secret);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropPolicyInstruction{
				Name:      "docs_read",
				TableName: "docs",
			},
			&PostgresDropPolicyInstruction{
				Name:      "docs_old",
				TableName: "docs",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(NOT secret)",
			},
		})
	})

	t.Run("CreateAPolicyThatReadsANewView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE docs (id INT, public BOOLEAN);`)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT, public BOOLEAN);
			CREATE VIEW public_doc_ids AS SELECT id FROM docs WHERE public;
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (id IN (SELECT id FROM public_doc_ids));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{
						Mode: "ENABLE",
					},
				},
			},
			&PostgresCreateViewInstruction{
				Name:  "public_doc_ids",
				Query: " SELECT id\n   FROM docs\n  WHERE public;",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(id IN ( SELECT public_doc_ids.id\n   FROM public_doc_ids))",
			},
		})
	})

	t.Run("DropAViewAndThePolicyThatReadsIt", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (id INT, public BOOLEAN);
			CREATE VIEW public_doc_ids AS SELECT id FROM docs WHERE public;
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (id IN (SELECT id FROM public_doc_ids));
		`)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT, public BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropPolicyInstruction{
				Name:      "docs_read",
				TableName: "docs",
			},
			&driversshared.SQLDropViewInstruction{Name: "public_doc_ids"},
		})
	})

	t.Run("CreateAPolicyForARoleWithACommaInItsName", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_write ON docs FOR INSERT TO "dbdiff,writer", dbdiff_reader WITH CHECK (id > 0);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreatePolicyInstruction{
				Name:       "docs_write",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "INSERT",
				Roles: []string{
					"dbdiff,writer",
					"dbdiff_reader",
				},
				WithCheck: "(id > 0)",
			},
		})
	})

	t.Run("CreateAPolicyWithAConstantExpression", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			CREATE POLICY docs_all ON docs USING (true) WITH CHECK (false);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreatePolicyInstruction{
				Name:       "docs_all",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "ALL",
				Roles: []string{
					"public",
				},
				Using:     "true",
				WithCheck: "false",
			},
		})
	})

	t.Run("AddAPolicyThatReadsANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE accounts (id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT);
			CREATE TABLE memberships (account_id INT);
			CREATE POLICY members ON accounts USING (id IN (SELECT account_id FROM memberships));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "memberships",
				Columns: []*PostgresColumn{
					{
						Name: "account_id",
						Type: "integer",
					},
				},
			},
			&PostgresCreatePolicyInstruction{
				Name:       "members",
				TableName:  "accounts",
				Permissive: "PERMISSIVE",
				Command:    "ALL",
				Roles:      []string{"public"},
				Using:      "(id IN ( SELECT memberships.account_id\n   FROM memberships))",
			},
		})
	})

	t.Run("CreateATableWithAPolicyThatReadsANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT);
			CREATE TABLE memberships (account_id INT);
			CREATE POLICY members ON accounts USING (id IN (SELECT account_id FROM memberships));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "accounts",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "memberships",
				Columns: []*PostgresColumn{
					{
						Name: "account_id",
						Type: "integer",
					},
				},
			},
			&PostgresCreatePolicyInstruction{
				Name:       "members",
				TableName:  "accounts",
				Permissive: "PERMISSIVE",
				Command:    "ALL",
				Roles:      []string{"public"},
				Using:      "(id IN ( SELECT memberships.account_id\n   FROM memberships))",
			},
		})
	})
}
