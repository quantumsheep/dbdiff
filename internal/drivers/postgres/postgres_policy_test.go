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
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(NOT secret)",
			},
			&PostgresDropPolicyInstruction{
				Name:      "docs_old",
				TableName: "docs",
			},
		})
	})
}
