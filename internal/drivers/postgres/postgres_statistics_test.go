package driverspostgres

import (
	"fmt"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresStatistics(t *testing.T) {
	t.Run("CreateStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE t (a INT, b INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, b FROM t",
			},
		})
	})

	t.Run("DropStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE t (a INT, b INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)
		driver.ExecOnDesired(`CREATE TABLE t (a INT, b INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
		})
	})

	t.Run("ModifyStatistics", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE t (a INT, b INT, c INT);
			CREATE STATISTICS st_ab ON a, b FROM t;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE t (a INT, b INT, c INT);
			CREATE STATISTICS st_ab ON a, c FROM t;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropStatisticsInstruction{Name: "st_ab"},
			&PostgresCreateStatisticsInstruction{
				Definition: "CREATE STATISTICS st_ab ON a, c FROM t",
			},
		})
	})

	t.Run("KeepTheSchemaNameInAStringLiteral", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE t (a INT, b TEXT);`)

		driver.ExecOnDesired(`CREATE TABLE t (a INT, b TEXT);`)
		driver.ExecOnDesired(fmt.Sprintf(`CREATE STATISTICS st_ab ON a, (b || '%s.t') FROM t;`, driver.desiredSchema))
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateStatisticsInstruction{
				Definition: fmt.Sprintf("CREATE STATISTICS st_ab ON a, (b || '%s.t'::text) FROM t", driver.desiredSchema),
			},
		})
	})
}
