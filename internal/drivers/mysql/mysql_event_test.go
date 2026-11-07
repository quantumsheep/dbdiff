package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLEvent(t *testing.T) {
	t.Run("CreateAndDropEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT old_nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
			&MySQLDropEventInstruction{
				Name: "old_nightly",
			},
		})
	})

	t.Run("ChangeEvent", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropEventInstruction{
				Name: "nightly",
			},
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 2 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})
	})

	t.Run("EventsWithTwoStartTimesCompareAsEqual", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")
		driver.ExecOnCurrent("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2031-06-15 12:00:00' DO SET @cleaned = 1;")

		driver.RequireInstructions(nil)
	})
}

func TestMariaDBEvent(t *testing.T) {
	t.Run("Event", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE EVENT nightly ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DO SET @cleaned = 1;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateEventInstruction{
				Name:       "nightly",
				Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' ON COMPLETION NOT PRESERVE ENABLE DO SET @cleaned = 1",
			},
		})
	})
}
