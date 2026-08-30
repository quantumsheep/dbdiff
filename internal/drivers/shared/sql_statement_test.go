package driversshared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileUsesTransaction(t *testing.T) {
	t.Run("APlainFile", func(t *testing.T) {
		require.True(t, FileUsesTransaction("CREATE TABLE t (id INTEGER);\n"))
	})

	t.Run("TheDirectiveAloneOnALine", func(t *testing.T) {
		require.False(t, FileUsesTransaction("-- dbdiff:no-transaction\nVACUUM;\n"))
	})

	t.Run("TheDirectiveWithSpaceAround", func(t *testing.T) {
		require.False(t, FileUsesTransaction("VACUUM;\n   -- dbdiff:no-transaction   \n"))
	})

	t.Run("TheAtlasDirective", func(t *testing.T) {
		require.False(t, FileUsesTransaction("-- atlas:txmode none\nVACUUM;\n"))
	})

	t.Run("TheTextInsideALongerLine", func(t *testing.T) {
		require.True(t, FileUsesTransaction("-- dbdiff:no-transaction is the directive\n"))
	})

	t.Run("AnEmptyFile", func(t *testing.T) {
		require.True(t, FileUsesTransaction(""))
	})
}

func TestSplitSQLStatements(t *testing.T) {
	t.Run("TwoStatements", func(t *testing.T) {
		require.Equal(t, []string{
			"CREATE TABLE users (id INTEGER)",
			"CREATE INDEX CONCURRENTLY ix ON users (id)",
		}, SplitSQLStatements("CREATE TABLE users (id INTEGER);\nCREATE INDEX CONCURRENTLY ix ON users (id);\n"))
	})

	t.Run("TheLastStatementNeedsNoSemicolon", func(t *testing.T) {
		require.Equal(t, []string{"VACUUM"}, SplitSQLStatements("VACUUM"))
	})

	t.Run("AFunctionBodyHoldsASemicolon", func(t *testing.T) {
		content := `
			CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $function$
				BEGIN
					NEW.updated_at := NOW();
					RETURN NEW;
				END;
			$function$;
			CREATE INDEX CONCURRENTLY ix ON users (id);
		`

		statements := SplitSQLStatements(content)
		require.Len(t, statements, 2)
		require.Contains(t, statements[0], "RETURN NEW;")
		require.Equal(t, "CREATE INDEX CONCURRENTLY ix ON users (id)", statements[1])
	})

	t.Run("AStringHoldsASemicolon", func(t *testing.T) {
		require.Equal(t, []string{
			"INSERT INTO t VALUES ('a;b', 'it''s;here')",
			"VACUUM",
		}, SplitSQLStatements("INSERT INTO t VALUES ('a;b', 'it''s;here');VACUUM;"))
	})

	t.Run("AnIdentifierHoldsASemicolon", func(t *testing.T) {
		require.Equal(t, []string{
			`CREATE TABLE "a;b" (id INTEGER)`,
			"VACUUM",
		}, SplitSQLStatements(`CREATE TABLE "a;b" (id INTEGER);VACUUM;`))
	})

	t.Run("AnEscapeStringHoldsAQuote", func(t *testing.T) {
		require.Equal(t, []string{
			`INSERT INTO t VALUES (E'a\';b')`,
			"VACUUM",
		}, SplitSQLStatements(`INSERT INTO t VALUES (E'a\';b');VACUUM;`))
	})

	t.Run("ACommentHoldsASemicolon", func(t *testing.T) {
		require.Equal(t, []string{
			"-- a; comment\n/* another; one /* that nests; */ */\nVACUUM",
		}, SplitSQLStatements("-- a; comment\n/* another; one /* that nests; */ */\nVACUUM;"))
	})

	t.Run("ADollarNamesAParameter", func(t *testing.T) {
		require.Equal(t, []string{
			"SELECT $1",
			"VACUUM",
		}, SplitSQLStatements("SELECT $1;VACUUM;"))
	})

	t.Run("NoStatement", func(t *testing.T) {
		require.Empty(t, SplitSQLStatements("\n  \n;;\n"))
	})

	t.Run("ATriggerBodyHoldsSemicolons", func(t *testing.T) {
		content := "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; SELECT 2; END;\nVACUUM;"

		require.Equal(t, []string{
			"CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; SELECT 2; END",
			"VACUUM",
		}, SplitSQLStatements(content))
	})

	t.Run("ATriggerBodyHoldsACaseExpression", func(t *testing.T) {
		content := "CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT CASE WHEN NEW.id > 0 THEN 1 ELSE 2 END; END;\nVACUUM;"

		statements := SplitSQLStatements(content)
		require.Len(t, statements, 2)
		require.Contains(t, statements[0], "ELSE 2 END; END")
	})

	t.Run("ARuleHoldsTwoActions", func(t *testing.T) {
		content := "CREATE RULE r AS ON DELETE TO users DO INSTEAD (SELECT 1; SELECT 2);\nVACUUM;"

		require.Equal(t, []string{
			"CREATE RULE r AS ON DELETE TO users DO INSTEAD (SELECT 1; SELECT 2)",
			"VACUUM",
		}, SplitSQLStatements(content))
	})

	t.Run("AnAtomicBodyHoldsSemicolons", func(t *testing.T) {
		content := "CREATE FUNCTION one() RETURNS integer BEGIN ATOMIC SELECT 1; END;\nVACUUM;"

		require.Equal(t, []string{
			"CREATE FUNCTION one() RETURNS integer BEGIN ATOMIC SELECT 1; END",
			"VACUUM",
		}, SplitSQLStatements(content))
	})

	t.Run("ATransactionOfTheUserSplits", func(t *testing.T) {
		require.Equal(t, []string{
			"BEGIN",
			"SELECT 1",
			"COMMIT",
		}, SplitSQLStatements("BEGIN;\nSELECT 1;\nCOMMIT;"))
	})

	t.Run("AProcedureBodyHoldsSemicolons", func(t *testing.T) {
		content := "CREATE PROCEDURE prune() BEGIN DELETE FROM a; DELETE FROM b; END;\nSELECT 1;"

		require.Equal(t, []string{
			"CREATE PROCEDURE prune() BEGIN DELETE FROM a; DELETE FROM b; END",
			"SELECT 1",
		}, SplitSQLStatements(content))
	})

	t.Run("AMySQLFunctionBodyHoldsSemicolons", func(t *testing.T) {
		content := "CREATE FUNCTION f(x int) RETURNS int BEGIN RETURN x; END;\nSELECT 1;"

		require.Equal(t, []string{
			"CREATE FUNCTION f(x int) RETURNS int BEGIN RETURN x; END",
			"SELECT 1",
		}, SplitSQLStatements(content))
	})

	t.Run("AnEventBodyHoldsSemicolons", func(t *testing.T) {
		content := "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO BEGIN SET @a = 1; SET @b = 2; END;\nSELECT 1;"

		require.Equal(t, []string{
			"CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO BEGIN SET @a = 1; SET @b = 2; END",
			"SELECT 1",
		}, SplitSQLStatements(content))
	})

	t.Run("ABacktickIdentifierHoldsASemicolon", func(t *testing.T) {
		require.Equal(t, []string{
			"SELECT `a;b` FROM t",
			"SELECT 2",
		}, SplitSQLStatements("SELECT `a;b` FROM t;\nSELECT 2;"))
	})
}
