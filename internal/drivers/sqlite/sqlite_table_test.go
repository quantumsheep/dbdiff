package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteTable(t *testing.T) {
	t.Run("CreateTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "users",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:    "name",
						Type:    "TEXT",
						NotNull: true,
					},
				},
			},
		})
	})

	t.Run("CreateTableWithoutRowIDAndStrict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE sessions (id TEXT PRIMARY KEY, token TEXT) WITHOUT ROWID, STRICT;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "sessions",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "TEXT",
						NotNull:    true,
						PrimaryKey: true,
					},
					{
						Name: "token",
						Type: "TEXT",
					},
				},
				WithoutRowID: true,
				Strict:       true,
			},
		})
	})

	t.Run("AddStrictRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE events (id INTEGER, label TEXT);
			INSERT INTO events (id, label) VALUES (1, 'start');
		`)
		driver.ExecOnDesired(`CREATE TABLE events (id INTEGER, label TEXT) STRICT;`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("events",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_events_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "id",
							Type: "INTEGER",
						},
						{
							Name: "label",
							Type: "TEXT",
						},
					},
					Strict: true,
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_events_temp",
					ColumnNames:       []string{"id", "label"},
					SelectExpressions: []string{`"id"`, `"label"`},
					FromTableName:     "events",
				},
				&driversshared.SQLDropTableInstruction{Name: "events"},
				&SQLiteAlterTableInstruction{
					Name:   "_events_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "events"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("events", "")
		require.Equal(t, []map[string]any{
			{
				"id":    int64(1),
				"label": "start",
			},
		}, rows)
	})

	t.Run("CreateTableWithANamedPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT,
				member TEXT,
				CONSTRAINT pk_memberships PRIMARY KEY (team, member)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "memberships",
				Columns: []*SQLiteColumn{
					{
						Name: "team",
						Type: "TEXT",
					},
					{
						Name: "member",
						Type: "TEXT",
					},
				},
				PrimaryKey:     []string{"team", "member"},
				PrimaryKeyName: "pk_memberships",
			},
		})
	})

	t.Run("ReorderColumnsRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE people (name TEXT, age INTEGER);
			INSERT INTO people (name, age) VALUES ('Ada', 36);
		`)
		driver.ExecOnDesired(`CREATE TABLE people (age INTEGER, name TEXT);`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("people",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_people_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "age",
							Type: "INTEGER",
						},
						{
							Name: "name",
							Type: "TEXT",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_people_temp",
					ColumnNames:       []string{"age", "name"},
					SelectExpressions: []string{`"age"`, `"name"`},
					FromTableName:     "people",
				},
				&driversshared.SQLDropTableInstruction{Name: "people"},
				&SQLiteAlterTableInstruction{
					Name:   "_people_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "people"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("people", "")
		require.Equal(t, []map[string]any{
			{"age": int64(36), "name": "Ada"},
		}, rows)
	})

	t.Run("RecreationAvoidsTheNameOfAnExistingTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE t (id INTEGER PRIMARY KEY);
			CREATE TABLE _t_temp (id INTEGER PRIMARY KEY);
			CREATE INDEX _T_TEMP2 ON _t_temp (id);
			INSERT INTO t (id) VALUES (1);
			INSERT INTO _t_temp (id) VALUES (2);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE);
			CREATE TABLE _t_temp (id INTEGER PRIMARY KEY);
			CREATE INDEX _T_TEMP2 ON _t_temp (id);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("t",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_t_temp3",
					Columns: []*SQLiteColumn{
						{
							Name:               "id",
							Type:               "INTEGER",
							PrimaryKey:         true,
							PrimaryKeyConflict: "REPLACE",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_t_temp3",
					ColumnNames:       []string{"id"},
					SelectExpressions: []string{`"id"`},
					FromTableName:     "t",
				},
				&driversshared.SQLDropTableInstruction{Name: "t"},
				&SQLiteAlterTableInstruction{
					Name:   "_t_temp3",
					Action: &driversshared.SQLRenameTableAction{NewName: "t"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
		}, driver.FetchAllFromCurrent("t", ""))
		require.Equal(t, []map[string]any{
			{"id": int64(2)},
		}, driver.FetchAllFromCurrent("_t_temp", ""))
	})

	t.Run("CreateTableWithNamedConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				id INTEGER,
				owner INTEGER,
				CONSTRAINT items_positive CHECK (id > 0),
				CONSTRAINT items_unique UNIQUE (id),
				CONSTRAINT items_owner FOREIGN KEY (owner) REFERENCES parents(id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "parents",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
				},
			},
			&SQLiteCreateTableInstruction{
				Name: "items",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
					{
						Name: "owner",
						Type: "INTEGER",
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{
						Name:    "items_unique",
						Columns: []string{"id"},
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Name:       "items_positive",
						Expression: "(id > 0)",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Name:     "items_owner",
						Table:    "parents",
						From:     []string{"owner"},
						To:       []string{"id"},
						OnUpdate: "NO ACTION",
						OnDelete: "NO ACTION",
					},
				},
			},
		})
	})

	t.Run("DropTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("DropTablesInTheReverseOrder", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "children"},
			&driversshared.SQLDropTableInstruction{Name: "parents"},
		})
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE "order ""list""" (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
			CREATE INDEX "idx name" ON "order ""list""" (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        `order "list"`,
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name: "name",
						Type: "TEXT",
					},
				},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "idx name",
				TableName: `order "list"`,
				Keys:      []string{`"name"`},
			},
		})
	})

	t.Run("CreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT,
				PRIMARY KEY (member, team)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "memberships",
				Columns: []*SQLiteColumn{
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "member",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name: "role",
						Type: "TEXT",
					},
				},
				PrimaryKey: []string{"member", "team"},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');`)

		_, err := driver.currentConnection.ExecContext(t.Context(), `INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'guest');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: memberships.member, memberships.team")
	})

	t.Run("CreateTableWithIntegerPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE counters (
				id INTEGER PRIMARY KEY,
				total INTEGER
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "counters",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name: "total",
						Type: "INTEGER",
					},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO counters (total) VALUES (5);`)

		rows := driver.FetchAllFromCurrent("counters", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "total": int64(5)},
		}, rows)
	})

	t.Run("RecreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level TEXT,
				PRIMARY KEY (team, member)
			);

			INSERT INTO memberships (team, member, level) VALUES ('red', 'Alice', '3'), ('blue', 'Bob', '1');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level INTEGER,
				PRIMARY KEY (team, member)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_memberships_temp",
					Columns: []*SQLiteColumn{
						{
							Name:    "team",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "member",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name: "level",
							Type: "INTEGER",
						},
					},
					PrimaryKey: []string{"team", "member"},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member", "level"},
					SelectExpressions: []string{`"team"`, `"member"`, `"level"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "blue", "member": "Bob", "level": int64(1)},
			{"team": "red", "member": "Alice", "level": int64(3)},
		}, rows)
	})

	t.Run("ModifyCompositePrimaryKeyKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member, team)
			);

			INSERT INTO memberships (team, member) VALUES ('red', 'Alice');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member COLLATE NOCASE, team DESC)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_memberships_temp",
					Columns: []*SQLiteColumn{
						{
							Name:    "team",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "member",
							Type:    "TEXT",
							NotNull: true,
						},
					},
					PrimaryKey:     []string{"member", "team"},
					PrimaryKeyKeys: []string{`"member" COLLATE NOCASE`, `"team" DESC`},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member"},
					SelectExpressions: []string{`"team"`, `"member"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice"},
		}, rows)

		driver.RequireInstructions(nil)
	})

	t.Run("ModifyCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, role)
			);

			INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, member)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_memberships_temp",
					Columns: []*SQLiteColumn{
						{
							Name:    "team",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "member",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "role",
							Type:    "TEXT",
							NotNull: true,
						},
					},
					PrimaryKey: []string{"team", "member"},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member", "role"},
					SelectExpressions: []string{`"team"`, `"member"`, `"role"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice", "role": "lead"},
		}, rows)
	})

	t.Run("RecreateTableWithPrimaryKeyAndIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (email, age) VALUES ('alice@example.com', '30'), ('bob@example.com', '25');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("users",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_users_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "email",
							Type:       "TEXT",
							PrimaryKey: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"email", "age"},
					SelectExpressions: []string{`"email"`, `"age"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
				&SQLiteCreateIndexInstruction{
					Name:      "idx_users_age",
					TableName: "users",
					Keys:      []string{`"age"`},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY email")

		require.Equal(t, []map[string]any{
			{"email": "alice@example.com", "age": int64(30)},
			{"email": "bob@example.com", "age": int64(25)},
		}, rows)
	})

	t.Run("TableNameThatDiffersInCaseOnlyIsTheSameTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE Users (id INTEGER PRIMARY KEY, c TEXT);
			CREATE INDEX users_c ON Users (c);

			INSERT INTO Users (id, c) VALUES (1, 'x');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, c TEXT, d TEXT);
			CREATE INDEX users_c ON users (c);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "d",
						Type: "TEXT",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "c": "x", "d": nil},
		}, rows)
	})
}
