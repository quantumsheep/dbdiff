# dbdiff - Agent Context

## What dbdiff is

dbdiff is a command line tool in Go. It reads the schema of two databases. Then it prints
the SQL statements that make the target schema equal to the source schema.

The command takes two arguments. The first argument is the source. The second argument is
the target. The source holds the wanted state. The output changes the target.

dbdiff supports SQLite and PostgreSQL. It compares schemas only. It does not compare data.

## The rules that matter most

Read this section before you write any code.

1. **Write for a human reader.** Another person must understand the code on the first
   read. Use full words in names. Keep the format that `gofmt` produces.
2. **Let the code explain itself.** A good name and a small function replace a comment.
   Write a comment only when the logic stays unclear without it. That case is rare. Never
   write a comment that repeats the code.
3. **Keep the file architecture.** One file holds one schema object of one engine. See
   [Repo layout](#repo-layout).
4. **Copy the diff algorithm of the drivers that exist.** Every driver walks the source
   objects first and the target objects second. See [How a diff works](#how-a-diff-works).
5. **Test every change of a driver.** Each new behavior gets a subtest. See
   [Tests](#tests).
6. **Run the generated SQL in the test.** A test that only compares strings proves
   nothing. See [Tests](#tests).
7. **Read the documentation of a library before you use it.** Your training data is older
   than the versions in `go.mod`. See [Library documentation](#library-documentation).
8. **Write in Simplified Technical English (ASD-STE100).** This rule covers code comments,
   commit messages, documentation, CLI help text, and error messages. See
   [Writing style](#writing-style).
9. **Delegate to subagents.** Split each task into scoped subtasks. Give each subagent a
   clean context. A small context gives a better result.

## Library documentation

Read the documentation of a library before you write code against it. Read the
documentation of the version in `go.mod`, never the version that you remember. A major
version renames functions and moves types.

| Library                   | Version | Where the documentation is                     |
| ------------------------- | ------- | ---------------------------------------------- |
| `github.com/urfave/cli`   | v3.6.1  | `go doc github.com/urfave/cli/v3`              |
| `github.com/jackc/pgx`    | v5.8.0  | `go doc github.com/jackc/pgx/v5/stdlib`        |
| `github.com/mattn/go-sqlite3` | v1.14.32 | `go doc github.com/mattn/go-sqlite3`      |
| `github.com/samber/lo`    | v1.52.0 | `go doc github.com/samber/lo`                  |
| `github.com/stretchr/testify` | v1.11.1 | `go doc github.com/stretchr/testify/require` |

**urfave/cli v3 is not the urfave/cli that you remember.** Version 3 replaces `cli.App`
with `cli.Command`. It passes a `context.Context` to every action. It reads a positional
argument with `cmd.StringArg`.

## Repo layout

```
dbdiff/
├── cmd/dbdiff/          main.go parses the flags and selects one driver. main_test.go
│                        builds the binary and checks the output and the exit code.
├── drivers/             One package. It holds every driver and every schema model.
├── docker-compose.yml   The PostgreSQL service for the tests
└── .github/workflows/   test.yaml (build and test) and tag.yaml (release)
```

The `drivers` package uses one file per schema object per engine:

```
drivers/
├── driver.go                  The Driver interface
├── identifier.go              quoteIdentifier and quoteIdentifiers
├── sqlite.go                  The SQLite driver: connections, queries, top-level diff
├── sqlite_table.go            SQLiteTable and its diff
├── sqlite_column.go           SQLiteColumn
├── sqlite_index.go            SQLiteIndex
├── sqlite_trigger.go          SQLiteTrigger
├── sqlite_view.go             SQLiteView
├── sqlite_foreign_key.go      SQLiteForeignKey
├── sqlite_test.go             The SQLite test harness and the tests
├── postgres.go                The PostgreSQL driver
├── postgres_table.go          PostgresTable and its diff
├── postgres_column.go         PostgresColumn
├── postgres_index.go          PostgresIndex
├── postgres_constraint.go     PostgresConstraint
├── postgres_trigger.go        PostgresTrigger
├── postgres_view.go           PostgresView
├── postgres_sequence.go       PostgresSequence
├── postgres_type.go           PostgresType, an enum type
├── postgres_function.go       PostgresFunction
├── postgres_extension.go      PostgresExtension
└── postgres_test.go           The PostgreSQL test harness and the tests
```

Do not add a package. Add a file to `drivers/` with the name `<engine>_<object>.go`.

## Build and run

```bash
go build -o ./bin/dbdiff ./cmd/dbdiff
go run ./cmd/dbdiff <source> <target>
go run ./cmd/dbdiff --driver postgres <source> <target>
```

The `--driver` flag accepts `sqlite3` and `postgres`. The default value is `sqlite3`.

The SQLite driver needs CGO, because `go-sqlite3` is a C binding. If a build fails with an
undefined symbol, set `CGO_ENABLED=1` before the build.

---

# Drivers

## The Driver interface

```go
type Driver interface {
	Close() error
	Diff(ctx context.Context) (string, error)
}
```

A driver holds two `*sql.DB` fields: `SourceDatabaseConnection` and
`TargetDatabaseConnection`. A constructor is `New<Engine>Driver(config *<Engine>DriverConfig)`.
The config struct holds the two connection strings. To register a new driver, add a case
to the switch in `cmd/dbdiff/main.go` and a value to the flag validator.

## Naming

| Element         | Pattern                            | Example                       |
| --------------- | ---------------------------------- | ----------------------------- |
| Model type      | `<Engine><Object>`                 | `SQLiteTable`, `PostgresView` |
| Read method     | `Get<Object>s(ctx, db, ...)`       | `GetTableColumns`             |
| Compare method  | `Diff<Object>s(other)`             | `DiffIndexes`                 |
| Lookup method   | `<Object>ByName(name) (*T, bool)`  | `ColumnByName`                |
| Equality method | `Equal(other *T) bool`             | `SQLiteIndex.Equal`           |
| SQL method      | `String() string`                  | `SQLiteColumn.String`         |

`HasEqualAttributes` compares two objects and ignores the name. The rename detection of
SQLite uses this method.

## How a diff works

Every diff function follows the same three steps. The first step reads the objects of the
source and the objects of the target. The second step walks the source objects. The third
step walks the target objects.

In the second step, the function looks for the target object with the same name. When the
name is absent, the function prints a `CREATE` statement. When the object exists and
differs, the function prints an `ALTER` statement, or a `DROP` statement and a `CREATE`
statement.

In the third step, the function prints a `DROP` statement for each target object that the
source does not hold.

Rules for the SQL output:

- Build the output with a `strings.Builder` and `fmt.Fprintf`.
- Return `strings.TrimSpace(diff.String())` at the end of the function.
- Pass every identifier through `quoteIdentifier`: `DROP TABLE %s;`. Never write
  `\"%s\"` in a format string. A name can hold a space or a double quote.
- End every statement with a semicolon and a newline.
- Print the additions and the modifications first. Print the removals last.
- `String()` returns the complete statement that creates the object.

Use `lo.Find`, `lo.Map`, and `lo.Values` from `samber/lo` for the list operations.

## SQLite driver

The SQLite driver reads the schema with `PRAGMA` statements and with `sqlite_master`:

| Data          | Query                                                     |
| ------------- | --------------------------------------------------------- |
| Tables        | `SELECT name FROM sqlite_master WHERE type='table'`        |
| Columns       | `PRAGMA table_info(<table>)`                               |
| Indexes       | `PRAGMA index_list(<table>)`, `PRAGMA index_info(<index>)` |
| Foreign keys  | `PRAGMA foreign_key_list(<table>)`                         |
| Triggers      | `SELECT name, sql FROM sqlite_master WHERE type='trigger'` |
| Views         | `SELECT name, sql FROM sqlite_master WHERE type='view'`    |

A `PRAGMA` statement takes no placeholder. The driver puts the name through
`quoteIdentifier` before it joins the name into the statement.

`NewSQLiteDriver` removes the `sqlite://` prefix from each path.

SQLite does not support `ALTER COLUMN`. `SQLiteTable.DiffTable` recreates the table when a
column changes, or when a foreign key changes. The recreation prints five parts in this
order:

1. `CREATE TABLE "_<name>_temp"` with the new columns and the new foreign keys.
2. `INSERT INTO "_<name>_temp" (...) SELECT ... FROM "<name>"` with an explicit column
   map. A new column takes its `DEFAULT` value, or `NULL`.
3. `DROP TABLE "<name>"`.
4. `ALTER TABLE "_<name>_temp" RENAME TO "<name>"`.
5. One `CREATE INDEX` statement for each index of the table.

`DiffColumns` detects a rename. A source column that the target does not hold, and that
holds the attributes of exactly one free target column, is a rename. Two candidates make
the guess unsafe. In that case the column becomes an addition, and the old columns become
removals. A target column that another rename holds is not a candidate.
`IsTypeChangeCompatible`
holds the type list that a recreation can convert: `TEXT`, `INTEGER`, `REAL`, and `BLOB`.
An incompatible type change becomes a `DROP COLUMN` statement and an `ADD COLUMN`
statement.

`GetTableForeignKeys` sorts the foreign keys with `sort.SliceStable`, because SQLite gives
no stable order. Keep that sort. Without it the output changes between two runs.

## PostgreSQL driver

The PostgreSQL driver connects through `pgx/v5/stdlib` with the driver name `pgx`. It
reads the schema of `current_schema()` only. The search path of the connection string
selects the schema.

| Data        | Source                                                   |
| ----------- | -------------------------------------------------------- |
| Tables      | `information_schema.tables`                              |
| Columns     | `information_schema.columns`                             |
| Views       | `information_schema.views`                               |
| Constraints | `pg_constraint` with `pg_get_constraintdef(oid)`         |
| Indexes     | `pg_indexes`, without the indexes of a constraint        |
| Triggers    | `pg_trigger` with `pg_get_triggerdef(oid)`               |
| Sequences   | `pg_sequences`, without the sequence that a column owns  |
| Enum types  | `pg_type` with `pg_enum`, in the order of `enumsortorder` |
| Functions   | `pg_proc` with `pg_get_functiondef(oid)`                 |
| Extensions  | `pg_extension`                                           |

`Diff` prints six sections in this order: extensions, enum types, sequences, functions,
tables, views. A table can use each of the first four, so these come first. Keep that
order when you add a section.

Three rules keep the output free of noise:

- An object that an extension owns stays out of the diff. Each query excludes a row with
  a `pg_depend` entry of the type `e`. The `CREATE EXTENSION` statement recreates it.
- A `SERIAL` column and an identity column own their sequence. `GetSequences` excludes a
  sequence with a `pg_depend` entry of the type `a` or `i`.
- `pg_get_functiondef` writes the name of the schema in the header. `GetFunctions`
  removes that prefix, because the source schema and the target schema differ.

`PostgresFunction.Signature` joins the name and the identity arguments. PostgreSQL accepts
several functions with one name, so the name alone identifies nothing.

`PostgresSequence.Diff` returns one `ALTER SEQUENCE` statement with every attribute that
changes. Separate statements fail, because a new minimum above the current value is
invalid.

`PostgresType.Diff` prints `ALTER TYPE ... ADD VALUE` when the target values are the first
values of the source. Every other change prints `DROP TYPE` and `CREATE TYPE`.

PostgreSQL supports `ALTER TABLE ALTER COLUMN`. The driver prints one statement per change
of a type, of a `NOT NULL` flag, or of a default value. A modified constraint, index, or
trigger becomes a `DROP` statement and a `CREATE` statement.

An index and a trigger keep the definition text that PostgreSQL returns. `String()` adds
the semicolon.

A query that casts a name to `regclass` takes the name from `quoteIdentifier`. A query
that compares a name to a text column takes the raw name. `GetTable` passes both forms to
the index query.

---

# Tests

Every test lives beside the code, in `drivers/<engine>_test.go` and in
`cmd/dbdiff/main_test.go`. There is no `tests` folder. The tests use
`github.com/stretchr/testify/require`.

## Run the tests

```bash
docker compose up -d    # PostgreSQL on port 5432, needed by the PostgreSQL tests
go test ./...
go test -run TestSQLiteDriver/RenameColumn ./drivers
```

The PostgreSQL tests need the database at
`postgres://user:password@localhost:5432/dbdiff`. The SQLite tests need no service,
because each one writes into `tb.TempDir()`.

## Structure

One engine gets one top-level function: `TestSQLiteDriver` or `TestPostgresDriver`. Each
behavior gets one `t.Run` subtest. The subtest name is a short noun phrase in CamelCase,
for example `AddColumn`, `ModifyIndexes`, or `ForeignKeys`.

Each engine has a test harness type. `TestingSQLiteDriver` and `TestingPostgresDriver`
embed the driver and hold the `testing.TB` value. The constructor builds the databases and
registers the cleanup with `tb.Cleanup`.

The PostgreSQL harness creates one source schema and one target schema in one database. It
drops both in the cleanup, and it checks the error of each drop with `require.NoError`. A
cleanup that fails without a check leaves a schema in the database of every run. Keep the
connection of the harness open until the cleanup ends.

| Helper                          | Purpose                                          |
| ------------------------------- | ------------------------------------------------ |
| `ExecOnSource(sql)`             | Builds the wanted schema                         |
| `ExecOnTarget(sql)`             | Builds the old schema, or applies the diff       |
| `RequireDiff(expected)`         | Compares the whole diff and returns it           |
| `FetchAllFromTarget(table, ...)`| Reads the rows of the target as maps (SQLite)    |

Every harness method calls `d.tb.Helper()` on its first line.

## How to write a test

1. Create the harness with `NewTestSQLiteDriver(t)` or `NewTestPostgresDriver(t)`.
2. Build the wanted schema with `ExecOnSource`.
3. Build the old schema with `ExecOnTarget`. Insert rows when the change moves data.
4. Compare the whole output with `RequireDiff`. Write the expected SQL as a raw string.
5. Apply the diff with `driver.ExecOnTarget(diff)`. This step proves that the SQL runs.
6. If the change moves data, read the rows with `FetchAllFromTarget` and compare them with
   `require.Equal`.

Rules:

- Compare the whole diff string. Never compare one line, and never use `strings.Contains`.
- Always apply the diff to the target after the comparison.
- A test that recreates a table must insert rows first, and must compare the rows after.
- Add a subtest for each new schema object and for each new kind of change.

## CLI tests

`TestDbdiffCommand` covers the binary. `buildDbdiff` compiles the command into
`tb.TempDir()` one time. `runDbdiff` runs the binary and returns the standard output, the
standard error, and the exit code. Add a subtest for each new flag and for each new error
of the command.

---

# Go conventions

- Run `gofmt -w .` and `go vet ./...` after every change. Correct every finding. The
  `lint` job of the CI runs `gofmt -l .` and fails on a file that it reports.
- Use tabs for the indentation. `gofmt` writes them.
- Name a variable with full words: `sourceDatabaseConnection`, not `srcConn`.
- Return an error to the caller. Never call `os.Exit` or `panic` in the `drivers` package.
- Wrap an error with context in `cmd/dbdiff/main.go`: `fmt.Errorf("failed to ...: %w", err)`.
- Close every `*sql.Rows` with `defer rows.Close()`. After the `Next` loop, return the
  error of `rows.Err()`. Without that check the driver reads a truncated schema.
- The CLI prints the error and exits with the code 1. `main` owns that, because
  `cmd.Run` returns the error to the caller.
- Pass the `context.Context` to every query with `QueryContext` or `ExecContext`.
- Use a placeholder for a value: `?` for SQLite and `$1` for PostgreSQL.
- The `drivers` package exports its types and its methods, because the tests and the CLI
  use them.

## Blocks

An `if`, a `for`, a `switch`, and a `select` are blocks. Two rules cover every block.

**Never initialize a variable in the header of a block.** Write the assignment on the line
above the block. Write `err := f()` and `if err != nil {` on two lines. Never write
`if err := f(); err != nil {`. When the scope holds an `err` variable already, write
`err = f()`.

**Put a blank line before a block and after a block.** Three cases take no blank line:

- The assignment that the condition checks belongs to the block. It stays on the line
  directly above the block, and the blank line goes before that assignment. A comment
  above the block belongs to the block in the same way.
- A block that opens a body takes no blank line above it.
- A block that closes a body takes no blank line below it. An `else` keeps `} else {` on
  one line, with no blank line above it and none below it.

```go
func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table';")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var tables []*SQLiteTable

	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		table, err := d.GetTable(ctx, db, tableName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return tables, nil
}
```

# Writing style

Write every text in ASD-STE100 Simplified Technical English. This rule covers code
comments, commit messages, documentation, CLI help text, and error messages. The standard
is a free download at asd-ste100.org.

- Write an instruction in the imperative. Use one instruction per sentence, and 20 words
  maximum.
- Write a description in the simple present, simple past, or simple future. Use 25 words
  maximum per sentence.
- Use the active voice. Name the actor.
- Use only these modal verbs: can, will, must. Do not use should, would, may, might, or
  could.
- Put the condition first, before the command: "If the build fails, read the log".
- Use one name for one item. Do not write "config" in one place and "settings" in another.
- Do not use a semicolon. Write two sentences instead.
- Keep the articles and the conjunction "that". Do not use a contraction.
- Delete a filler word: simply, just, seamlessly, robust, comprehensive, powerful.
- Write "for example" for "e.g." and "that is" for "i.e.". Do not write "etc.".
- Never use an em dash. Use a regular dash instead.
- Do not change code, identifiers, commands, file paths, or a quoted error message.

# Git

- Do not run `git commit` unless the user asks for it.
- Use Conventional Commits with a scope: `feat(sqlite): add the view diff`.
- The scopes of this repo are `sqlite`, `postgres`, `drivers`, `cli`, `ci`, and `docs`.
  Omit the scope for a change that crosses several parts.
- Mark a breaking change with `!`: `feat(cli)!: rename the driver flag`.
- Write the subject in lowercase, in the imperative, with no final period.
- Update the support table of `README.md` in the same commit as the feature.
- A tag with the form `v*.*.*` starts the release workflow. It builds five binaries and
  publishes them.

# Delegation

This section speaks to the agent that answers the user. That agent is the parent.

**If you are a subagent, stop here. Do the work yourself. Never spawn another subagent.
Return your result to the parent.**

## How the parent delegates

Split a large task across subagents. A clean context gives a better result.

- Spawn a subagent to isolate context, to run independent work in parallel, or to do bulk
  mechanical work.
- Keep the work in the parent when the parent needs the reasoning, or when the synthesis
  needs every part at the same time.
- Pick the cheapest model that does the subtask well. Haiku does mechanical work. Sonnet
  does scoped research and code exploration. Opus does planning and trade-offs.
- The parent owns the final answer.

## Parallel writes

Two agents that write to one file will corrupt the work of each other. These rules keep
parallel work safe on one checkout:

- Give each subagent a file list that no other subagent shares. The file split of
  `drivers/` makes this easy. One agent takes `sqlite_index.go`. Another agent takes
  `sqlite_view.go`.
- Both engines write into one test file each. Two agents must never edit
  `drivers/sqlite_test.go` at the same time.
- A subagent must never run `git checkout`, `git reset`, `git stash`, `git add`, or
  `git commit`. If a subagent finds work outside its scope, it reports the work. It never
  reverts the work.
- For a bulk edit, ask each subagent for its decisions. Then apply every decision
  yourself, in one place.

## Before you change the working tree

Run `ListAgents` before any command that changes shared state. This covers `git checkout`,
`git reset`, `git stash`, `gofmt -w .`, and a bulk apply script.

If an agent still runs, stop it with `TaskStop`. Then confirm that the state is `killed`.

A subagent reads the version of this file from the start of the session. If you change a
rule here during a session, repeat that rule in the spawn prompt.

# Known gaps

This section records the state of the repo on 2026-08-16. It is not a rule set. Correct an
item when your task touches it.

- Neither driver compares data. The `Data` column of the `README.md` table is `❌` for
  every engine.
- The diff prints no dependency order for a removal. A `DROP TYPE` statement comes before
  the `DROP TABLE` statement of the table that uses the type, and PostgreSQL refuses it.
- A modified function takes `CREATE OR REPLACE FUNCTION`. PostgreSQL refuses that
  statement when the return type changes. The diff prints no `DROP FUNCTION` statement
  before it.
- One database holds one extension one time. The test of `CREATE EXTENSION` compares the
  diff, and it applies no diff, because the source schema and the target schema share the
  database.
- `ALTER SEQUENCE` fails when the new minimum is above the current value. The driver
  prints no `RESTART` clause, because a restart changes data.
- The PostgreSQL driver compares no aggregate, no operator, no domain, and no composite
  type. `GetTypes` reads an enum type only.
- The SQLite driver reads no partial index and no index that an expression builds.
  `SQLiteIndex` holds a column list only.
- The PostgreSQL driver reads the schema of `current_schema()` only. A connection string
  without a search path reads the `public` schema.
- A type change of PostgreSQL prints `ALTER COLUMN ... TYPE` without a `USING` clause.
  PostgreSQL refuses the statement when it holds no automatic cast.
- The row-iteration errors of `rows.Err()` reach the caller, but no test covers that path.
  A test needs a connection that fails in the middle of a read.
