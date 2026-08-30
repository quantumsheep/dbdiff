# dbdiff - Agent Context

## What dbdiff is

dbdiff is a command line tool in Go. It reads the schema of two databases and prints the SQL statements that make the source schema equal to the target schema. The source holds the current schema that the output changes. The target holds the wanted schema. Every diff walks the target objects first, and it prints the statements that change the source. dbdiff supports SQLite, PostgreSQL, and MySQL with MariaDB. An argument names a database, a `.sql` file, or a directory of `.sql` files.

## The rules that matter most

Read this section before you write any code.

1. **Write for a human reader.** Use full words in names. Keep the format that `gofmt` produces.
2. **Let the code explain itself.** Write no comment by default. A comment stays only when its removal lets a reader break the code. See [Comments](#comments).
3. **Keep the file architecture.** One file holds one schema object of one engine. See [Repo layout](#repo-layout).
4. **Copy the diff algorithm of the drivers that exist.** Every driver walks the target objects first and the source objects second. See [How a diff works](#how-a-diff-works).
5. **Test every change of a driver.** Each new behavior gets a subtest. See [Tests](#tests).
6. **Run the generated SQL in the test.** A test that only compares strings proves nothing.
7. **Read the documentation of a library before you use it.** Read `go.mod` for the current version. Then read that version with `go doc <import path>`, never the version that you remember.
8. **Write in Simplified Technical English (ASD-STE100).** This rule covers code comments, commit messages, documentation, CLI help text, and error messages. See [Writing style](#writing-style).
9. **Delegate to subagents.** Split each task into scoped subtasks. Give each subagent a clean context. See [Delegation](#delegation).
10. **Read the neighbor files before you change a driver.** The code comments hold the engine rules and the order constraints that this file does not repeat.

## Repo layout

| Path                              | Content                                                                                              |
| --------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `cmd/dbdiff/`                     | `main.go` builds the root command. `main_test.go` runs the binary.                                   |
| `cmd/dbdiff/cmd/`                 | One package per command: `<command>/command.go`, with a `command_test.go` beside it.                 |
| `cmd/dbdiff/internal/helpers/`    | Version and the usage error handler                                                                  |
| `cmd/dbdiff/internal/migrations/` | The flag and configuration reading of the eight migrate commands                                     |
| `cmd/dbdiff/internal/clitest/`    | The helpers that build the binary and run it                                                         |
| `internal/sqltest/`               | The writer of a `.sql` file, shared by the driver tests, the migrations tests, and `clitest`         |
| `internal/drivers/`               | One file, `new_driver.go`. It holds `NewDriver`, the one driver switch of the program.               |
| `internal/drivers/shared/`        | The engine-free core: `Driver`, the `SQL` instruction types, quoting, the SQL sources, the statement splitter, the comment builders, `DetectDriver`, `DiffByKey`, and `DiffData` |
| `internal/drivers/sqlite/`        | The SQLite driver: the schema models, the `SQLite` instruction types, and the data comparison        |
| `internal/drivers/postgres/`      | The PostgreSQL driver, with the same shape                                                           |
| `internal/drivers/mysql/`         | The MySQL driver, with the same shape. It covers MariaDB too.                                        |
| `internal/migrations/`            | The model of a migration and the migrator of each engine. It knows no command and no flag.           |
| `drivers/`                        | The public package that runs a diff from a Go program. Its types stay stable.                        |
| `migrations/`                     | The public package that applies the migrations from a Go program. Its types stay stable.             |

An engine package uses one file per schema object, with the name `<engine>_<object>.go`, for example `sqlite/sqlite_index.go`. The instruction types live in its `instruction_*.go` files. Each engine package holds one main test file, `<engine>_test.go`.

Do not add a package for a schema object. Add a file to the engine package. A new engine adds one package under `internal/drivers/`, one case to `NewDriver`, one case to `NewMigrator`, one name to `SupportedDriverNames`, and one branch to `DetectDriver`.

A subpackage of `internal/drivers` never imports its parent. `driverssqlite`, `driverspostgres`, and `driversmysql` import `driversshared` only.

## Package names

A command package takes the name of its path, with no separator: `cmd/dbdiff/cmd/diff` holds `package cmddiff`, and `cmd/dbdiff/cmd/migrate/cmd/generate` holds `package cmdmigrategenerate`. The driver packages follow the same rule: `driversshared`, `driverssqlite`, `driverspostgres`, and `driversmysql`. The name differs from the directory, so every import writes the name:

```go
driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
```

Each package of `cmd/dbdiff/internal` keeps its plain name: `migrations`, `helpers`, and `clitest`. A file that imports both members of a name pair writes a prefix on one of them:

| Import path                      | Name in a file that imports both members of the pair |
| -------------------------------- | ---------------------------------------------------- |
| `internal/drivers`               | `drivers`                                            |
| `drivers`                        | `dbdiffdrivers`                                      |
| `cmd/dbdiff/internal/migrations` | `migrations`                                         |
| `internal/migrations`            | `coremigrations`                                     |

## Build and run

```bash
go build -o ./bin/dbdiff ./cmd/dbdiff
go run ./cmd/dbdiff diff <source> <target>
```

The `dbdiff` command holds `--driver` and `--schema`, with `Local` false, so every subcommand reads them. Every other flag belongs to the command that reads it:

| Command                | Flags                                                 |
| ---------------------- | ----------------------------------------------------- |
| `diff`                 | `--privileges`, `--comments`, `--data`, `--exit-code` |
| Each `migrate` command | `--config`, `--source`, `--target`                    |
| `migrate preview`      | The three flags above, and `--run`                    |
| `migrate up`           | The three flags above, and `--to`                     |
| `migrate baseline`     | The three flags above, and `--to`                     |

Each subcommand of `migrate` writes its flags in its own `Command` function, because each command needs its own flag instances. A change of the text of one of the three migration flags touches each of the eight files.

The SQLite driver needs CGO. If a build fails with an undefined symbol, set `CGO_ENABLED=1` before the build.

---

# Drivers

## The Driver interface

```go
type Driver interface {
	Close() error
	Diff(ctx context.Context) ([]Instruction, error)
}
```

A driver holds two `*sql.DB` fields: `SourceDatabaseConnection` and `TargetDatabaseConnection`. A constructor is `New<Engine>Driver(ctx context.Context, config *<Engine>DriverConfig)`. `OpenSide(ctx, path, ...)` reads a database, or it builds a temporary database from a SQL source. `Close` releases the temporary database.

`DetectDriver` in `internal/drivers/shared/driver_detection.go` names the engine when the user gives no `--driver` flag. A SQL source takes the engine of the other side. Two empty or two different names give an error that names the `--driver` flag.

## Naming

| Element           | Pattern                                                           | Example                            |
| ----------------- | ----------------------------------------------------------------- | ---------------------------------- |
| Model type        | `<Engine><Object>`                                                | `SQLiteTable`, `PostgresView`      |
| Read method       | `Get<Object>s(ctx, db, ...)`                                      | `GetTableColumns`                  |
| Compare method    | `Diff<Object>s(other)`                                            | `DiffIndexes`                      |
| Lookup method     | `<Object>ByName(name) (*T, bool)`                                 | `ColumnByName`                     |
| Equality method   | `Equal(other *T) bool`                                            | `SQLiteIndex.Equal`                |
| Statement method  | `String() string`                                                 | `SQLUpdateInstruction.String`      |
| Fragment method   | `Definition()` or `Clause()`                                      | `SQLiteColumn.Definition`          |
| Instruction list  | `Instructions() []Instruction`                                    | `SQLiteTable.Instructions`         |
| Instruction maker | `CreateInstruction()`, `DropInstruction()`, `UpdateInstruction()` | `PostgresDomain.CreateInstruction` |
| Instruction type  | `<Prefix><Statement>Instruction`                                  | `SQLUpdateInstruction`             |
| Action type       | `<Prefix><Action>Action`                                          | `PostgresSetNotNullAction`         |

`HasEqualAttributes` compares two objects and ignores the name. The rename detection of SQLite uses this method.

## How a diff works

Every diff function follows three steps. Read the objects of both sides. Walk the target objects: print a `CREATE` statement for an absent name, and an `ALTER` statement or a `DROP` and `CREATE` pair for a changed object. Walk the source objects and print a `DROP` statement for each object that the target does not hold.

The shared generic `DiffByKey` in `internal/drivers/shared/diff_by_key.go` implements these steps once, with a `DiffRules` value per object. An engine writes its own three steps only where the object needs more than a key match.

Rules for the output:

- Build the output as `[]Instruction`. Append a value, and never format a string.
- Return the list at the end of the function. A section returns a `*SectionDiff`.
- An instruction type holds one field for each part of its PostgreSQL synopsis. The type quotes every identifier, so no call site holds a format string.
- Print the additions and the modifications first. Print the removals last.

Use `lo.Find`, `lo.Map`, and `lo.Values` from `samber/lo` for the list operations.

## Instructions

An `Instruction` is one complete SQL statement, or one comment line:

```go
type Instruction interface {
	String() string
	Comment() string
}
```

`Comment` names the object that the statement changes, for example `Create the table "users"`. A `Comment` method calls one of the builders of `internal/drivers/shared/instruction_comment.go`, so it stays one line. An instruction that holds a comment text names the field `Text`, because a field named `Comment` cannot live beside the method.

A new instruction type takes one `Comment` method below its `String` method, one subtest in `instruction_test.go`, and one subtest in `instruction_comment_test.go` of its package. Those files hold the SQL text of each statement. A driver test compares instructions, and it trusts those files.

In the driver packages, `String() string` belongs to an instruction type only. A model type answers with an instruction, with a fragment, or with a list of instructions. This command finds a violation, so it must return nothing:

```bash
rg -n 'func \([a-z][A-Za-z0-9]* \*?[A-Za-z]+\) String\(\) string' internal/drivers/ --glob '!**/instruction*.go'
```

A nested part renders through a method that names what the method returns, for example `TableActionClause()`. Each part interface takes a different method name, so no part satisfies another part.

The prefix of a type name reports the portability of the syntax: `SQL` for both engines, `SQLite` and `Postgres` for a syntax that one engine holds or writes differently.

## The SQLite driver

The driver reads the schema with `PRAGMA` statements and `sqlite_master`. A `PRAGMA` statement takes no placeholder, so the driver puts every name through `QuoteIdentifier`. SQLite holds no `ALTER COLUMN`, so a changed column, foreign key, or table constraint recreates the table. The driver also detects a renamed column through `HasEqualAttributes`, with exactly one candidate.

SQLite gives no stable order for the tables and the foreign keys, so the driver sorts them with a stable sort. Keep every such sort. Without it the output changes between two runs. The code comments of the package hold the other engine rules.

## The PostgreSQL driver

The driver connects through `pgx/v5/stdlib` and reads the `pg_catalog` tables. Give every catalog query a stable `ORDER BY`, because PostgreSQL gives no stable order. `Diff` verifies first that the schema exists on both sides. Without that check an absent schema reads as an empty schema, and the diff drops every object of the source.

`Diff` prints twelve sections in a dependency order. Each `Diff<Object>s` function returns a `SectionDiff` with `EarlyRemovals`, `Additions`, and `Removals`. `Diff` prints every `EarlyRemovals` in the REVERSE section order, then every `Additions` in the section order, then every `Removals` in the REVERSE section order. A new section needs no other work to get a correct order.

Three rules keep the output free of noise:

- An object that an extension owns stays out of the diff.
- A sequence that a `SERIAL` column or an identity column owns stays out of the diff.
- A query that reads a definition text removes the prefix of `current_schema()`. Without that step the diff drops and recreates an object that did not change. Keep that step in every new query that reads a definition text.

An object with no `ALTER` action, for example a policy or a rule, prints a `DROP` and `CREATE` pair, and that pair stays one modification in `Additions`. The code comments of the package hold the order rules of the tables section.

## The MySQL driver

The driver reads the information_schema tables with the connected database as the schema, and it covers MySQL and MariaDB. Give every catalog query a stable `ORDER BY`. The driver quotes an identifier with its own `QuoteIdentifier`, because MySQL uses the backtick. The shared instruction types quote with the double quote, so the mysql package holds its own instruction types.

The driver reads `SELECT VERSION()` per side and branches on MariaDB where the catalog differs: the STATISTICS table of MariaDB holds no EXPRESSION column, the CHECK_CONSTRAINTS table of MariaDB holds the table name, MariaDB stores every column default as an expression, and the sequences exist on MariaDB only. MySQL escapes a quote and a backslash in a catalog expression text, and `unescapeCatalogText` removes that escape.

MySQL gives no creation order for the tables, so a diff that creates or drops a table with a foreign key, or that adds a key to a table that another instruction creates, wraps the output in a `SET FOREIGN_KEY_CHECKS` pair. MySQL builds an index with the name of a foreign key, so the table creation and the index additions skip a CREATE INDEX statement with that name. The code comments of the package hold the other engine rules and the order rules of `DiffTable`.

`Diff` prints the sections in this order: sequences, tables with their triggers, routines, views, events, privileges, data. A routine, an event, and the partition clause of a table come from `SHOW CREATE`, with the DEFINER clause and the final semicolon removed. The event comparison also removes the STARTS clause, because MySQL writes the creation time into an event without one. The engine and the collation of a table compare through a default flag, so two databases with two different defaults compare as equal. The data comparison gives the shared `DiffData` a backtick `Quote` hook and its own statement types, because the shared types quote with the double quote.

## SQL sources

An argument names SQL text when the path ends in `.sql`, or when the path is a directory. A path that holds `://` never names SQL text. A directory gives its top-level `.sql` files in the order of the names, without the `.down.sql` files.

Each engine materializes a source as a temporary database: SQLite as a file, PostgreSQL as a scratch server from `fergusstrange/embedded-postgres`, and MySQL as a scratch database on the server of the other side. The mysql driver refuses two SQL sources, because it then holds no server. The scratch server takes the major version of the database of the other side, or the `version` key of `dbdiff.yaml`. Do not name a version in the code. Four settings of the scratch server matter. Keep them:

- The logger is `io.Discard`, because the default logger writes to the stream that holds the diff.
- The port comes from a free-port lookup, because a real server holds the default port 5432.
- `Version` stays absent for an empty version, because an empty value gives an invalid configuration.
- `BinariesPath` names a stable cache directory whose name holds the version. Without it every run extracts the archive again.

## The no-transaction directive

A file that holds no directive runs in ONE call. A file that holds the line `-- dbdiff:no-transaction`, or the line `-- atlas:txmode none` of Atlas, runs one call for each statement. The directive is the one signal that starts the split. Never read the SQL text to make that decision.

`internal/drivers/shared/sql_statement.go` holds the directive and the statement splitter, because the replay of a SQL source and the apply of a migration both read them. The splitter understands comments, strings, quoted identifiers, and dollar quotes.

## Migrations

`Migrator` in `internal/migrations` is a sibling of `Driver`, not a wrapper of it. `internal/migrations` imports `drivers`, and `drivers` imports nothing of it. The constant `MigrationHistoryTableName` stays in `drivers`, because the diff hides that table. The `migrate` commands give the word `source` to the migration directory, and the word `target` to the other side.

A migration entry holds one of five states: `pending`, `applied`, `changed` (checksum differs), `missing` (a row without a file), and `out of order` (no row, and the version sorts before the last applied version).

Three behavior rules. Keep them:

- `status`, `preview`, and `verify` report an out of order file. `up` and `step` refuse it, because applying it breaks the version order.
- The preview applies every pending file in ONE transaction and rolls it back at the end, because a file reads the objects of the files before it.
- `up` and `step` commit one file at a time, and they roll back no earlier file. The one difference: `step` asks a question before each file.

## Data comparison

The `--data` flag sets the `CompareData` field of the driver config. The schema output stays the same in every case. `driversshared.DiffData` in `internal/drivers/shared/data_diff.go` holds the comparison, and each engine gives it a `DataRules` value. The data section prints after the whole schema section. Format each value as an SQL literal before the comparison, so `NULL` never equals the text `'NULL'`.

---

# Tests

Every test lives beside the code. There is no `tests` folder. The tests use `github.com/stretchr/testify/require`.

## Run the tests

```bash
docker compose up -d    # PostgreSQL on 5432, MySQL on 3306, and MariaDB on 3307
go test ./...
go test -run TestSQLiteDriver/RenameColumn ./internal/drivers/sqlite
```

The PostgreSQL tests need the database at `postgres://user:password@localhost:5432/dbdiff`. The MySQL tests need `root:password@tcp(localhost:3306)/dbdiff`, and the MariaDB tests need the same on the port 3307. `DBDIFF_TEST_SKIP_POSTGRES=1` stops `TestPostgresDriver`, and `DBDIFF_TEST_SKIP_MYSQL=1` stops `TestMySQLDriver` and `TestMariaDBDriver`. The CI gives the two variables and the `-short` flag to macOS and to Windows. Keep the variables empty on Linux, because a silent skip hides a server failure there.

## Structure

One engine gets one top-level function for each type under test: `TestSQLiteDriver` and `TestSQLiteMigrator`, `TestPostgresDriver` and `TestPostgresMigrator`, `TestMySQLDriver` and `TestMySQLMigrator`. `TestMariaDBDriver` runs the MariaDB branches of the mysql driver. Each behavior gets one `t.Run` subtest, named with a short noun phrase in CamelCase, for example `AddColumn`.

`TestingSQLiteDriver` and `TestingPostgresDriver` embed the driver and hold the `testing.TB` value. The constructor builds the databases and registers the cleanup with `tb.Cleanup`. The PostgreSQL harness creates one source schema and one target schema in one database, drops both in the cleanup, and checks each drop with `require.NoError`. Keep the connection of the harness open until the cleanup ends. Every harness method calls `d.tb.Helper()` on its first line.

| Helper                                   | Purpose                                            |
| ---------------------------------------- | -------------------------------------------------- |
| `ExecOnTarget(sql)`                      | Builds the wanted schema                           |
| `ExecOnSource(sql)`                      | Builds the old schema, or applies the diff         |
| `RequireInstructions(expected)`          | Compares the whole instruction list and returns it |
| `FetchAllFromSource(table, ...)`         | Reads the rows of the source as maps (SQLite)      |
| `WriteSQLFile(directory, name, content)` | Writes one `.sql` file of a SQL source             |

`NewTestSQLiteDriverWithPaths(tb, target, source)` builds a driver for two given paths. Use it for a test of a SQL source. A test that starts the temporary PostgreSQL server calls `t.Skip` under `testing.Short`, because the first run downloads the server.

## How to write a test

1. Create the harness with `NewTestSQLiteDriver(t)` or `NewTestPostgresDriver(t)`.
2. Build the wanted schema with `ExecOnTarget`.
3. Build the old schema with `ExecOnSource`. Insert rows when the change moves data.
4. Compare the whole output with `RequireInstructions`. Write the expected instructions as Go values.
5. Apply the diff with `driver.ExecOnSource(diff)`. This step proves that the SQL runs.
6. If the change moves data, read the rows with `FetchAllFromSource` and compare them with `require.Equal`.

Rules:

- Compare the whole instruction list. Never compare one instruction, and never compare the SQL text.
- Always apply the diff to the source after the comparison.
- A test that recreates a table must insert rows first, and must compare the rows after.
- Add a subtest for each new schema object and for each new kind of change.

## CLI tests

Each command package holds the tests of its own command. `TestDbdiffCommand` of `cmd/dbdiff/main_test.go` covers the root command and the `diff` command. `TestMigrateCommand` covers the flow that walks every subcommand of `migrate`. Add a subtest for each new flag and for each new error of the command. The `cmd/dbdiff/internal/clitest` package holds `Build`, `Run`, and the writers of the test fixtures.

---

# Go conventions

- Run `gofmt -w .` and `golangci-lint run ./...` after every change. Correct every finding. `.golangci.yml` holds no exclusion and no `nolint` comment. If a linter reports the code, correct the code.
- Name a variable with full words: `sourceDatabaseConnection`, not `srcConn`.
- Return an error to the caller. Never call `os.Exit` or `panic` in the `drivers` package.
- Wrap an error with context in a command package: `fmt.Errorf("failed to ...: %w", err)`.
- Close every `*sql.Rows` with `defer func() { _ = rows.Close() }()`. After the `Next` loop, return the error of `rows.Err()`. Without that check the driver reads a truncated schema.
- Drop the error of a cleanup call with the blank identifier: `_ = connection.Close()`. In a test, check that error with `require.NoError` instead.
- Pass the `context.Context` to every query with `QueryContext` or `ExecContext`.
- Use a placeholder for a value: `?` for SQLite and `$1` for PostgreSQL.

## Comments

Write no comment. That is the default. One test decides each comment: if the removal of the comment lets a reader break the code, keep the comment. In every other case, delete the comment.

Four kinds of comment pass the test:

- An order that a later change can break.
- A rule of the engine that the code cannot show, for example "SQLite refuses an ADD COLUMN action that holds a STORED generated column."
- A step that looks unnecessary. Name what breaks without the step.
- A field or a branch that the code near it ignores.

Every other comment fails the test: a sentence that repeats a name, a sentence that reads the code aloud, the SQL synopsis above an instruction type, the same reason in two places, and the empty value of a field. Write the reason alone, and do not open a comment with the name of the item below it. This rule covers a test file too. A subtest name replaces a comment above the subtest.

## Blocks

An `if`, a `for`, a `switch`, and a `select` are blocks. Two rules cover every block.

**Never initialize a variable in the header of a block.** Write `err := f()` and `if err != nil {` on two lines. Never write `if err := f(); err != nil {`. When the scope holds an `err` variable already, write `err = f()`.

**Put a blank line before a block and after a block.** Three cases take no blank line:

- The assignment that the condition checks stays on the line directly above the block, and the blank line goes before that assignment. A comment above the block belongs to the block in the same way.
- A block that opens a body takes no blank line above it.
- A block that closes a body takes no blank line below it. An `else` keeps `} else {` on one line.

```go
rows, err := db.QueryContext(ctx, query)
if err != nil {
	return nil, err
}

defer func() { _ = rows.Close() }()

for rows.Next() {
	var tableName string

	err := rows.Scan(&tableName)
	if err != nil {
		return nil, err
	}

	tables = append(tables, tableName)
}

err = rows.Err()
if err != nil {
	return nil, err
}
```

## Struct literals

**A struct literal declares one field per line.** A literal that sets one field or more splits every field onto its own line, with the closing brace on its own line. This rule covers a standard-library type such as `sql.NullString`, with no exception. **An empty literal stays on one line**: `&PostgresDropDomainDefaultAction{}`.

# Writing style

Write every text in ASD-STE100 Simplified Technical English. This rule covers code comments, commit messages, documentation, CLI help text, and error messages.

- Write an instruction in the imperative. Use one instruction per sentence, and 20 words maximum.
- Write a description in the simple present, simple past, or simple future. Use 25 words maximum per sentence.
- Use the active voice. Name the actor.
- Use only these modal verbs: can, will, must. Do not use should, would, may, might, or could.
- Put the condition first, before the command: "If the build fails, read the log".
- Use one name for one item. Do not write "config" in one place and "settings" in another.
- Do not use a semicolon. Write two sentences instead.
- Keep the articles and the conjunction "that". Do not use a contraction.
- Delete a filler word: simply, just, seamlessly, robust, comprehensive, powerful.
- Write "for example" for "e.g." and "that is" for "i.e.". Do not write "etc.".
- Never use an em dash. Use a regular dash instead.
- Do not change code, identifiers, commands, file paths, or a quoted error message.
- Do not wrap a paragraph at a column limit. Write each paragraph on one line.

# Git

- Do not run `git commit` unless the user asks for it.
- Use Conventional Commits with a scope: `feat(sqlite): add the view diff`. The scopes are `sqlite`, `postgres`, `drivers`, `cli`, `ci`, and `docs`. Omit the scope for a change that crosses several parts. Mark a breaking change with `!`.
- Write the subject in lowercase, in the imperative, with no final period. Write the subject only, with no body and no trailer. Do not add a `Claude-Session:` trailer. This rule covers a new commit and a commit that you write again.
- Update the support table of `README.md` in the same commit as the feature.
- A tag with the form `v*.*.*` starts the release workflow.

# Delegation

This section speaks to the agent that answers the user. That agent is the parent.

**If you are a subagent, stop here. Do the work yourself. Never spawn another subagent. Return your result to the parent.**

Split a large task across subagents. A clean context gives a better result. Spawn a subagent to isolate context, to run independent work in parallel, or to do bulk mechanical work. Keep the work in the parent when the parent needs the reasoning. Pick the cheapest model that does the subtask well: Haiku for mechanical work, Sonnet for scoped research, Opus for planning. The parent owns the final answer.

Rules for parallel work on one checkout:

- Give each subagent a file list that no other subagent shares. Two agents must never edit one test file at the same time.
- A subagent must never run `git checkout`, `git reset`, `git stash`, `git add`, or `git commit`. If a subagent finds work outside its scope, it reports the work. It never reverts the work.
- For a bulk edit, ask each subagent for its decisions. Then apply every decision yourself, in one place.
- Run `ListAgents` before any command that changes shared state, for example `git reset` or `gofmt -w .`. If an agent still runs, stop it with `TaskStop`.
- A subagent reads the version of this file from the start of the session. If you change a rule here during a session, repeat that rule in the spawn prompt.
