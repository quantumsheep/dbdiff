# dbdiff - Agent Context

## What dbdiff is

dbdiff is a command line tool in Go. It reads the schema of two databases and prints the SQL statements that make the source schema equal to the target schema.

The first argument is the source, which holds the current schema that the output changes. The second argument is the target, which holds the wanted schema. The whole codebase reads the two words in that one way: the `drivers` package walks the target objects first, and it prints the statements that change the source.

dbdiff supports SQLite and PostgreSQL. It compares schemas, and the `--data` flag adds the comparison of the rows. An argument names a database, a `.sql` file, or a directory of `.sql` files.

## The rules that matter most

Read this section before you write any code.

1. **Write for a human reader.** Another person must understand the code on the first read. Use full words in names. Keep the format that `gofmt` produces.
2. **Let the code explain itself.** Write no comment by default. A comment stays only when its removal lets a reader break the code. See [Comments](#comments).
3. **Keep the file architecture.** One file holds one schema object of one engine. See [Repo layout](#repo-layout).
4. **Copy the diff algorithm of the drivers that exist.** Every driver walks the target objects first and the source objects second. See [How a diff works](#how-a-diff-works).
5. **Test every change of a driver.** Each new behavior gets a subtest. See [Tests](#tests).
6. **Run the generated SQL in the test.** A test that only compares strings proves nothing. See [Tests](#tests).
7. **Read the documentation of a library before you use it.** Your training data is older than the versions in `go.mod`. See [Library documentation](#library-documentation).
8. **Write in Simplified Technical English (ASD-STE100).** This rule covers code comments, commit messages, documentation, CLI help text, and error messages. See [Writing style](#writing-style).
9. **Delegate to subagents.** Split each task into scoped subtasks. Give each subagent a clean context. A small context gives a better result.
10. **Read the neighbor files before you change a driver.** The code comments hold the engine rules and the order constraints that this file does not repeat.

## Library documentation

Before you write code against a library, read `go.mod` for its current version. Then read the documentation of that version with `go doc <import path>`, never the version that you remember. A major version renames functions and moves types.

## Repo layout

| Path                              | Content                                                                                                                                                                    |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cmd/dbdiff/`                     | `main.go` builds the root command. `main_test.go` builds the binary and checks the output and the exit code.                                                               |
| `cmd/dbdiff/cmd/`                 | One package per command. A command lives in `<command>/command.go`, and a subcommand lives in `<command>/cmd/<subcommand>/command.go`, with a `command_test.go` beside it. |
| `cmd/dbdiff/internal/helpers/`    | Version and the usage error handler, which every command reads                                                                                                             |
| `cmd/dbdiff/internal/drivers/`    | `NewDriver`, the one driver switch of the program                                                                                                                          |
| `cmd/dbdiff/internal/migrations/` | The functions of the seven migrate commands. A function of this package reads a flag, writes to a stream, or asks a question.                                              |
| `cmd/dbdiff/internal/clitest/`    | The helpers that build the binary and run it                                                                                                                               |
| `drivers/`                        | One package. It holds every driver, every schema model, and every instruction type.                                                                                        |
| `internal/migrations/`            | The model of a migration and the migrator of each engine. It knows no command and no flag.                                                                                 |

The `drivers` package uses one file per schema object per engine, with the name `<engine>_<object>.go`: for example `sqlite_index.go` and `postgres_view.go`. Shared code sits in files with no engine prefix, for example `driver.go`, `sql_source.go`, and `identifier.go`. The instruction types live in the `instruction*.go` files, split by engine and by object family. Each engine holds one test file, `<engine>_test.go`.

Do not add a package. Add a file to `drivers/` with the same pattern.

## Package names

A command package takes the name of its path, with no separator: `cmd/dbdiff/cmd/diff` holds `package cmddiff`, and `cmd/dbdiff/cmd/migrate/cmd/generate` holds `package cmdmigrategenerate`. The name of the package differs from the name of the directory, so every import of a command package writes the name:

```go
cmdmigrategenerate "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/migrate/cmd/generate"
```

A test file of a command package takes the same name with the suffix `_test`.

Each package of `cmd/dbdiff/internal` keeps its plain name: `drivers`, `migrations`, `helpers`, and `clitest`. Two of them take the name of a package of the module root. A file that imports both writes a prefix on the one of the root:

| Import path                      | Name in a file that imports both |
| -------------------------------- | -------------------------------- |
| `cmd/dbdiff/internal/drivers`    | `drivers`                        |
| `drivers`                        | `dbdiffdrivers`                  |
| `cmd/dbdiff/internal/migrations` | `migrations`                     |
| `internal/migrations`            | `coremigrations`                 |

## Build and run

```bash
go build -o ./bin/dbdiff ./cmd/dbdiff
go run ./cmd/dbdiff diff <source> <target>
go run ./cmd/dbdiff diff --driver postgres <source> <target>
```

The `--driver` flag accepts `sqlite3` and `postgres`. An empty value starts the detection of `DetectDriver`.

The `dbdiff` command holds `--driver` and `--schema`, because the `diff` command and every subcommand of `migrate` read them. The `Local` field of a flag is false, so a flag of a command applies to each subcommand of that command.

Every other flag belongs to the command that reads it:

| Command                | Flags                                                 |
| ---------------------- | ----------------------------------------------------- |
| `diff`                 | `--privileges`, `--comments`, `--data`, `--exit-code` |
| Each `migrate` command | `--config`, `--source`, `--target`                    |
| `migrate preview`      | The three flags above, and `--run`                    |
| `migrate up`           | The three flags above, and `--to`                     |

Each subcommand of `migrate` writes its flags in its own `Command` function, and the `migrate` command itself declares no flag. A flag holds the value that the parser writes into it, so each command needs its own instances. A change of the text of one of the three migration flags touches each of the seven files.

The SQLite driver needs CGO, because `go-sqlite3` is a C binding. If a build fails with an undefined symbol, set `CGO_ENABLED=1` before the build.

---

# Drivers

## The Driver interface

```go
type Driver interface {
	Close() error
	Diff(ctx context.Context) ([]Instruction, error)
}
```

A driver holds two `*sql.DB` fields: `SourceDatabaseConnection` and `TargetDatabaseConnection`. A constructor is `New<Engine>Driver(ctx context.Context, config *<Engine>DriverConfig)`. The constructor takes a context, because a SQL source runs statements before the diff starts.

A driver opens one side with `OpenSide(ctx, path, ...)`. That method reads a database, or it builds a temporary database from a SQL source. `Close` releases the temporary database.

To register a new driver: add a case to the switch in `NewDriver` of `cmd/dbdiff/internal/drivers`, add a case to the switch in `NewMigrator` of `internal/migrations`, and add a value to `SupportedDriverNames` in `driver_detection.go`. The validator of the `--driver` flag reads that list.

`DetectDriver` in `driver_detection.go` names the engine when the user gives no `--driver` flag. A `sqlite://` prefix or a plain path names sqlite3. A `postgres://` URL or a keyword string names postgres. A SQL source and an unknown scheme name nothing. Two empty or two different names give an error that names the `--driver` flag. One name and one empty name give that one name, because a SQL source takes the engine of the other side.

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

Every diff function follows the same three steps. The first step reads the objects of the target and the objects of the source. The second step walks the target objects: when the name is absent from the source, print a `CREATE` statement, and when the object differs, print an `ALTER` statement or a `DROP` and `CREATE` pair. The third step walks the source objects and prints a `DROP` statement for each object that the target does not hold.

Rules for the output:

- Build the output as `[]Instruction`. Append a value, and never format a string.
- Return the list at the end of the function. A section returns a `*SectionDiff`.
- An instruction type holds one field for each part of its PostgreSQL synopsis. The type quotes every identifier, so no call site holds a format string.
- Print the additions and the modifications first. Print the removals last.
- `Instructions()` returns the list that creates the object and its dependants.

Use `lo.Find`, `lo.Map`, and `lo.Values` from `samber/lo` for the list operations.

## Instructions

A diff is a list of instructions. An `Instruction` is one complete SQL statement, or one comment line.

```go
type Instruction interface {
	String() string
	Comment() string
}
```

`String` gives the statement. `Comment` names the object that the statement changes, for example `Create the table "users"`. The `--comments` flag prints those comments, and `AnnotateInstructions` groups the instructions that share one comment text. A new instruction type takes one `Comment` method below its `String` method, one subtest in `drivers/instruction_test.go`, and one subtest in `drivers/instruction_comment_test.go`. A `Comment` method calls one of the builders of `instruction_comment.go`, so it stays one line. A field named `Comment` cannot live beside the method, so an instruction that holds a comment text names the field `Text`.

In this package, `String() string` belongs to an instruction type only. A model type answers with an instruction, with a fragment, or with a list of instructions. That rule stops a fragment from reaching the output, because a bare column definition is not a statement. This command finds a violation, so it must return nothing:

```bash
rg -n 'func \([a-z][A-Za-z0-9]* \*?[A-Za-z]+\) String\(\) string' drivers/ --glob '!drivers/instruction*.go'
```

A nested part renders through a method that names what the method returns. Each part interface takes a different method name, so no part satisfies another part:

```go
type AlterTableAction interface  { TableActionClause() string }
type AlterDomainAction interface { DomainActionClause() string }
type Condition interface         { ConditionClause() string }
```

The prefix of a type name reports the portability of the syntax, and not the driver that emits the statement:

| Prefix     | Meaning                                                              |
| ---------- | -------------------------------------------------------------------- |
| `SQL`      | Both engines accept the same syntax                                  |
| `SQLite`   | The syntax of SQLite differs, or SQLite alone holds the form         |
| `Postgres` | The syntax of PostgreSQL differs, or PostgreSQL alone holds the form |

`drivers/instruction_test.go` is the one place that holds the SQL text of a statement. A driver test compares instructions, and it trusts that file.

## The SQLite driver

The driver reads the schema with `PRAGMA` statements and with `sqlite_master`. A `PRAGMA` statement takes no placeholder, so the driver puts every name through `QuoteIdentifier` before it joins the name into the statement. The parser of `sqlite_definition.go` reads the parts of a `CREATE TABLE` statement that no PRAGMA reports, for example a collation, a check, or the table options.

SQLite does not support `ALTER COLUMN`. When a column, a foreign key, or a table constraint changes, the driver recreates the table: it creates a temporary table, copies the rows with an explicit column map, drops the old table, renames the temporary table, and rebuilds every index and trigger of the target. After a recreation the diff compares no index and no trigger of that table, because the recreation rebuilds them all.

The driver detects a renamed column. A target column that the source does not hold, and that holds the attributes of exactly one free source column, is a rename. Two candidates make the guess unsafe, and the column becomes an addition and a removal.

SQLite gives no stable order for the tables and the foreign keys. The driver keeps the creation order of the tables and sorts the foreign keys with a stable sort. Keep every such sort. Without it the output changes between two runs.

## The PostgreSQL driver

The driver connects through `pgx/v5/stdlib`. It reads one schema: the `--schema` flag names it, and an empty value keeps the search path of the connection string. `Diff` verifies first that the schema exists on both sides. Without that check an absent schema reads as an empty schema, and the diff drops every object of the source.

The driver reads the `pg_catalog` tables, because `information_schema` hides a materialized view, a partition bound, and other details. Give every catalog query a stable `ORDER BY`, because PostgreSQL gives no stable order.

`Diff` prints twelve sections in a dependency order, from extensions to privileges. Each `Diff<Object>s` function returns a `SectionDiff` with an `EarlyRemovals` field, an `Additions` field, and a `Removals` field. `Diff` prints every `EarlyRemovals` in the REVERSE section order, then every `Additions` in the section order, then every `Removals` in the REVERSE section order. PostgreSQL refuses a `DROP` statement while another object uses the object, so a new section needs no other work to get a correct order. `EarlyRemovals` covers the object that blocks an addition of another section: for example, a view drops before the table changes that it blocks, and its `CREATE VIEW` comes after them.

Order rules inside the tables section. Keep them:

- A `CREATE TABLE` statement holds no foreign key. Every foreign key and every rule prints after every table, because two tables can name each other, and a rule can name a second table.
- The tables sort by dependency, and the removals walk that order backward.
- The constraint block and the index block print BEFORE the column removals. A column drop removes its constraints and indexes, and a later `DROP CONSTRAINT` then fails.

Three rules keep the output free of noise:

- An object that an extension owns stays out of the diff. The `CREATE EXTENSION` statement recreates it.
- A sequence that a `SERIAL` column or an identity column owns stays out of the diff. The column definition builds it again.
- A query that reads a definition text (a function, an index, a trigger) removes the prefix of `current_schema()`. Without that step the text of two equal objects differs, and the diff drops and recreates an object that did not change - in the SOURCE schema. Keep that step in every new query that reads a definition text.

PostgreSQL holds no `ALTER` action for some objects, for example a policy, a rule, the query of a materialized view, or the expression of a generated column. A change of such an object prints a `DROP` statement and a `CREATE` statement, and that pair stays one modification in `Additions`.

## SQL sources

An argument names SQL text when the path ends in `.sql`, or when the path is a directory. A path that holds `://` is a connection URL, so it never names SQL text. A directory gives the `.sql` files of its top level, in the order of the names, without the `.down.sql` files. A directory that holds no `.sql` file gives an empty schema, which the first `migrate generate` reads.

Each engine materializes a source as a temporary database: SQLite as a file in a temporary directory, and PostgreSQL as a scratch server from `fergusstrange/embedded-postgres`. `Close` removes it.

The scratch server takes the major version of the database of the other side. When both sides are SQL sources, the version comes from the `version` key of `dbdiff.yaml`, and an empty key gives the default version of the library. Do not name a version in the code. The user names it, because only the user knows the version of the real server.

Four settings of the scratch server matter. Keep them:

- The logger is `io.Discard`, because the default logger writes to the stream that holds the diff.
- The port comes from a free-port lookup, because a real server holds the default port 5432.
- `Version` stays absent for an empty version, because an empty value gives an invalid configuration.
- `BinariesPath` names a stable cache directory whose name holds the version. Without it every run extracts the archive again.

## The no-transaction directive

A file that holds no directive runs in ONE call. A file that holds the line `-- dbdiff:no-transaction`, or the line `-- atlas:txmode none` of Atlas, runs one call for each statement. PostgreSQL runs the statements of one call in an implicit transaction block, and it refuses `CREATE INDEX CONCURRENTLY` and every other statement of that kind there. The directive is the one signal that starts the split. Never read the SQL text to make that decision.

`drivers/sql_statement.go` holds the directive and the statement splitter, because the replay of a SQL source and the apply of a migration both read them, and `drivers` imports neither migrations package. The splitter understands comments, strings, quoted identifiers, and dollar quotes, so a function body that holds a semicolon stays one statement.

## Migrations

The `migrate` command group builds a directory of migration files from a diff, and it applies them to a database with a lock and a history table. It gives the word `source` to the migration directory. It gives the word `target` to the other side: the wanted schema of `generate`, and the database that the six other commands change.

`Migrator` in `internal/migrations` is a sibling of `Driver`, not a wrapper of it. A migration needs a lock, a history table, and transactions, and a plain schema diff needs none of them. The command builds the driver and the migrator, and `NewDriver` and `NewMigrator` hold the one engine switch of each half. `internal/migrations` imports `drivers`, and `drivers` imports nothing of it. The constant `MigrationHistoryTableName` stays in `drivers`, because the diff hides that table.

A migration entry holds one of five states: `pending` (no history row), `applied` (the file and its row agree on the checksum), `changed` (they disagree), `missing` (a row without a file), and `out of order` (no row, and the version sorts before the last applied version).

Three behavior rules. Keep them:

- `status`, `preview`, and `verify` report an out of order file, because their job is to report. `up` and `step` refuse it, because applying it writes a history row that breaks the version order, and only a new generate can fix that record.
- The preview applies every pending file in ONE transaction and rolls it back at the end. A file reads the objects of the files before it, and a separate transaction for each file hides those objects.
- `up` and `step` commit one file at a time, and they roll back no earlier file. The one difference between the two commands: `step` asks a question before each file.

## Data comparison

The `--data` flag sets the `CompareData` field of the driver config. The schema output stays the same in every case. Each engine holds its own `DiffData` copy, like every other part of the diff.

The data section prints after the whole schema section, because a new row needs its table and its column. The comparison covers the tables that both sides hold, and it needs the primary key: a table with no primary key, or with a different one in the source, gets a comment line and no row statement. The output holds an `INSERT` statement for a key of the target only, an `UPDATE` statement for a key with a different row, and a `DELETE` statement for a key of the source only. A table that the target only holds gets one `INSERT` statement per row, because the schema section creates it empty. Format each value as an SQL literal before the comparison, so `NULL` never equals the text `'NULL'`.

---

# Tests

Every test lives beside the code, in `drivers/<engine>_test.go`, in a `<name>_test.go` of a migrations package, and in a `command_test.go` file of a command package. There is no `tests` folder. The tests use `github.com/stretchr/testify/require`.

## Run the tests

```bash
docker compose up -d    # PostgreSQL on port 5432, needed by the PostgreSQL tests
go test ./...
go test -run TestSQLiteDriver/RenameColumn ./drivers
```

The PostgreSQL tests need the database at `postgres://user:password@localhost:5432/dbdiff`. The SQLite tests need no service, because each one writes into `tb.TempDir()`.

`DBDIFF_TEST_SKIP_POSTGRES=1` stops `TestPostgresDriver`, which needs that server. A runner of macOS and a runner of Windows start no service container, so the CI gives that variable and the `-short` flag to those two platforms. Keep the variable empty on Linux. A server that fails there must fail the build, and a silent skip hides that failure.

## Structure

One engine gets one top-level function for each type under test: `TestSQLiteDriver` and `TestSQLiteMigrator`, `TestPostgresDriver` and `TestPostgresMigrator`. Each behavior gets one `t.Run` subtest. The subtest name is a short noun phrase in CamelCase, for example `AddColumn`, `ModifyIndexes`, or `ForeignKeys`.

Each engine has a test harness type. `TestingSQLiteDriver` and `TestingPostgresDriver` embed the driver and hold the `testing.TB` value. The constructor builds the databases and registers the cleanup with `tb.Cleanup`.

The PostgreSQL harness creates one source schema and one target schema in one database. It drops both in the cleanup, and it checks the error of each drop with `require.NoError`. A cleanup that fails without a check leaves a schema in the database of every run. Keep the connection of the harness open until the cleanup ends.

| Helper                                   | Purpose                                            |
| ---------------------------------------- | -------------------------------------------------- |
| `ExecOnTarget(sql)`                      | Builds the wanted schema                           |
| `ExecOnSource(sql)`                      | Builds the old schema, or applies the diff         |
| `RequireInstructions(expected)`          | Compares the whole instruction list and returns it |
| `FetchAllFromSource(table, ...)`         | Reads the rows of the source as maps (SQLite)      |
| `WriteSQLFile(directory, name, content)` | Writes one `.sql` file of a SQL source             |

`NewTestSQLiteDriverWithPaths(tb, target, source)` builds a driver for two given paths. Use it for a test of a SQL source. `NewTestSQLiteDriver` calls it with two database files.

A test that starts the temporary PostgreSQL server calls `t.Skip` under `testing.Short`, because the first run downloads the server.

Every harness method calls `d.tb.Helper()` on its first line.

## How to write a test

1. Create the harness with `NewTestSQLiteDriver(t)` or `NewTestPostgresDriver(t)`.
2. Build the wanted schema with `ExecOnTarget`.
3. Build the old schema with `ExecOnSource`. Insert rows when the change moves data.
4. Compare the whole output with `RequireInstructions`. Write the expected instructions as Go values.
5. Apply the diff with `driver.ExecOnSource(diff)`. This step proves that the SQL runs.
6. If the change moves data, read the rows with `FetchAllFromSource` and compare them with `require.Equal`.

Rules:

- Compare the whole instruction list. Never compare one instruction, and never compare the SQL text. `instruction_test.go` covers the text.
- Always apply the diff to the source after the comparison.
- A test that recreates a table must insert rows first, and must compare the rows after.
- Add a subtest for each new schema object and for each new kind of change.

## CLI tests

Each command package holds the tests of its own command. `TestDbdiffCommand` of `cmd/dbdiff/main_test.go` runs the binary, so it covers the root command and the `diff` command. `TestMigrateCommand` covers the flow that walks every subcommand of `migrate`, and `TestMigrate<Subcommand>Command` covers one subcommand alone. Add a subtest for each new flag and for each new error of the command.

The `cmd/dbdiff/internal/clitest` package holds the helpers. `Build` compiles the command into `tb.TempDir()` one time, and `Run` runs the binary and returns the standard output, the standard error, and the exit code. The package also holds the writers of a `.sql` file, of a SQLite database, of a `dbdiff.yaml` file, and of a migrations directory.

---

# Go conventions

- Run `gofmt -w .` and `golangci-lint run ./...` after every change. Correct every finding. The `lint` job of the CI runs the same command. `.golangci.yml` enables the standard linters and the `gci`, `gofmt`, and `goimports` formatters. `golangci-lint fmt` writes the format that they ask for. It holds no exclusion and no `nolint` comment. If a linter reports the code, correct the code.
- Use tabs for the indentation. `gofmt` writes them.
- Name a variable with full words: `sourceDatabaseConnection`, not `srcConn`.
- Return an error to the caller. Never call `os.Exit` or `panic` in the `drivers` package.
- Wrap an error with context in a command package: `fmt.Errorf("failed to ...: %w", err)`.
- Close every `*sql.Rows` with `defer func() { _ = rows.Close() }()`. After the `Next` loop, return the error of `rows.Err()`. Without that check the driver reads a truncated schema. The blank identifier tells `errcheck` that the code drops the error of `Close`, because `rows.Err()` gives the same error.
- Drop the error of a cleanup call with the blank identifier: `_ = connection.Close()`. The code returns the error of the operation that failed, and the error of the cleanup hides it. In a test, check that error with `require.NoError` instead.
- The CLI prints the error and exits with the code 1. `main` owns that, because `cmd.Run` returns the error to the caller.
- Pass the `context.Context` to every query with `QueryContext` or `ExecContext`.
- Use a placeholder for a value: `?` for SQLite and `$1` for PostgreSQL.
- The `drivers` package exports its types and its methods, because the tests and the CLI use them.

## Comments

Write no comment. That is the default. A good name and a small function tell the reader what the code does.

One test decides each comment. If the removal of the comment lets a reader break the code, keep the comment. In every other case, delete the comment.

Four kinds of comment pass the test:

- An order that a later change can break. Example: "PostgreSQL refuses to drop the index that the replica identity of the source holds, so this block comes first."
- A rule of the engine that the code cannot show. Example: "SQLite refuses an ADD COLUMN action that holds a STORED generated column."
- A step that looks unnecessary. Name what breaks without the step. Example: "Without this step the diff prints a DROP statement for an object that did not change."
- A field or a branch that the code near it ignores. Example: "String writes no comment, because CREATE TABLE accepts none."

Delete every other comment. These five kinds fail the test:

- A sentence that repeats the name of a function, of a type, or of a field. Delete `// GetOwners returns the owner of each object of the schema.`
- A sentence that reads the code aloud. Delete `// The loop walks the target columns and it appends each new column.`
- The SQL synopsis above an instruction type. The `String` method below holds the same text.
- The same reason in two places. Keep the reason at the place that a reader changes first. A test gets no copy of a reason that the driver holds already.
- The empty value of a field. The declaration `Collation string` tells enough.

Write the reason alone. Do not open a comment with the name of the item below it. Write "PostgreSQL accepts a comment in no CREATE statement." Do not write "CommentInstructions returns the statement of the comment. PostgreSQL accepts a comment in no CREATE statement."

This rule covers a test file too. A subtest name is a short noun phrase, and that name replaces a comment above the subtest.

## Blocks

An `if`, a `for`, a `switch`, and a `select` are blocks. Two rules cover every block.

**Never initialize a variable in the header of a block.** Write the assignment on the line above the block. Write `err := f()` and `if err != nil {` on two lines. Never write `if err := f(); err != nil {`. When the scope holds an `err` variable already, write `err = f()`.

**Put a blank line before a block and after a block.** Three cases take no blank line:

- The assignment that the condition checks belongs to the block. It stays on the line directly above the block, and the blank line goes before that assignment. A comment above the block belongs to the block in the same way.
- A block that opens a body takes no blank line above it.
- A block that closes a body takes no blank line below it. An `else` keeps `} else {` on one line, with no blank line above it and none below it.

```go
func (d *SQLiteDriver) GetTables(ctx context.Context, db *sql.DB) ([]*SQLiteTable, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table';")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

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

## Struct literals

**A struct literal declares one field per line.** A literal that sets one field or more splits every field onto its own line, with the closing brace on its own line. This rule covers a type of this repository and a standard-library type such as `sql.NullString`, with no exception.

**An empty literal stays on one line.** It sets no field, so it has nothing to split: `&PostgresDropDomainDefaultAction{}`.

```go
// Before
return &PostgresDropAggregateInstruction{Name: a.Name, Arguments: a.Arguments}

// After
return &PostgresDropAggregateInstruction{
	Name:      a.Name,
	Arguments: a.Arguments,
}
```

# Writing style

Write every text in ASD-STE100 Simplified Technical English. This rule covers code comments, commit messages, documentation, CLI help text, and error messages. The standard is a free download at asd-ste100.org.

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
- Use Conventional Commits with a scope: `feat(sqlite): add the view diff`.
- The scopes of this repo are `sqlite`, `postgres`, `drivers`, `cli`, `ci`, and `docs`. Omit the scope for a change that crosses several parts.
- Mark a breaking change with `!`: `feat(cli)!: rename the driver flag`.
- Write the subject in lowercase, in the imperative, with no final period.
- Write the subject only. The subject gives enough information, so a commit takes no body.
- Write no trailer. Do not add a `Claude-Session:` trailer, and do not add another trailer. This rule covers a new commit and a commit that you write again.
- Update the support table of `README.md` in the same commit as the feature.
- A tag with the form `v*.*.*` starts the release workflow. It builds five binaries and publishes them.

# Delegation

This section speaks to the agent that answers the user. That agent is the parent.

**If you are a subagent, stop here. Do the work yourself. Never spawn another subagent. Return your result to the parent.**

## How the parent delegates

Split a large task across subagents. A clean context gives a better result.

- Spawn a subagent to isolate context, to run independent work in parallel, or to do bulk mechanical work.
- Keep the work in the parent when the parent needs the reasoning, or when the synthesis needs every part at the same time.
- Pick the cheapest model that does the subtask well. Haiku does mechanical work. Sonnet does scoped research and code exploration. Opus does planning and trade-offs.
- The parent owns the final answer.

## Parallel writes

Two agents that write to one file will corrupt the work of each other. These rules keep parallel work safe on one checkout:

- Give each subagent a file list that no other subagent shares. The file split of `drivers/` makes this easy. One agent takes `sqlite_index.go`. Another agent takes `sqlite_view.go`.
- Both engines write into one test file each. Two agents must never edit `drivers/sqlite_test.go` at the same time.
- A subagent must never run `git checkout`, `git reset`, `git stash`, `git add`, or `git commit`. If a subagent finds work outside its scope, it reports the work. It never reverts the work.
- For a bulk edit, ask each subagent for its decisions. Then apply every decision yourself, in one place.

## Before you change the working tree

Run `ListAgents` before any command that changes shared state. This covers `git checkout`, `git reset`, `git stash`, `gofmt -w .`, and a bulk apply script.

If an agent still runs, stop it with `TaskStop`. Then confirm that the state is `killed`.

A subagent reads the version of this file from the start of the session. If you change a rule here during a session, repeat that rule in the spawn prompt.
