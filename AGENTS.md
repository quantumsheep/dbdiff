# dbdiff - Agent Context

## What dbdiff is

dbdiff is a command line tool in Go. It reads the schema of two databases. Then it prints
the SQL statements that make the target schema equal to the source schema.

The command takes two arguments. The first argument is the source. The second argument is
the target. The source holds the wanted state. The output changes the target.

dbdiff supports SQLite and PostgreSQL. It compares schemas. The `--data` flag adds the
comparison of the rows, and the default value of that flag is off.

An argument names a database, a `.sql` file, or a directory of `.sql` files. See
[SQL sources](#sql-sources).

## The rules that matter most

Read this section before you write any code.

1. **Write for a human reader.** Another person must understand the code on the first
   read. Use full words in names. Keep the format that `gofmt` produces.
2. **Let the code explain itself.** Write no comment by default. A comment stays only when
   its removal lets a reader break the code. See [Comments](#comments).
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
| `github.com/urfave/cli`   | v3.11.0 | `go doc github.com/urfave/cli/v3`              |
| `github.com/jackc/pgx`    | v5.10.0 | `go doc github.com/jackc/pgx/v5/stdlib`        |
| `github.com/mattn/go-sqlite3` | v1.14.50 | `go doc github.com/mattn/go-sqlite3`      |
| `github.com/samber/lo`    | v1.53.0 | `go doc github.com/samber/lo`                  |
| `github.com/stretchr/testify` | v1.12.0 | `go doc github.com/stretchr/testify/require` |
| `github.com/fergusstrange/embedded-postgres` | v1.34.0 | `go doc github.com/fergusstrange/embedded-postgres` |

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
├── driver.go                  The Driver interface and SectionDiff
├── driver_detection.go        DetectDriver and the driver names
├── identifier.go              quoteIdentifier and quoteIdentifiers
├── sql_source.go              The SQL file source: detection, file order, apply
├── driver_detection_test.go   The tests of DetectDriver
├── sqlite.go                  The SQLite driver: connections, queries, top-level diff
├── sqlite_table.go            SQLiteTable and its diff
├── sqlite_column.go           SQLiteColumn
├── sqlite_index.go            SQLiteIndex
├── sqlite_trigger.go          SQLiteTrigger
├── sqlite_view.go             SQLiteView
├── sqlite_foreign_key.go      SQLiteForeignKey
├── sqlite_definition.go       The parser of a CREATE TABLE statement of SQLite
├── sqlite_unique_constraint.go SQLiteUniqueConstraint, a key of two or more columns
├── sqlite_check_constraint.go SQLiteCheckConstraint, a check of a table
├── sqlite_virtual_table.go    SQLiteVirtualTable and its diff
├── sqlite_data.go             The row comparison of SQLite
├── sqlite_sql_source.go       The temporary SQLite database of a SQL source
├── sqlite_test.go             The SQLite test harness and the tests
├── postgres.go                The PostgreSQL driver
├── postgres_table.go          PostgresTable and its diff
├── postgres_column.go         PostgresColumn
├── postgres_index.go          PostgresIndex
├── postgres_constraint.go     PostgresConstraint
├── postgres_trigger.go        PostgresTrigger
├── postgres_view.go           PostgresView
├── postgres_materialized_view.go PostgresMaterializedView
├── postgres_policy.go         PostgresPolicy, a row level security policy
├── postgres_rule.go           PostgresRule, a rule of a table
├── postgres_statistics.go     PostgresStatistics, an extended statistics object
├── postgres_privilege.go      PostgresPrivilege and PostgresOwner
├── postgres_sequence.go       PostgresSequence
├── postgres_type.go           PostgresType, an enum type
├── postgres_domain.go         PostgresDomain
├── postgres_composite_type.go PostgresCompositeType
├── postgres_function.go       PostgresFunction
├── postgres_aggregate.go      PostgresAggregate
├── postgres_operator.go       PostgresOperator
├── postgres_extension.go      PostgresExtension
├── postgres_cast.go           The automatic cast lookup and the USING clause
├── postgres_data.go           The row comparison of PostgreSQL
├── postgres_sql_source.go     The temporary PostgreSQL server of a SQL source
└── postgres_test.go           The PostgreSQL test harness and the tests
```

Do not add a package. Add a file to `drivers/` with the name `<engine>_<object>.go`.

## Build and run

```bash
go build -o ./bin/dbdiff ./cmd/dbdiff
go run ./cmd/dbdiff <source> <target>
go run ./cmd/dbdiff --driver postgres <source> <target>
```

The `--driver` flag accepts `sqlite3` and `postgres`. An empty value starts the detection
of `DetectDriver`. See [Driver detection](#driver-detection).

The SQLite driver needs CGO, because `go-sqlite3` is a C binding. If a build fails with an
undefined symbol, set `CGO_ENABLED=1` before the build.

---

# Drivers

## The Driver interface

```go
type Driver interface {
	Close() error
	Diff(ctx context.Context) ([]Instruction, error)
}
```

A driver holds two `*sql.DB` fields: `SourceDatabaseConnection` and
`TargetDatabaseConnection`. A constructor is
`New<Engine>Driver(ctx context.Context, config *<Engine>DriverConfig)`. The constructor
takes a context, because a SQL source runs statements before the diff starts. The config
struct holds the two connection strings. To register a new driver, add a case to the switch
in `cmd/dbdiff/main.go` and a value to the flag validator.

A driver opens one side with `OpenSide(ctx, path, ...)`. That method reads a database, or
it builds a temporary database from a SQL source. `Close` releases the temporary
database.

## Driver detection

`DetectDriver(source, target)` in `driver_detection.go` names the engine when the user
gives no `--driver` flag. It reads each argument alone with `detectDriverOfArgument`, and
that function answers with an empty name when the argument names no engine.

| Argument                                              | Name                 |
| ----------------------------------------------------- | -------------------- |
| The prefix `sqlite://`                                 | `SQLiteDriverName`   |
| The prefix `postgres://` or `postgresql://`            | `PostgresDriverName` |
| Another `://` scheme                                   | empty                |
| A SQL source, which `IsSQLSource` reports              | empty                |
| A keyword string, for example `host=localhost user=app` | `PostgresDriverName` |
| Another path                                           | `SQLiteDriverName`   |

Two empty names give an error, and two different names give an error. Each message names
the `--driver` flag. One name and one empty name give that one name, because a SQL source
takes the engine of the other side.

## Naming

| Element         | Pattern                            | Example                       |
| --------------- | ---------------------------------- | ----------------------------- |
| Model type      | `<Engine><Object>`                 | `SQLiteTable`, `PostgresView` |
| Read method     | `Get<Object>s(ctx, db, ...)`       | `GetTableColumns`             |
| Compare method  | `Diff<Object>s(other)`             | `DiffIndexes`                 |
| Lookup method   | `<Object>ByName(name) (*T, bool)`  | `ColumnByName`                |
| Equality method | `Equal(other *T) bool`             | `SQLiteIndex.Equal`           |
| Statement method   | `String() string`                  | `SQLUpdateInstruction.String` |
| Fragment method    | `Definition()` or `Clause()`       | `SQLiteColumn.Definition`     |
| Instruction list   | `Instructions() []Instruction`     | `SQLiteTable.Instructions`    |
| Instruction maker  | `CreateInstruction()`, `DropInstruction()`, `UpdateInstruction()` | `PostgresDomain.CreateInstruction` |
| Instruction type   | `<Prefix><Statement>Instruction`   | `SQLUpdateInstruction`        |
| Action type        | `<Prefix><Action>Action`           | `PostgresSetNotNullAction`    |

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

Rules for the output:

- Build the output as `[]Instruction`. Append a value, and never format a string.
- Return the list at the end of the function. A section returns a `*SectionDiff`.
- An instruction type holds one field for each part of its PostgreSQL synopsis. The type
  quotes every identifier, so no call site holds a format string.
- Print the additions and the modifications first. Print the removals last.
- `Instructions()` returns the list that creates the object and its dependants.

Use `lo.Find`, `lo.Map`, and `lo.Values` from `samber/lo` for the list operations.

## Instructions

A diff is a list of instructions. An `Instruction` is one complete SQL statement, or one
comment line.

```go
type Instruction interface {
	String() string
}
```

In this package, `String() string` belongs to an instruction type only. A model type
answers with an instruction, with a fragment, or with a list of instructions. That rule
stops a fragment from reaching the output, because a bare column definition is not a
statement.

This command finds a violation of the rule. It matches a `String() string` method outside
the instruction files, so it must return nothing:

```bash
rg -n 'func \([a-z][A-Za-z0-9]* \*?[A-Za-z]+\) String\(\) string' drivers/ --glob '!drivers/instruction*.go'
```

A nested part renders through a method that names what the method returns. Each part
interface takes a different method name, so no part satisfies another part:

```go
type AlterTableAction interface  { TableActionClause() string }
type AlterDomainAction interface { DomainActionClause() string }
type Condition interface         { ConditionClause() string }
```

The prefix of a type name reports the portability of the syntax, and not the driver that
emits the statement:

| Prefix     | Meaning                                                              |
| ---------- | --------------------------------------------------------------------- |
| `SQL`      | Both engines accept the same syntax                                   |
| `SQLite`   | The syntax of SQLite differs, or SQLite alone holds the form           |
| `Postgres` | The syntax of PostgreSQL differs, or PostgreSQL alone holds the form   |

`ALTER TABLE` takes one type for each engine, because SQLite accepts one action and
PostgreSQL accepts a list.

The types live in six files:

| File                                      | Content                                                            |
| ------------------------------------------ | ------------------------------------------------------------------- |
| `drivers/instruction.go`                  | The interfaces, the shared statements and actions, the conditions   |
| `drivers/instruction_sqlite.go`           | The SQLite statements                                                |
| `drivers/instruction_postgres_table.go`   | The table, the index, the trigger, the view                         |
| `drivers/instruction_postgres_type.go`    | The enum type, the composite type, the domain                       |
| `drivers/instruction_postgres_routine.go` | The function, the aggregate, the operator                           |
| `drivers/instruction_postgres_object.go`  | The extension, the sequence                                          |

`drivers/instruction_test.go` holds one subtest for each type. It is the one place that
holds the SQL text of a statement. A driver test compares instructions, and it trusts that
file.

## SQLite driver

The SQLite driver reads the schema with `PRAGMA` statements and with `sqlite_master`:

| Data          | Query                                                     |
| ------------- | --------------------------------------------------------- |
| Tables        | `SELECT name FROM sqlite_master WHERE type='table'`        |
| Columns       | `PRAGMA table_info(<table>)`                               |
| Indexes       | `PRAGMA index_list(<table>)`, `PRAGMA index_info(<index>)`, and the `sql` column of `sqlite_master` |
| Foreign keys  | `PRAGMA foreign_key_list(<table>)`                         |
| Triggers      | `SELECT name, sql FROM sqlite_master WHERE type='trigger'` |
| Views         | `SELECT name, sql FROM sqlite_master WHERE type='view'`    |

A `PRAGMA` statement takes no placeholder. The driver puts the name through
`quoteIdentifier` before it joins the name into the statement.

`NewSQLiteDriver` removes the `sqlite://` prefix from each path.

`SQLiteTable.DiffTable` compares the columns, the indexes, and the triggers of one table.
`SQLiteDriver.DiffTables` calls that one method. It calls `DiffIndexes` and `DiffTriggers`
in no other place, because those two methods compare the target that `DiffTable` can drop.

SQLite does not support `ALTER COLUMN`. `DiffTable` recreates the table when a column
changes, when a foreign key changes, or when a table constraint changes.
`SQLiteTableColumnsDiff.NeedsRecreation` reports that case. The recreation prints six parts
in this order:

1. `CREATE TABLE "_<name>_temp"` with the new columns and the new foreign keys.
2. `INSERT INTO "_<name>_temp" (...) SELECT ... FROM "<name>"` with an explicit column
   map. A new column takes its `DEFAULT` value, or `NULL`.
3. `DROP TABLE "<name>"`.
4. `ALTER TABLE "_<name>_temp" RENAME TO "<name>"`.
5. One `CREATE INDEX` statement for each index of the source table.
6. One `CREATE TRIGGER` statement for each trigger of the source table.

Part 3 removes each index and each trigger of the table. Parts 5 and 6 build every one of
them again. `DiffTable` returns the list at that point, and it compares no index and no
trigger. Without that step the diff prints a `CREATE INDEX` statement two times, or it
prints a `DROP INDEX` statement for an index that part 3 removed. A trigger that the two
sides both hold gets no statement, and the recreation loses it.

`DiffColumns` detects a rename. A source column that the target does not hold, and that
holds the attributes of exactly one free target column, is a rename. Two candidates make
the guess unsafe. In that case the column becomes an addition, and the old columns become
removals. A target column that another rename holds is not a candidate.
`IsTypeChangeCompatible`
holds the type list that a recreation can convert: `TEXT`, `INTEGER`, `REAL`, and `BLOB`.
An incompatible type change becomes a `DROP COLUMN` statement and an `ADD COLUMN`
statement.

`GetTables` reads the names from `sqlite_master`, in the order of `rowid`. That order is
the order of the creation, so a table that holds a foreign key comes after the table that
it names. Keep that order. `PRAGMA table_list` gives the kind of each table, and the query
drops a row of the kind `virtual` or `shadow`. A shadow table belongs to the module of a
virtual table, and the module builds it again.

`GetTableColumns` reads `PRAGMA table_xinfo`. `PRAGMA table_info` gives no generated
column, so it hides that column from the whole diff. The `hidden` value names the kind of
the column: 1 is a hidden column of a virtual table, 2 is a VIRTUAL generated column, and 3
is a STORED one. The PRAGMA gives no expression, so `parseTableDefinition` in
`sqlite_definition.go` reads the expression from the CREATE TABLE statement of
`sqlite_master`.

`parseTableDefinition` in `sqlite_definition.go` reads every part of a `CREATE TABLE`
statement that no PRAGMA reports: the collation of a column, the keyword `AUTOINCREMENT`,
a check of a column, a check of a table, and the table options `WITHOUT ROWID` and
`STRICT`. `GetTable` and `GetTableColumns` each call it one time. A change of one of these
sets `TableOptionsChanged` or makes the column differ, and the table then needs a
recreation.

Two rules cover a generated column. The `INSERT` statement of a recreation names no such
column, because SQLite computes it and refuses a value for it. SQLite also refuses an
`ADD COLUMN` action that holds a STORED generated column, so `NeedsRecreation` answers true
for that addition.

`GetTriggers` reads the triggers of a table and the triggers of a view, because
`sqlite_master` holds the name of the view in `tbl_name` of an `INSTEAD OF` trigger.
`SQLiteView.Diff` compares them. A `DROP VIEW` statement removes every trigger of the view,
so the recreation of a view builds each trigger of the source again.

`GetTableForeignKeys` sorts the foreign keys with `sort.SliceStable`, because SQLite gives
no stable order. Keep that sort. Without it the output changes between two runs.

`PRAGMA index_list` gives an `origin` value for each index. `GetTableIndexes` keeps the
origin `c`, which is an index that a `CREATE INDEX` statement built. `GetTableUniqueKeys`
reads the origin `u`, which is an index that a `UNIQUE` constraint built. A key of one
column sets the `Unique` field of that column. A key of two or more columns becomes a
table constraint of `SQLiteTable`. Without this the recreation of a table loses the
constraint.

The `pk` value of `PRAGMA table_info` is a position, not a flag. SQLite numbers the
columns of a composite primary key 1, 2, 3 in the key order. A key of one column keeps the
column constraint form, which keeps `INTEGER PRIMARY KEY` as the rowid alias. A key of two
or more columns becomes a table constraint.

`SQLiteIndex` holds a `Keys` field and a `Where` field. A key is the SQL text of one key
part, so an expression key keeps its text. `PRAGMA index_info` gives a NULL name for an
expression, and `parseIndexDefinition` reads the text of that key from the `sql` column of
`sqlite_master`. An index that a `UNIQUE` constraint or a `PRIMARY KEY` builds holds no
`sql` row, and every key of that index comes from the PRAGMA name.

## PostgreSQL driver

The PostgreSQL driver connects through `pgx/v5/stdlib` with the driver name `pgx`. It
reads one schema. The `--schema` flag names that schema, and an empty value keeps the
schema of the search path of the connection string. `openPostgresConnection` writes the
name into the `search_path` runtime parameter with `pgx.ParseConfig`, so every query keeps
`current_schema()` and needs no change.

`Diff` calls `VerifySchema` for each side first. PostgreSQL accepts a search path that
names no schema, and it then reads an empty schema. Without that check the diff prints a
`DROP` statement for every object of the target.

| Data        | Source                                                   |
| ----------- | -------------------------------------------------------- |
| Tables      | `pg_class` with `relkind IN ('r', 'p')`                  |
| Materialized views | `pg_matviews`                                     |
| Columns     | `information_schema.columns`                             |
| Views       | `information_schema.views`                               |
| Constraints | `pg_constraint` with `pg_get_constraintdef(oid)`         |
| Indexes     | `pg_indexes`, without the indexes of a constraint        |
| Triggers    | `pg_trigger` with `pg_get_triggerdef(oid)` and `tgenabled` |
| Sequences   | `pg_sequences`, without the sequence that a column owns  |
| Enum types  | `pg_type` with `pg_enum`, in the order of `enumsortorder` |
| Domains     | `pg_type` with `typtype = 'd'`, and the checks of `pg_constraint` |
| Composite types | `pg_type` with `typtype = 'c'`, in the order of `attnum` |
| Functions   | `pg_proc` with `pg_get_functiondef(oid)`                 |
| Aggregates  | `pg_aggregate` with `pg_proc`                            |
| Operators   | `pg_operator`                                            |
| Extensions  | `pg_extension`                                           |
| Rules       | `pg_rules`                                               |
| Extended statistics | `pg_statistic_ext` with `pg_get_statisticsobjdef` |
| Replica identity | `pg_class.relreplident`, and `pg_index.indisreplident` for the index |
| Policies    | `pg_policies`                                            |
| Comments    | `obj_description` and `col_description`                  |
| Collations  | `pg_collation`, when `attcollation` differs from `typcollation` |
| Column storage | `pg_attribute.attstorage`, when it differs from `pg_type.typstorage` |
| Column statistics | `pg_attribute.attstattarget`                          |

`Diff` prints thirteen sections in this order: extensions, enum types, domains, composite
types, sequences, functions, aggregates, operators, tables, extended statistics, views,
materialized views, privileges. A table can use each of the first five, an aggregate and an
operator use a function, an extended statistics object names a table, and a materialized
view reads a table or a view. The privileges section comes last, because a GRANT statement
names an object that the other sections build. Without the `--privileges` flag that section
stays empty. Keep that order when you add a section.

`GetTables` reads `pg_class` with `relkind IN ('r', 'p')`, and not
`information_schema.tables`. The value `p` names a partitioned table, and `relispartition`
names a partition. The query reads `pg_get_partkeydef` and `pg_get_expr(relpartbound)`,
because `information_schema` reports neither. A partition takes the columns, the
constraints, and the indexes of its parent, so `DiffTable` returns no instruction for it
and `Instructions` prints one `CREATE TABLE ... PARTITION OF` statement. The index query
drops a row that `pg_inherits` names, because PostgreSQL builds that index from the index
of the parent. It also removes the keyword `ONLY`, which PostgreSQL writes for the index of
a partitioned table.

`pg_inherits` names the parent of a partition and the parent of a table of `INHERITS`.
`GetTables` tests `relispartition` to separate the two. Without that test a table of
`INHERITS` takes the statement of a partition, and that statement holds no bound, so
PostgreSQL rejects it.

`GetTables` sorts the tables with `sortTablesByDependency`. The name of a child can sort
before the name of its parent, and a statement needs the parent: a
`CREATE TABLE ... PARTITION OF` statement needs the parent of the partition, and a foreign
key needs the table that it names. `DiffTables` walks the target tables backward, because a
`DROP TABLE` statement takes the reverse order. Keep that sort and that direction.

PostgreSQL accepts no comment and no row level security option in a `CREATE TABLE`
statement, so `Instructions` prints a separate `COMMENT ON` statement and a separate
`ALTER TABLE ... ROW LEVEL SECURITY` statement after the `CREATE TABLE` statement.

The alias of `pg_collation` in the column query is `column_collation`. The word
`collation` is a reserved word of PostgreSQL, and a query that uses it as an alias fails
with a syntax error.

`DiffTables` prints every rule after every table. The action of a rule can name a second
table, so a rule that comes with its own table names a table that is not there yet.
`Instructions` returns no rule for that reason, and `DiffRules` and `RuleInstructions`
return them apart.

`GetMaterializedViews` reads `pg_matviews`. `information_schema` reports no materialized
view, so a query of `information_schema.views` finds none of them. The view keeps its
indexes, because `pg_indexes` holds them and `GetTable` reads no index of a view.

Each `Diff<Object>s` function returns a `SectionDiff`. That type holds an `EarlyRemovals`
field, an `Additions` field, and a `Removals` field. The source loop writes into
`Additions`, and the target loop writes into `Removals`. A modification that needs a `DROP`
statement and a `CREATE` statement stays in `Additions`, because it is one modification of
one object.

`Diff` prints the three parts in this order:

1. Every `EarlyRemovals`, in the REVERSE section order.
2. Every `Additions`, in the section order.
3. Every `Removals`, in the REVERSE section order.

PostgreSQL refuses a `DROP` statement while another object uses the object, so the
dependency must go away first. A new section needs no other work to get that order.

`EarlyRemovals` covers the object that blocks an addition of another section. `DiffViews`
writes every `DROP VIEW` statement into that field, because a view reads a column of a
table, and PostgreSQL refuses a change of that column while the view exists. A view that
changes gets its `DROP VIEW` in `EarlyRemovals` and its `CREATE VIEW` in `Additions`, so
the two statements wrap the table changes.

A type change of a column keeps the text of `view_definition` equal, so the definition
alone detects no change. `GetViews` reads the columns that each view uses from `pg_depend`
and `pg_rewrite`, and `PostgresView.HasEqualColumns` compares them.

Three rules keep the output free of noise:

- An object that an extension owns stays out of the diff. Each query excludes a row with
  a `pg_depend` entry of the type `e`. The `CREATE EXTENSION` statement recreates it.
- A `SERIAL` column and an identity column own their sequence. `GetSequences` excludes a
  sequence with a `pg_depend` entry of the type `a` or `i`. The `Serial` field of the
  column holds the word that builds that sequence again, and `GetTable` clears the default
  of such a column, because the word gives it.
- `pg_get_functiondef` writes the name of the schema in the header. `GetFunctions`
  removes that prefix, because the source schema and the target schema differ.

`PostgresFunction.Signature` joins the name and the identity arguments. PostgreSQL accepts
several functions with one name, so the name alone identifies nothing.

`PostgresSequence.Diff` returns one `ALTER SEQUENCE` statement with every attribute that
changes. Separate statements fail, because a new minimum above the current value is
invalid. `GetSequences` reads `last_value`, and `Diff` adds a `RESTART WITH` clause when
the current value falls outside the new range. Add that clause in no other case, because a
restart changes data.

`PostgresType.Diff` prints `ALTER TYPE ... ADD VALUE` when the target values are the first
values of the source. Every other change prints `DROP TYPE` and `CREATE TYPE`.

`GetTable` reads `attidentity` and `attgenerated` for each column. `pg_attrdef` holds the
expression of a stored generated column, and it holds the default value of every other
column. The query separates the two with a `CASE` expression. Keep that step. Without it a
generated column becomes a column with a `DEFAULT` clause, and PostgreSQL refuses a
`DEFAULT` expression that reads another column.

Two rules fix the order of the identity actions. PostgreSQL refuses an identity on a column
that accepts a null value, so `PostgresAddIdentityAction` comes after the `NOT NULL` block.
PostgreSQL refuses to remove the `NOT NULL` flag of an identity column, so
`PostgresDropIdentityAction` comes before that block. `DiffTable` prints one action on each
side of the block for that reason.

PostgreSQL holds no action that changes the expression of a generated column. `DiffTable`
prints one `DROP COLUMN` action and one `ADD COLUMN` action in one statement. The column
holds no data of its own, so that pair loses no row.

PostgreSQL supports `ALTER TABLE ALTER COLUMN`. The driver prints one statement per change
of a type, of a `NOT NULL` flag, or of a default value. A type change gets a `USING`
clause when PostgreSQL holds no automatic cast between the two types. `HasAutomaticCast`
in `postgres_cast.go` asks the database, because the rules of `pg_cast` are long. A modified constraint, index, or
trigger becomes a `DROP` statement and a `CREATE` statement.

An index and a trigger keep the definition text that PostgreSQL returns. `String()` adds
the semicolon.

PostgreSQL writes the name of the schema into the definition text of a function, of an
index, and of a trigger. Each of the three queries removes the prefix of
`current_schema()` with `regexp_replace` and `quote_ident`. Without that step the text of
two equal objects differs, because the source schema and the target schema hold a
different name. The diff then prints a `DROP` statement and a `CREATE` statement for an
object that did not change, and the `CREATE` statement builds the object in the SOURCE
schema. Keep that step in every new query that reads a definition text.

A query that casts a name to `regclass` takes the name from `quoteIdentifier`. A query
that compares a name to a text column takes the raw name. `GetTable` passes both forms to
the index query.

PostgreSQL drops every constraint and every index of a column with the column. `DiffTable`
prints the constraint block and the index block BEFORE the column removals. Keep that
order. A `DROP CONSTRAINT` statement after a `DROP COLUMN` statement of the same column
fails, because the column removal dropped the constraint already.

`GetViews` sorts the views with `sortViewsByDependency`. A view can read a second view, so
a `CREATE VIEW` statement needs the views that it reads first, and a `DROP VIEW` statement
takes the reverse order. `DiffViews` walks the source views forward and the target views
backward.

## SQL sources

An argument names SQL text when the path ends in `.sql`, or when the path is a directory.
A path that holds `://` is a connection URL, so it never names SQL text. `IsSQLSource` in
`sql_source.go` holds that rule.

`NewSQLSource` reads the file list. A file gives a list of one. A directory gives the
`.sql` files of its top level, in the order of the names. It drops a file whose name ends
in `.down.sql`, because a down migration removes the schema that its up migration built. A
directory that holds no `.sql` file is an error.

`ApplyTo` sends each whole file in one `ExecContext` call. Do not add a statement splitter.
Both engines accept several statements in one call, and a correct splitter needs a parser,
because a function body of PostgreSQL holds a semicolon.

Each engine materializes a source in its own file:

- `sqlite_sql_source.go` writes a database into a directory of `os.MkdirTemp`. `Close`
  removes that directory.
- `postgres_sql_source.go` starts a `PostgresScratchServer` with
  `fergusstrange/embedded-postgres`. The server takes a free port of the loopback
  interface, and it holds one database for each side. `Close` stops it and removes its
  files.

`postgresScratchVersionOfConfig` selects the version of the temporary server. A comparison
of SQL text against a database reads the major version of that database with
`DetectPostgresScratchVersion`, and the temporary server takes the version of the same
major. The statements then match the engine that runs them. Two SQL sources give an empty
version, and the library then selects its default version. Do not name a version in the
code. A server that gives no version also gives an empty value, and the diff reports the
connection later.

Four settings of the temporary server matter. Keep them:

- The logger is `io.Discard`. The default logger of the library writes to the standard
  output, and dbdiff writes the SQL statements to that stream.
- The port comes from `findFreePort`. The default port of the library is 5432, and a real
  server holds that port.
- `Version` stays absent for an empty version. A call with an empty value gives an invalid
  configuration, and it also replaces the default of the library.
- `BinariesPath` names a stable directory of the cache of the user. Without it every run
  extracts the archive again. The name holds the version, and the default version takes the
  module version of the library, so a new library reads no stale binaries.

## Data comparison

The `--data` flag sets the `CompareData` field of the driver config. The field is false by
default, and the schema output stays the same in every case. `DiffData` lives in
`sqlite_data.go` and in `postgres_data.go`. Each engine holds its own copy, like every
other part of the diff.

`Diff` prints the data section after the whole schema section, because a new row needs its
table and its column.

- The comparison covers a table that the source and the target both hold. The schema
  section already creates or drops the other tables.
- The comparison needs the primary key of the table. A table with no primary key gets a
  comment line, and no row statement. A table with a different primary key in the target
  gets the same treatment.
- The output holds an `INSERT` statement for a key of the source only, an `UPDATE`
  statement for a key that both sides hold with a different row, and a `DELETE` statement
  for a key of the target only.
- `formatSQLiteValue` and `formatPostgresValue` make an SQL literal of each value first.
  The comparison then works on the literal, so `NULL` never equals the text `'NULL'`.

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

`DBDIFF_TEST_SKIP_POSTGRES=1` stops `TestPostgresDriver`, which needs that server. A runner
of macOS and a runner of Windows start no service container, so the CI gives that variable
and the `-short` flag to those two platforms. Keep the variable empty on Linux. A server
that fails there must fail the build, and a silent skip hides that failure.

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
| `RequireInstructions(expected)` | Compares the whole instruction list and returns it |
| `FetchAllFromTarget(table, ...)`| Reads the rows of the target as maps (SQLite)    |
| `WriteSQLFile(directory, name, content)` | Writes one `.sql` file of a SQL source  |

`NewTestSQLiteDriverWithPaths(tb, source, target)` builds a driver for two given paths. Use
it for a test of a SQL source. `NewTestSQLiteDriver` calls it with two database files.

A test that starts the temporary PostgreSQL server calls `t.Skip` under `testing.Short`,
because the first run downloads the server.

Every harness method calls `d.tb.Helper()` on its first line.

## How to write a test

1. Create the harness with `NewTestSQLiteDriver(t)` or `NewTestPostgresDriver(t)`.
2. Build the wanted schema with `ExecOnSource`.
3. Build the old schema with `ExecOnTarget`. Insert rows when the change moves data.
4. Compare the whole output with `RequireInstructions`. Write the expected instructions as
   Go values.
5. Apply the diff with `driver.ExecOnTarget(diff)`. This step proves that the SQL runs.
6. If the change moves data, read the rows with `FetchAllFromTarget` and compare them with
   `require.Equal`.

Rules:

- Compare the whole instruction list. Never compare one instruction, and never compare the
  SQL text. `instruction_test.go` covers the text.
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

## Comments

Write no comment. That is the default. A good name and a small function tell the reader what
the code does.

One test decides each comment. If the removal of the comment lets a reader break the code,
keep the comment. In every other case, delete the comment.

Four kinds of comment pass the test:

- An order that a later change can break. Example: "PostgreSQL refuses to drop the index
  that the replica identity of the target holds, so this block comes first."
- A rule of the engine that the code cannot show. Example: "SQLite refuses an ADD COLUMN
  action that holds a STORED generated column."
- A step that looks unnecessary. Name what breaks without the step. Example: "Without this
  step the diff prints a DROP statement for an object that did not change."
- A field or a branch that the code near it ignores. Example: "String writes no comment,
  because CREATE TABLE accepts none."

Delete every other comment. These five kinds fail the test:

- A sentence that repeats the name of a function, of a type, or of a field. Delete
  `// GetOwners returns the owner of each object of the schema.`
- A sentence that reads the code aloud. Delete `// The loop walks the source columns and it
  appends each new column.`
- The SQL synopsis above an instruction type. The `String` method below holds the same text.
- The same reason in two places. Keep the reason at the place that a reader changes first.
  A test gets no copy of a reason that the driver holds already.
- The empty value of a field. The declaration `Collation string` tells enough.

Write the reason alone. Do not open a comment with the name of the item below it. Write
"PostgreSQL accepts a comment in no CREATE statement." Do not write "CommentInstructions
returns the statement of the comment. PostgreSQL accepts a comment in no CREATE statement."

This rule covers a test file too. A subtest name is a short noun phrase, and that name
replaces a comment above the subtest.

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

## Struct literals

**A struct literal declares one field per line.** A literal that sets one field or more
splits every field onto its own line, with the closing brace on its own line. This rule
covers a type of this repository and a standard-library type such as `sql.NullString`,
with no exception.

**An empty literal stays on one line.** It sets no field, so it has nothing to split:
`&PostgresDropDomainDefaultAction{}`.

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
- Write the subject only. The subject gives enough information, so a commit takes no body.
- Write no trailer. Do not add a `Claude-Session:` trailer, and do not add another trailer.
  This rule covers a new commit and a commit that you write again.
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

This section records the state of the repo on 2026-08-17. It is not a rule set. Correct an
item when your task touches it.

- The data comparison prints no row of a table that the source only holds. The schema
  section creates that table, and the table stays empty.
- The `--privileges` flag compares the owner and the privileges of an object of `pg_class`.
  It reads no privilege of a schema, of a function, or of a type, and no default privilege.
- The PostgreSQL driver compares one schema for each run. The `--schema` flag selects that
  schema. This item is a boundary of the tool, not a defect. To compare two schemas, run
  dbdiff two times. The driver prints no `CREATE SCHEMA` statement, and it detects no
  object that moved from one schema to another schema.
- A SQL source reads no annotation of a migration tool. A goose file holds the up migration
  and the down migration in one file, behind a `-- +goose` comment. dbdiff applies both
  parts. A golang-migrate directory and a directory of numbered files work.
- A SQL source reads the top level of a directory only. It reads no subdirectory.
