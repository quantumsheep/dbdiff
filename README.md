<div align="center">

# dbdiff

**dbdiff reads the schema of two databases. Then it prints the SQL statements that make the second schema equal to the first one.**

[![Tests](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml/badge.svg)](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml)
[![Release](https://img.shields.io/github/v/release/quantumsheep/dbdiff?label=release)](https://github.com/quantumsheep/dbdiff/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/quantumsheep/dbdiff.svg)](https://pkg.go.dev/github.com/quantumsheep/dbdiff)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

```console
$ dbdiff current.sqlite final.sqlite
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
CREATE VIEW active_users AS SELECT id, email FROM users;
DROP TABLE "audit";
```

- **Two engines.** dbdiff supports SQLite and PostgreSQL.
- **Databases and files.** Each side is a database, a `.sql` file, or a directory of `.sql` migration files.
- **One binary.** Each release holds a file for Linux, for Windows, and for macOS.
- **Write on request.** `dbdiff diff` prints the statements and changes no database. `dbdiff
  migrate up` applies a migration file to a database.
- **Rows.** dbdiff compares the schema by default. The `--data` flag adds the comparison of the rows.

---

## Contents

- [Installation](#installation)
- [Usage](#usage)
- [Flags](#flags)
- [Driver detection](#driver-detection)
- [SQLite](#sqlite)
- [PostgreSQL](#postgresql)
- [SQL files](#sql-files)
- [Data comparison](#data-comparison)
- [Migrations](#migrations)
- [Supported objects](#supported-objects)
- [Limits](#limits)
- [Development](#development)
- [License](#license)

## Installation

### From a release

Download a binary from the [releases](https://github.com/quantumsheep/dbdiff/releases)
page. Each release holds a binary for Linux, for Windows, and for macOS, on amd64 and on
arm64.

### With Go

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

> [!NOTE]
> The SQLite driver is a C binding. If the build fails with an undefined symbol, set
> `CGO_ENABLED=1` before the build.

## Usage

dbdiff takes seven commands:

| Command                           | Result                                                           |
| ---------------------------------- | ------------------------------------------------------------------ |
| `dbdiff diff <source> <target>`    | Print the statements that make the source equal to the target     |
| `dbdiff migrate generate <name>`   | Write the next migration file                                     |
| `dbdiff migrate status`            | List each migration and its state                                 |
| `dbdiff migrate preview`           | Print the statements that `up` will run                           |
| `dbdiff migrate up`                | Apply every pending migration                                     |
| `dbdiff migrate step`              | Apply every pending migration, and ask before each file           |
| `dbdiff migrate verify`            | Compare the database against the replay of the applied migrations |

A command line with no command name runs the `diff` command:

```bash
dbdiff current.sqlite final.sqlite
dbdiff diff current.sqlite final.sqlite
```

Both lines give the same result. A database file named `migrate` needs the form
`./migrate`. Without that form, dbdiff reads the bare name `migrate` as a command name:

```bash
dbdiff ./migrate final.sqlite
```

Read [Migrations](#migrations) for the six commands of `migrate`. The rest of this section
covers `diff`.

`diff` takes two arguments:

```bash
dbdiff diff [flags] <source> <target>
```

The first argument is the source. It holds the current schema. The second argument is the
target. It holds the final schema. The output changes the source into the target.

| Command                                   | Result                                           |
| ------------------------------------------ | ------------------------------------------------ |
| `dbdiff diff current.sqlite final.sqlite` | Compare two SQLite files                         |
| `dbdiff diff current.sqlite schema.sql`   | Compare a database against a SQL file            |
| `dbdiff diff current.sqlite ./migrations` | Compare a database against a migration directory |

The output holds one SQL statement per line:

```sql
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
DROP TABLE "audit";
CREATE VIEW active_users AS SELECT id, email FROM users;
```

dbdiff writes the statements to the standard output. It changes no database. To apply the
statements, send them to the client of the engine:

```bash
dbdiff diff current.sqlite final.sqlite | sqlite3 current.sqlite
```

> [!CAUTION]
> Read the output before you apply it. A statement can delete a table, a column, or a row
> of the source database. dbdiff holds no rollback.

### Comments

The `--comments` flag prints a comment before each object that the output changes. The
statements of one object take one comment:

```sql
-- Modify the table "users"
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
-- Create the index "users_email" of the table "users"
CREATE INDEX "users_email" ON "users" ("email");
-- Drop the table "audit"
DROP TABLE "audit";
```

## Flags

### `diff`

| Flag       | Value                   | Purpose                                                                                                                        |
| ---------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `--driver` | `sqlite3` or `postgres` | Select the database engine. The default value comes from the source and the target. See [Driver detection](#driver-detection). |
| `--schema` | A schema name           | Name the schema that the postgres driver reads. The default value is the schema of the search path.                            |
| `--data`   | none                    | Add the comparison of the rows. The default value is off.                                                                      |
| `--comments` | none                  | Print a comment before each object that the output changes. The default value is off.                                          |
| `--privileges` | none               | Add the comparison of the owner and the privileges. The postgres driver accepts this flag. The default value is off.            |

### `dbdiff`

`--version` is a global option of the `dbdiff` command itself, not of `diff` or of
`migrate`. Give it before the command name:

```bash
dbdiff --version
```

### `migrate generate`, `migrate status`, `migrate preview`, `migrate up`, `migrate step`, and `migrate verify`

| Flag           | Value                   | Purpose                                                                                                |
| -------------- | ----------------------- | --------------------------------------------------------------------------------------------------------- |
| `--config`     | A file path             | Name the configuration file. The default value is `dbdiff.yaml` of the working directory.                 |
| `--driver`     | `sqlite3` or `postgres` | Select the database engine. This flag overrides the key `driver` of the configuration file.               |
| `--source`     | A directory             | Name the directory of the migration files. This flag overrides the key `source` of the configuration file. |
| `--target`     | A database, a `.sql` file, or a directory of `.sql` files | Name the target. For `generate`, the target is the wanted schema. For the five other commands, the target is the database that the migrations change. This flag overrides the key `target` of the configuration file. |
| `--schema`     | A schema name           | Name the schema that the postgres driver reads. This flag overrides the key `schema` of the configuration file. |

`migrate preview` also accepts one flag of its own:

| Flag    | Value | Purpose                                                                                |
| ------- | ----- | ----------------------------------------------------------------------------------------- |
| `--run` | none  | Apply the statements of every pending file in one transaction, and roll that transaction back. The default value is off. |

Read [Migrations](#migrations) for the five keys of the configuration file.

## Driver detection

If you give no `--driver` flag, dbdiff reads the engine from the source and the target:

| Argument                                                    | Driver     |
| ----------------------------------------------------------- | ---------- |
| A path with the prefix `sqlite://`                          | `sqlite3`  |
| A URL with the prefix `postgres://` or `postgresql://`      | `postgres` |
| A connection string of the form `host=localhost dbname=app` | `postgres` |
| A `.sql` file or a directory                                | none       |
| Another path                                                | `sqlite3`  |

One argument is sufficient. In this example the source names the engine, and dbdiff applies
`schema.sql` to a temporary PostgreSQL server:

```bash
dbdiff postgres://user:password@localhost:5432/production schema.sql
```

<details>
<summary>The two cases that give an error</summary>

In the first case the two arguments name SQL text, so no argument names an engine:

```bash
dbdiff old_schema.sql new_schema.sql
# dbdiff: cannot detect the driver of "old_schema.sql" and "new_schema.sql". Use the --driver flag
```

In the second case the two arguments name a different engine:

```bash
dbdiff sqlite://current.db postgres://user:password@localhost:5432/final
# dbdiff: "sqlite://current.db" names the sqlite3 driver and "postgres://user:password@localhost:5432/final" names the postgres driver. Use the --driver flag
```

</details>

Give the `--driver` flag to correct the two cases. The flag has priority, so dbdiff runs no
detection when you give it.

## SQLite

The driver accepts a file path, or a path with the prefix `sqlite://`:

```bash
dbdiff current.sqlite final.sqlite
dbdiff sqlite://current.sqlite sqlite://final.sqlite
```

SQLite holds no schema. If you give the `--schema` flag with this driver, dbdiff gives an
error.

**Table recreation.** SQLite holds no `ALTER COLUMN` statement. If a column changes, or if
a foreign key changes, the driver recreates the table. The recreation copies the rows into
a new table, drops the old table, and renames the new table. A new column takes its default
value, or `NULL`.

**Generated columns.** The driver keeps a `STORED` generated column and a `VIRTUAL` one.
The `INSERT` statement of a table recreation names no generated column, because SQLite
computes that column. SQLite refuses an `ADD COLUMN` action that holds a `STORED` generated
column, so a new column of that kind recreates the table.

**Column attributes.** No `PRAGMA` statement reports a collation, the keyword
`AUTOINCREMENT`, or a check. The driver reads each of them from the `CREATE TABLE`
statement of `sqlite_master`. It reads the table options `WITHOUT ROWID` and `STRICT` from
the same text. A change of one of these needs a new table, because SQLite holds no
`ALTER COLUMN` action.

**View triggers.** SQLite holds an `INSTEAD OF` trigger on a view. The driver compares the
triggers of a view, and it builds each of them again after a `DROP VIEW` statement, because
that statement removes every trigger of the view.

**Rename detection.** The driver detects a renamed column. A target column that the source
does not hold, and that holds the attributes of exactly one free source column, is a
rename. Two candidates make the guess unsafe. In that case the column becomes an addition,
and the old column becomes a removal.

## PostgreSQL

Give a connection string for each side:

```bash
dbdiff --driver postgres \
  postgres://user:password@localhost:5432/source \
  postgres://user:password@localhost:5432/target
```

The `--schema` flag names one schema. The driver reads that schema in the source database
and in the target database:

```bash
dbdiff --driver postgres --schema app \
  postgres://user:password@localhost:5432/source \
  postgres://user:password@localhost:5432/target
```

Without that flag, the search path of the connection string selects the schema. The
default schema is `public`. If a database holds no schema with the given name, dbdiff
gives an error.

**Section order.** The driver prints thirteen sections in this order:

```
extensions → enum types → domains → composite types → sequences
  → functions → aggregates → operators → tables → extended statistics
  → views → materialized views → privileges
```

A table can use each of the first five objects. A materialized view reads a table or a
view. That order gives each statement the objects that it needs. The privileges section
comes last, because a `GRANT` statement names an object that the other sections build.
Without the `--privileges` flag that section stays empty.

**Owned objects.** An object that an extension owns stays out of the output. The
`CREATE EXTENSION` statement builds that object again. A sequence that a `SERIAL` column or
an identity column owns stays out of the output for the same reason.

**Serial columns.** The driver writes the word `serial`, `bigserial`, or `smallserial` in
the definition of a column that owns its sequence. That word builds the sequence, so the
output holds no `CREATE SEQUENCE` statement for it. The definition holds no `DEFAULT`
clause, because the word gives the column that default.

**Comments.** The driver compares the comment of a table and the comment of a column.
PostgreSQL accepts a comment in no `CREATE` statement, so the output prints a separate
`COMMENT ON` statement. A comment that goes away gives the keyword `NULL`.

**Row level security.** The driver compares the two switches of a table and each policy of
it. PostgreSQL holds no action that changes a policy, so a changed policy prints a
`DROP POLICY` statement and a `CREATE POLICY` statement.

**Collations.** The driver keeps the collation of a column when that collation differs from
the collation of the type. PostgreSQL changes a collation through the `TYPE` action, so the
output prints `ALTER COLUMN ... TYPE ... COLLATE ...`.

**Partitioned tables.** The driver keeps the `PARTITION BY` clause of a parent, and it
prints one `CREATE TABLE ... PARTITION OF` statement for each partition. A partition takes
the columns, the constraints, and the indexes of its parent, so the output names none of
them. A `DROP TABLE` statement of a parent removes every partition of it, so the output
prints no second statement for those partitions.

**Table order.** The driver sorts the tables so that a table comes after each table that
it needs: the parent of a partition, the parent of an `INHERITS` table, and each table that
a foreign key names. The `DROP TABLE` statements take the reverse order.

**Foreign keys.** A `CREATE TABLE` statement holds no foreign key. The output prints one
`ALTER TABLE ... ADD CONSTRAINT` statement for each foreign key of a new table, after every
table. Two tables can name each other, and no order of two such statements works. A
partition takes the foreign keys of its parent, so the output prints none for it.

**Storage parameters.** The driver compares the `WITH` options of a table, for example
`fillfactor`. A parameter that the target does not hold takes a `RESET` action, which gives
that parameter its default value again.

**Unlogged tables.** The driver keeps the `UNLOGGED` keyword of a table. A change of that
keyword prints `ALTER TABLE ... SET LOGGED` or `ALTER TABLE ... SET UNLOGGED`.

**Column storage and statistics.** The driver keeps the storage mode of a column, and it
keeps the statistics target of a column. A column definition accepts neither, so the output
prints a separate `ALTER TABLE ... ALTER COLUMN` statement after the `CREATE TABLE`
statement. An `ALTER COLUMN ... TYPE` action gives the column the storage mode of the new
type, so the output sets the mode again after that action. A column that keeps the mode of
its type takes `SET STORAGE DEFAULT`, and PostgreSQL 16 accepts that mode. A column that
keeps the default target of the server takes `SET STATISTICS -1`.

**Trigger modes.** The driver keeps the mode of a trigger: `ENABLE`, `DISABLE`,
`ENABLE REPLICA`, or `ENABLE ALWAYS`. A `CREATE TRIGGER` statement accepts no mode, so the
output prints a separate `ALTER TABLE ... TRIGGER` statement after it. PostgreSQL builds
every trigger with `ENABLE`, so that mode needs no statement.

**Replica identity.** The driver keeps the replica identity of a table. Logical replication
reads that mode to identify a row of the table. The mode `USING INDEX` names an index, so
the output prints the `CREATE INDEX` statement of that index first. The output changes the
mode before a `DROP INDEX` statement, because PostgreSQL refuses to drop the index that the
replica identity of the source holds.

**Table inheritance.** A table of `INHERITS` is no partition. The driver prints a
`CREATE TABLE ... INHERITS` statement for it, and it keeps every column of that table.
PostgreSQL merges a column that the parent and the child both declare.

**Privileges.** The `--privileges` flag adds the comparison of the owner and of the
privileges. A role belongs to the server and not to the schema, so that comparison stays off
by default. A target server holds other role names in most cases, and a `GRANT` statement of
a role that is absent fails.

```bash
dbdiff --driver postgres --privileges \
  postgres://user:password@localhost:5432/source \
  postgres://user:password@localhost:5432/target
```

**Extended statistics.** The driver compares each `CREATE STATISTICS` object. Such an
object names a table, so the output prints it after the tables. PostgreSQL holds no action
that changes the columns of the object, so a new definition prints a `DROP` statement and a
`CREATE` statement.

**Rules.** The driver compares the rules of a table. PostgreSQL holds no action that
changes a rule, so a new definition prints a `DROP RULE` statement and a `CREATE RULE`
statement. A view holds an implicit `_RETURN` rule, and the output names no such rule.

**View check options.** The driver keeps `WITH LOCAL CHECK OPTION` and
`WITH CASCADED CHECK OPTION`. The query text of a view holds none of that clause, so a new
option alone prints a `DROP VIEW` statement and a `CREATE VIEW` statement.

**Materialized views.** The driver compares the query and the indexes of a materialized
view. A changed query prints a `DROP MATERIALIZED VIEW` statement and a
`CREATE MATERIALIZED VIEW` statement, because PostgreSQL holds no action that replaces the
query. The output builds each index of the view again after that pair.

**Identity columns.** The driver keeps `GENERATED ALWAYS AS IDENTITY` and
`GENERATED BY DEFAULT AS IDENTITY`. It keeps the options of the sequence of that column,
for example `START WITH 100 INCREMENT BY 5`, and it prints only an option that differs from
the default of the type. If a column becomes an identity column, the output sets
the `NOT NULL` flag first, because PostgreSQL refuses an identity on a column that accepts a
null value. If a column stops to be an identity column, the output prints `DROP IDENTITY`
first, for the same reason in reverse.

**Generated columns.** The driver keeps `GENERATED ALWAYS AS (expression) STORED`.
PostgreSQL holds no action that changes the expression of a generated column, so a new
expression prints one `DROP COLUMN` action and one `ADD COLUMN` action in one statement. The
column holds no data of its own, so that pair loses no row.

## SQL files

An argument names SQL text in two cases. The first case is a path that ends in `.sql`. The
second case is a directory. dbdiff reads the `.sql` files of the top level of that
directory, sorts the names, and applies the files in that order. It skips a file whose name
ends in `.down.sql`, because a down migration removes the schema that its up migration
built.

```bash
dbdiff schema.sql production.sqlite
dbdiff ./migrations production.sqlite
dbdiff --driver sqlite3 old_schema.sql new_schema.sql
```

A connection URL holds `://`, so a URL never names SQL text.

Two SQL sources name no engine. Give the `--driver` flag in that case. See
[Driver detection](#driver-detection).

dbdiff applies the SQL to a temporary database, and then it compares that database. The
`--driver` flag names the dialect of the files, and it names the engine of the temporary
database.

| Driver     | Temporary database                                                                                                                                                            |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sqlite3`  | A temporary SQLite file. It needs no other program.                                                                                                                           |
| `postgres` | A temporary PostgreSQL server on a free port of the loopback interface. The first run downloads that server. Later runs read the binaries of the cache directory of the user. |

The temporary PostgreSQL server takes the version of the database of the other side. This
example reads the version of `production`, and it applies `schema.sql` to a server of that
version:

```bash
dbdiff --driver postgres schema.sql postgres://user:password@localhost:5432/production
```

Two SQL files give no version, so the temporary server takes the default version:

```bash
dbdiff --driver postgres old_schema.sql new_schema.sql
```

dbdiff removes the temporary database at the end of the run. It changes no file of the
source.

### Limits of a SQL file

- The SQL must be correct for the engine that the `--driver` flag names.
- dbdiff reads no annotation of a migration tool. A goose file holds the up migration and
  the down migration in one file, behind a `-- +goose` comment. dbdiff applies both parts,
  so a goose directory gives a wrong schema. A golang-migrate directory and a directory of
  numbered files work.
- dbdiff reads the top level of the directory only. It reads no subdirectory.
- dbdiff applies each file in one call. PostgreSQL runs the statements of one call in an
  implicit transaction block, and it refuses `CREATE INDEX CONCURRENTLY` there. Write the
  line `-- dbdiff:no-transaction` in such a file. dbdiff then runs one call for each
  statement of that file.
- A directory that holds no `.sql` file gives an empty schema. dbdiff then prints one
  `DROP` statement for each object of the other side.

## Data comparison

The `--data` flag adds the comparison of the rows:

```bash
dbdiff --data current.sqlite final.sqlite
```

The data section comes after the schema section, because a new row needs its table and its
column. The output holds three kinds of statement:

| Statement | Case                                            |
| --------- | ----------------------------------------------- |
| `INSERT`  | A key that the target only holds                |
| `UPDATE`  | A key that both sides hold with a different row |
| `DELETE`  | A key that the source only holds                |

The comparison needs the primary key of the table. A table with no primary key gets a
comment line, and no row statement. A table with a different primary key in the source gets
the same treatment.

## Migrations

`dbdiff migrate` reads a configuration file, `dbdiff.yaml`, from the working directory. The
`--config` flag names another path. The file holds five keys:

| Key          | Purpose                                                                             |
| ------------ | -------------------------------------------------------------------------------------- |
| `driver`     | The database engine. The value is `sqlite3` or `postgres`.                             |
| `source`     | The directory that holds the migration files.                                          |
| `target`     | The target. A database, a `.sql` file, or a directory of `.sql` files.                  |
| `schema`     | The name of the schema. The postgres driver reads this key.                            |
| `version`    | The version of the real PostgreSQL server. Read the note below.                        |

```yaml
driver: postgres
source: ./migrations
target: ./schema.sql
schema: app
version: 17.11.0
```

The `target` key of the file holds the wanted schema of `migrate generate`. The five other
commands change a database, and they name that database with the `--target` flag. Do not
put a production connection string in a file that goes into git. Give the `--target` flag,
or set the `DBDIFF_TARGET` variable:

```bash
export DBDIFF_TARGET=postgres://user:password@localhost:5432/production
dbdiff migrate up
```

`migrate generate` reads no database, because it compares the target against the replay of
the migration files. That command needs the `driver` key or the
`--driver` flag. Both sides of the comparison are SQL sources, and detection needs one
side to name an engine.

With the postgres driver, `migrate generate` runs both sides on a temporary server. Neither
side is a real database, so dbdiff cannot read the version of the server that will later
run the file. Set the `version` key to that version. Without it, the temporary server takes
the default version of its library, and that version can hold syntax that the real server
refuses. Name the same major version as the database, for example the version that
`SELECT version();` reports on the database.

### The commands

`migrate generate` compares the target against the replay of the migration directory, and
it writes the difference as a new file:

```console
$ dbdiff migrate generate add_created_at
migrations/20260822143000_add_created_at.sql
```

`migrate status` lists each migration file and its state:

```console
$ dbdiff migrate status
20260822143000_add_created_at            applied   2026-08-22T14:35:00Z
20260822150000_add_index                 pending
```

When a file is `changed` or `missing`, `status` exits with the code 1. When a file is only
`out of order`, it exits with the code 0.

`migrate preview` prints one block for each pending migration file, and it changes no
database:

```console
$ dbdiff migrate preview
20260822150000_add_index
  -- dbdiff v1.4.0
  -- generated 2026-08-22T15:00:00Z

  -- Create the index "users_email" of the table "users"
  CREATE INDEX "users_email" ON "users" ("email");
20260822151500_add_status
  -- dbdiff v1.4.0
  -- generated 2026-08-22T15:15:00Z

  -- Modify the table "users"
  ALTER TABLE "users" ADD COLUMN "status" TEXT;
```

Each block holds the whole text of the file, header included. `preview` prints no position
such as `[1/2]`, because a file is no longer a list of statements to step through.

An out of order file names no statement. Instead, `preview` prints a line that names the
file and states that `up` will refuse it.

Give the `--run` flag to prove that the statements run. That flag applies every pending
file in one transaction, and it rolls that transaction back. Each file reads the objects of
the files before it. `preview --run` skips a file that holds the directive
`-- dbdiff:no-transaction`, and it prints a line that names the file. dbdiff cannot roll
such a file back. Read [The no-transaction directive](#the-no-transaction-directive).

`migrate up` applies every pending migration file. Each file runs in one transaction:

```console
$ dbdiff migrate up
Applied 20260822150000_add_index.
```

`migrate step` applies every pending file, and it asks a question before each file:

```console
$ dbdiff migrate step
20260822150000_add_index
  -- dbdiff v1.4.0
  -- generated 2026-08-22T15:00:00Z

  -- Create the index "users_email" of the table "users"
  CREATE INDEX "users_email" ON "users" ("email");
  [a]pply  apply the [r]est  [q]uit ? a
Applied 20260822150000_add_index.
20260822151500_add_status
  -- dbdiff v1.4.0
  -- generated 2026-08-22T15:15:00Z

  -- Modify the table "users"
  ALTER TABLE "users" ADD COLUMN "status" TEXT;
  [a]pply  apply the [r]est  [q]uit ? q
The run stopped. Every file that dbdiff applied stays applied.
```

`step` gives three answers: apply the file, apply the rest of the run, and quit. A quit
stops the run, and it does not roll back a file that already committed. Each file runs in
its own transaction, and dbdiff opens that transaction only after you answer. `step` needs
a terminal, because it reads the answer from the standard input. A closed input gives an
error that names the `up` command. Use `step` to review a migration on a machine with a
terminal. Use `up` for an automated run, for example a deploy script.

`step` holds the migration lock for the whole run, so another dbdiff process waits while a
person answers the prompts. `up` suits a script, and `step` suits a person at a terminal.

`step` gives no skip answer. If a step skips a file, the files after it run against a
schema that the skipped file was to build.

`migrate verify` builds a temporary schema from the migration files that the database
already applied, and it compares that schema against the database:

```console
$ dbdiff migrate verify
The database holds the schema of the migrations.
```

When `verify` finds a difference, it prints the difference and exits with the code 1. A
pipeline can use `migrate verify` as a gate.

### The migration file

A migration file is named `<version>_<name>.sql`, for example
`20260822143000_add_created_at.sql`. The version is a UTC timestamp. The file holds a
header, and then the comment and the statement of each change:

```sql
-- dbdiff v1.4.0
-- generated 2026-08-22T14:30:00Z

-- Modify the table "users"
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
```

`migrate up` and `migrate step` send the whole file to the engine in one call. Read
[Limits](#limits) for what that means for a failure.

### The no-transaction directive

A file that holds the line `-- dbdiff:no-transaction`, alone on the line, runs outside a
transaction. dbdiff reads no other value on that line. The directive covers a statement
that refuses a transaction, for example `CREATE INDEX CONCURRENTLY`, `VACUUM`, and
`ALTER SYSTEM` of PostgreSQL, and `VACUUM` of SQLite. `PRAGMA foreign_keys` of SQLite is
also inert inside a transaction. dbdiff generates none of these statements.

```sql
-- dbdiff:no-transaction
CREATE INDEX CONCURRENTLY "users_email_idx" ON "users" ("email");
```

With the postgres driver, put one statement in a file that holds the directive. dbdiff
sends the whole file to the engine in one call. PostgreSQL puts a call of several
statements into a transaction of its own. The SQLite driver accepts several statements in
such a file.

> [!WARNING]
> The directive costs the atomicity guarantee of a migration, and dbdiff cannot restore
> it. Two failures can follow. The file can apply, and then fail to write its history row.
> The next run then applies the file again. The file can also fail halfway. Part of the
> file stays applied, and dbdiff records no row for it. The next run then applies the file
> from the start.

`preview --run` skips a file that holds the directive, and it prints a line that names the
file. A rollback cannot undo such a file, so `preview --run` never applies it.

### The history table

`dbdiff migrate` records each applied file in the table `dbdiff_migrations`, with the
columns `version`, `name`, `checksum`, and `applied_at`. The checksum detects a file that
changed after its migration ran. `migrate status` and `migrate verify` report such a file
as `changed`.

## Supported objects

| Object          | SQLite                                  | PostgreSQL |
| --------------- | --------------------------------------- | ---------- |
| Tables          | ✅                                      | ✅         |
| Identity columns  | ➖                                    | ✅         |
| Table options     | ✅ (WITHOUT ROWID, STRICT)            | ➖         |
| Virtual tables    | ✅                                    | ➖         |
| Generated columns | ✅                                    | ✅         |
| Column storage and statistics | ➖                        | ✅         |
| Indexes         | ✅                                      | ✅         |
| Constraints     | ✅ (foreign keys, primary keys, unique, checks) | ✅  |
| Triggers        | ✅ (of a table and of a view)           | ✅ (with the mode) |
| Views           | ✅                                      | ✅         |
| Materialized views | ➖                                   | ✅         |
| Rules             | ➖                                    | ✅         |
| Extended statistics | ➖                                  | ✅         |
| Partitioned tables | ➖                                   | ✅         |
| Replica identity  | ➖                                    | ✅         |
| Sequences       | ➖                                      | ✅         |
| Enum types      | ➖                                      | ✅         |
| Domains         | ➖                                      | ✅         |
| Composite types | ➖                                      | ✅         |
| Functions       | ➖                                      | ✅         |
| Procedures      | ➖                                      | ✅         |
| Aggregates      | ➖                                      | ✅         |
| Operators       | ➖                                      | ✅         |
| Extensions      | ➖                                      | ✅         |
| Comments        | ➖                                      | ✅         |
| Row level security | ➖                                   | ✅         |
| Privileges      | ➖                                      | ✅ (`--privileges`) |
| Data            | ✅                                      | ✅         |

✅ dbdiff compares this object. ➖ the engine holds no such object. A table covers its
columns.

dbdiff does not support MySQL.

The SQLite driver compares a virtual table, for example an `fts4` table. It replays the
`CREATE VIRTUAL TABLE` statement, and it names no shadow table of the module. SQLite holds
no `ALTER` statement for such a table, so a new definition prints a `DROP` statement and a
`CREATE` statement. The `.sql` file of a source must name a module that the build holds.

The SQLite driver keeps the name of a table constraint. A column constraint holds no name,
so a named `UNIQUE` constraint of one column stays a table constraint.

The SQLite driver keeps the `DEFERRABLE` clause of a foreign key. SQLite writes a key of
one column as a column constraint or as a table constraint, and the driver reads the clause
from either form.

The SQLite driver keeps the `ON CONFLICT` clause of a `PRIMARY KEY`, of a `UNIQUE`
constraint, and of a `NOT NULL` constraint. A new clause recreates the table.

The SQLite driver keeps the direction and the collation of each key of an index. The
keyword `ASC` is the default of SQLite, so an index that names it equals an index that does
not.

The SQLite driver compares a partial index and an index that an expression builds. It
prints a primary key of one column and a `UNIQUE` constraint of one column in the
definition of that column. It prints a primary key of two or more columns and a `UNIQUE`
constraint of two or more columns as a table constraint.

## Limits

- The data comparison covers a table that the source and the target both hold. A table
  that the target only holds stays empty. The schema section creates that table.
- The `--privileges` flag compares the owner and the privileges of a table, of a view, of a
  materialized view, and of a sequence. It compares no privilege of a schema, of a function,
  or of a type, and it reads no default privilege of `ALTER DEFAULT PRIVILEGES`.
- A column that keeps the storage mode of its type takes `SET STORAGE DEFAULT`.
  PostgreSQL 16 accepts that mode, and an older server refuses it.
- The PostgreSQL driver compares one schema for each run. To compare two schemas, run
  dbdiff two times. The driver prints no `CREATE SCHEMA` statement, and it detects no
  object that moved from one schema to another schema.
- A SQL source of the postgres driver needs a download on the first run. Read
  [Limits of a SQL file](#limits-of-a-sql-file) for the other limits.
- dbdiff generates no down migration. A rollback is a restore from a backup.
- The name `dbdiff_migrations` is reserved. `dbdiff diff` hides a table with that name.
- `migrate generate` needs the name of the engine, because both of its sides are SQL
  sources.
- With the postgres driver, a `migrate generate` whose `target` is a `.sql` file or a
  directory of `.sql` files needs the `version` key of `dbdiff.yaml`. Without it, the
  generated file can hold syntax that the real target server refuses.
- `migrate preview --run` applies every pending file in one transaction. A file that reads
  a value that an earlier file added to an enum type fails there. `migrate up` applies that
  file without error, because `up` commits each file.
- `migrate up` and `migrate step` send a whole migration file to the engine in one call.
  dbdiff splits no statement. A file that holds a statement that the engine refuses gives
  an error that names the file, and not the position of the statement.

## Development

```bash
docker compose up -d                      # Start PostgreSQL on port 5432
go build -o ./bin/dbdiff ./cmd/dbdiff     # Build the binary
go test ./...                             # Run the tests
```

The PostgreSQL tests need the database at `postgres://user:password@localhost:5432/dbdiff`.
The command `docker compose up -d` starts that database. The SQLite tests need no service,
because each test writes into a temporary directory.

To run the tests without that database, give the variable and the flag that the CI gives to
macOS and to Windows:

```bash
DBDIFF_TEST_SKIP_POSTGRES=1 go test -short ./...
```

## License

dbdiff uses the MIT license. Read the [LICENSE](LICENSE) file.
