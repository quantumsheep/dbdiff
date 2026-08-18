<div align="center">

# dbdiff

**dbdiff reads the schema of two databases. Then it prints the SQL statements that make the second schema equal to the first one.**

[![Tests](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml/badge.svg)](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml)
[![Release](https://img.shields.io/github/v/release/quantumsheep/dbdiff?label=release)](https://github.com/quantumsheep/dbdiff/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/quantumsheep/dbdiff.svg)](https://pkg.go.dev/github.com/quantumsheep/dbdiff)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

```console
$ dbdiff source.sqlite target.sqlite
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
CREATE VIEW active_users AS SELECT id, email FROM users;
DROP TABLE "audit";
```

- **Two engines.** dbdiff supports SQLite and PostgreSQL.
- **Databases and files.** Each side is a database, a `.sql` file, or a directory of `.sql` migration files.
- **One binary.** Each release holds a file for Linux, for Windows, and for macOS.
- **No write.** dbdiff prints the statements to the standard output. It changes no database.
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

dbdiff takes two arguments:

```bash
dbdiff [flags] <source> <target>
```

The first argument is the source. It holds the wanted schema. The second argument is the
target. The output changes the target.

| Command                              | Result                                           |
| ------------------------------------ | ------------------------------------------------ |
| `dbdiff source.sqlite target.sqlite` | Compare two SQLite files                         |
| `dbdiff schema.sql target.sqlite`    | Compare a SQL file against a database            |
| `dbdiff ./migrations target.sqlite`  | Compare a migration directory against a database |

The output holds one SQL statement per line:

```sql
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
DROP TABLE "audit";
CREATE VIEW active_users AS SELECT id, email FROM users;
```

dbdiff writes the statements to the standard output. It does not change the target
database. To apply the statements, send them to the client of the engine:

```bash
dbdiff source.sqlite target.sqlite | sqlite3 target.sqlite
```

> [!CAUTION]
> Read the output before you apply it. A statement can delete a table, a column, or a row
> of the target database. dbdiff holds no rollback.

## Flags

| Flag       | Value                   | Purpose                                                                                                                        |
| ---------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `--driver` | `sqlite3` or `postgres` | Select the database engine. The default value comes from the source and the target. See [Driver detection](#driver-detection). |
| `--schema` | A schema name           | Name the schema that the postgres driver reads. The default value is the schema of the search path.                            |
| `--data`   | none                    | Add the comparison of the rows. The default value is off.                                                                      |

## Driver detection

If you give no `--driver` flag, dbdiff reads the engine from the source and the target:

| Argument                                                    | Driver     |
| ----------------------------------------------------------- | ---------- |
| A path with the prefix `sqlite://`                          | `sqlite3`  |
| A URL with the prefix `postgres://` or `postgresql://`      | `postgres` |
| A connection string of the form `host=localhost dbname=app` | `postgres` |
| A `.sql` file or a directory                                | none       |
| Another path                                                | `sqlite3`  |

One argument is sufficient. In this example the target names the engine, and dbdiff applies
`schema.sql` to a temporary PostgreSQL server:

```bash
dbdiff schema.sql postgres://user:password@localhost:5432/production
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
dbdiff sqlite://source.db postgres://user:password@localhost:5432/target
# dbdiff: "sqlite://source.db" names the sqlite3 driver and "postgres://user:password@localhost:5432/target" names the postgres driver. Use the --driver flag
```

</details>

Give the `--driver` flag to correct the two cases. The flag has priority, so dbdiff runs no
detection when you give it.

## SQLite

The driver accepts a file path, or a path with the prefix `sqlite://`:

```bash
dbdiff source.sqlite target.sqlite
dbdiff sqlite://source.sqlite sqlite://target.sqlite
```

SQLite holds no schema. If you give the `--schema` flag with this driver, dbdiff gives an
error.

**Table recreation.** SQLite holds no `ALTER COLUMN` statement. If a column changes, or if
a foreign key changes, the driver recreates the table. The recreation copies the rows into
a new table, drops the old table, and renames the new table. A new column takes its default
value, or `NULL`.

**Rename detection.** The driver detects a renamed column. A source column that the target
does not hold, and that holds the attributes of exactly one free target column, is a
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

**Section order.** The driver prints ten sections in this order:

```
extensions → enum types → domains → composite types → sequences
  → functions → aggregates → operators → tables → views
```

A table can use each of the first five objects. That order gives each statement the objects
that it needs.

**Owned objects.** An object that an extension owns stays out of the output. The
`CREATE EXTENSION` statement builds that object again. A sequence that a `SERIAL` column or
an identity column owns stays out of the output for the same reason.

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

## Data comparison

The `--data` flag adds the comparison of the rows:

```bash
dbdiff --data source.sqlite target.sqlite
```

The data section comes after the schema section, because a new row needs its table and its
column. The output holds three kinds of statement:

| Statement | Case                                            |
| --------- | ----------------------------------------------- |
| `INSERT`  | A key that the source only holds                |
| `UPDATE`  | A key that both sides hold with a different row |
| `DELETE`  | A key that the target only holds                |

The comparison needs the primary key of the table. A table with no primary key gets a
comment line, and no row statement. A table with a different primary key in the target gets
the same treatment.

## Supported objects

| Object          | SQLite                                  | PostgreSQL |
| --------------- | --------------------------------------- | ---------- |
| Tables          | ✅                                      | ✅         |
| Indexes         | ✅                                      | ✅         |
| Constraints     | ✅ (foreign keys, primary keys, unique) | ✅         |
| Triggers        | ✅                                      | ✅         |
| Views           | ✅                                      | ✅         |
| Sequences       | ➖                                      | ✅         |
| Enum types      | ➖                                      | ✅         |
| Domains         | ➖                                      | ✅         |
| Composite types | ➖                                      | ✅         |
| Functions       | ➖                                      | ✅         |
| Aggregates      | ➖                                      | ✅         |
| Operators       | ➖                                      | ✅         |
| Extensions      | ➖                                      | ✅         |
| Data            | ✅                                      | ✅         |

✅ dbdiff compares this object. ➖ the engine holds no such object. A table covers its
columns.

dbdiff does not support MySQL.

The SQLite driver compares a partial index and an index that an expression builds. It
prints a primary key of one column and a `UNIQUE` constraint of one column in the
definition of that column. It prints a primary key of two or more columns and a `UNIQUE`
constraint of two or more columns as a table constraint.

## Limits

- The data comparison covers a table that the source and the target both hold. A table
  that the source only holds stays empty. The schema section creates that table.
- The PostgreSQL driver compares one schema for each run. To compare two schemas, run
  dbdiff two times. The driver prints no `CREATE SCHEMA` statement, and it detects no
  object that moved from one schema to another schema.
- A SQL source of the postgres driver needs a download on the first run. Read
  [Limits of a SQL file](#limits-of-a-sql-file) for the other limits.

## Development

```bash
docker compose up -d                      # Start PostgreSQL on port 5432
go build -o ./bin/dbdiff ./cmd/dbdiff     # Build the binary
go test ./...                             # Run the tests
```

The PostgreSQL tests need the database at `postgres://user:password@localhost:5432/dbdiff`.
The command `docker compose up -d` starts that database. The SQLite tests need no service,
because each test writes into a temporary directory.

## License

dbdiff uses the MIT license. Read the [LICENSE](LICENSE) file.
