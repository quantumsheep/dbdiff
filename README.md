# dbdiff

dbdiff reads the schema of two databases. Then it prints the SQL statements that make the
second schema equal to the first one.

dbdiff supports SQLite and PostgreSQL. It compares the schema by default. The `--data`
flag adds the comparison of the rows.

## Installation

### From a release

Download a binary from the [releases](https://github.com/quantumsheep/dbdiff/releases)
page. Each release holds a binary for Linux, for Windows, and for macOS, on amd64 and on
arm64.

### With Go

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

The SQLite driver is a C binding. If the build fails with an undefined symbol, set
`CGO_ENABLED=1` before the build.

## Usage

dbdiff takes two arguments:

```bash
dbdiff [flags] <source> <target>
```

The first argument is the source. It holds the wanted schema. The second argument is the
target. The output changes the target.

This example compares two SQLite files:

```bash
dbdiff source.sqlite target.sqlite
```

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

CAUTION: Read the output before you apply it. A statement can delete a table, a column, or
a row of the target database. dbdiff holds no rollback.

### Flags

| Flag       | Value                    | Purpose                                                                                          |
| ---------- | ------------------------ | ------------------------------------------------------------------------------------------------ |
| `--driver` | `sqlite3` or `postgres`  | Select the database engine. The default value is `sqlite3`.                                       |
| `--schema` | A schema name            | Name the schema that the postgres driver reads. The default value is the schema of the search path. |
| `--data`   | none                     | Add the comparison of the rows. The default value is off.                                         |

## SQLite

The driver accepts a file path, or a path with the prefix `sqlite://`:

```bash
dbdiff source.sqlite target.sqlite
dbdiff sqlite://source.sqlite sqlite://target.sqlite
```

SQLite holds no schema. If you give the `--schema` flag with this driver, dbdiff gives an
error.

SQLite holds no `ALTER COLUMN` statement. If a column changes, or if a foreign key
changes, the driver recreates the table. The recreation copies the rows into a new table,
drops the old table, and renames the new table. A new column takes its default value, or
`NULL`.

The driver detects a renamed column. A source column that the target does not hold, and
that holds the attributes of exactly one free target column, is a rename. Two candidates
make the guess unsafe. In that case the column becomes an addition, and the old column
becomes a removal.

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

The driver prints ten sections in this order: extensions, enum types, domains, composite
types, sequences, functions, aggregates, operators, tables, and views. A table can use each
of the first five objects. That order gives each statement the objects that it needs.

An object that an extension owns stays out of the output. The `CREATE EXTENSION` statement
builds that object again. A sequence that a `SERIAL` column or an identity column owns
stays out of the output for the same reason.

## Data comparison

The `--data` flag adds the comparison of the rows:

```bash
dbdiff --data source.sqlite target.sqlite
```

The data section comes after the schema section, because a new row needs its table and its
column. The output holds three kinds of statement:

- An `INSERT` statement for a key that the source only holds.
- An `UPDATE` statement for a key that both sides hold with a different row.
- A `DELETE` statement for a key that the target only holds.

The comparison needs the primary key of the table. A table with no primary key gets a
comment line, and no row statement. A table with a different primary key in the target gets
the same treatment.

## Supported objects

| Object          | SQLite                                  | PostgreSQL |
| --------------- | --------------------------------------- | ---------- |
| Tables          | ✅                                       | ✅          |
| Indexes         | ✅                                       | ✅          |
| Constraints     | ✅ (foreign keys, primary keys, unique)  | ✅          |
| Triggers        | ✅                                       | ✅          |
| Views           | ✅                                       | ✅          |
| Sequences       | ➖                                       | ✅          |
| Enum types      | ➖                                       | ✅          |
| Domains         | ➖                                       | ✅          |
| Composite types | ➖                                       | ✅          |
| Functions       | ➖                                       | ✅          |
| Aggregates      | ➖                                       | ✅          |
| Operators       | ➖                                       | ✅          |
| Extensions      | ➖                                       | ✅          |
| Data            | ✅                                       | ✅          |

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

## Development

```bash
docker compose up -d
go build -o ./bin/dbdiff ./cmd/dbdiff
go test ./...
```

The PostgreSQL tests need the database at `postgres://user:password@localhost:5432/dbdiff`.
The command `docker compose up -d` starts that database. The SQLite tests need no service,
because each test writes into a temporary directory.

## License

dbdiff uses the MIT license. Read the [LICENSE](LICENSE) file.
