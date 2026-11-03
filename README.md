<div align="center">

# dbdiff

**Compare two database schemas and print the SQL that makes one equal to the other.**

![Tests](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml/badge.svg)
![Release](https://img.shields.io/github/v/release/quantumsheep/dbdiff?label=release)
![Go Reference](https://pkg.go.dev/badge/github.com/quantumsheep/dbdiff.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

</div>

```console
$ dbdiff diff current.sqlite final.sqlite
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
DROP TABLE "audit";
CREATE VIEW active_users AS SELECT id, email FROM users;
```

dbdiff supports SQLite, PostgreSQL, MySQL, and MariaDB. Each side is a database, a `.sql` file, or a directory of `.sql` files.

## Installation

Download a binary from the [releases](https://github.com/quantumsheep/dbdiff/releases) page, or install with Go:

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

The SQLite driver needs `CGO_ENABLED=1`.

## Usage

| Command                          | Result                                                            |
| -------------------------------- | ----------------------------------------------------------------- |
| `dbdiff diff <source> <target>`  | Print the statements that make the source equal to the target     |
| `dbdiff migrate generate <name>` | Write the next migration file                                     |
| `dbdiff migrate status`          | List each migration and its state                                 |
| `dbdiff migrate preview`         | Print the statements that `up` will run                           |
| `dbdiff migrate up`              | Apply every pending migration                                     |
| `dbdiff migrate step`            | Apply every pending migration, and ask before each file           |
| `dbdiff migrate verify`          | Compare the database against the replay of the applied migrations |
| `dbdiff migrate repair`          | Make the history table agree with the migration files             |
| `dbdiff migrate baseline`        | Record every migration file as applied, and run no file           |

`dbdiff diff` changes no database. To apply its output, send it to the client of the engine:

```bash
dbdiff diff current.sqlite schema.sql | sqlite3 current.sqlite
dbdiff diff postgres://localhost:5432/source postgres://localhost:5432/target
```

> [!CAUTION]
> Read the output before you apply it. It can drop tables, columns, and rows.

### Flags of `diff`

| Flag             | Value                             | Purpose                                                             |
| ---------------- | --------------------------------- | ------------------------------------------------------------------- |
| `--driver`       | `sqlite3`, `postgres`, or `mysql` | Select the engine. dbdiff detects it from the arguments by default. |
| `--schema`       | A schema name                     | Select the PostgreSQL schema. The default is the search path.       |
| `--data`         | none                              | Compare the rows too.                                               |
| `--comments`     | none                              | Print a comment before each change.                                 |
| `--privileges`   | none                              | Compare the owners and the privileges (PostgreSQL and MySQL).       |
| `--exit-code`    | none                              | If the schemas differ, exit with code 1.                            |
| `--ignore-table` | A table name                      | Ignore this table. Repeat the flag for more tables.                 |

### Flags of `migrate`

| Flag             | Value                             | Purpose                                                                                               |
| ---------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `--config`       | A file path                       | Read this configuration file. The default is `dbdiff.yaml` in the working directory.                  |
| `--source`       | A directory path                  | Read the migration files from this directory.                                                         |
| `--target`       | A connection URL                  | Change this database. The default is `DBDIFF_TARGET`. For `generate`, the value is the wanted schema. |
| `--to`           | A migration version               | For `up` and `baseline`, stop after this version. The default is the last migration.                  |
| `--run`          | none                              | For `preview`, apply the pending files in one transaction. Then roll back the transaction.            |
| `--driver`       | `sqlite3`, `postgres`, or `mysql` | Select the engine. dbdiff detects it from the source and the target by default.                       |
| `--schema`       | A schema name                     | Select the PostgreSQL schema. The default is the search path.                                         |
| `--ignore-table` | A table name                      | Ignore this table. Repeat the flag for more tables.                                                   |

## SQL files

dbdiff runs a `.sql` file, or the `.sql` files of a directory in name order, in a temporary database. For PostgreSQL and MySQL, it downloads a temporary server on the first run, with the version of the other side. The `version` key of `dbdiff.yaml` sets another version.

Each file runs in one transaction. If a file holds a statement that cannot run in a transaction, for example `CREATE INDEX CONCURRENTLY`, add the line `-- dbdiff:no-transaction` to the file.

### Server requirements

| System                      | Run before the first use                                     |
| --------------------------- | ------------------------------------------------------------ |
| macOS, with MariaDB         | `brew install mariadb`                                       |
| Debian, Ubuntu before 24.04 | `apt-get install libaio1`                                    |
| Ubuntu 24.04 and later      | `apt-get install libaio1t64`, then the `ln -s` command below |

```bash
ln -s /usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH)/libaio.so.1t64 \
      /usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH)/libaio.so.1
```

dbdiff does not support MariaDB on Linux with arm64.

## Data comparison

`--data` compares the rows by primary key and prints `INSERT`, `UPDATE`, and `DELETE` statements. dbdiff skips a table with no primary key.

## Migrations

`dbdiff migrate` reads `dbdiff.yaml` from the working directory. A flag overrides its key.

```yaml
driver: postgres
source: ./migrations # migration files
target: ./schema.sql # wanted schema, for `migrate generate`
schema: app
version: 17.11.0     # temporary server version
ignore:
  tables:
    - example
```

`migrate generate` writes the difference between the target and the migration files as a new file:

```console
$ dbdiff migrate generate add_created_at
migrations/20260822143000_add_created_at.sql
```

The other commands act on the database that `--target` or `DBDIFF_TARGET` names:

```bash
export DBDIFF_TARGET=postgres://user:password@localhost:5432/production
dbdiff migrate up
```

- `migrate up` applies each file in its own transaction and records it in `dbdiff_migrations`. `--to <version>` stops after that version.
- On MySQL, and in a file with `-- dbdiff:no-transaction`, a file that stops before its end can leave part of its changes. If this occurs, repair the database. Then run `migrate repair`.
- `migrate baseline` records the files as applied, and runs no file. If the database already has the schema, use this command.

### Run dbdiff from Go

Run a diff:

```go
import "github.com/quantumsheep/dbdiff/drivers"

driver, err := drivers.NewDriver(drivers.SQLiteDriverName)

statements, err := driver.Diff(ctx,
	drivers.NewConnectionStringDataSource("app.sqlite"),
	drivers.NewFileDataSource("schema.sql"),
)

fmt.Println(statements.String())
```

Apply the migrations, for example at server start:

```go
import (
	"github.com/quantumsheep/dbdiff/drivers"
	"github.com/quantumsheep/dbdiff/migrations"
)

driver, err := drivers.NewDriver(drivers.PostgresDriverName)

migrator := driver.Migrator(
	migrations.WithTargetDataSource(
		drivers.NewConnectionStringDataSource(os.Getenv("DATABASE_URL")),
	),
	migrations.WithMigrationDirectory("migrations"),
)

err = migrator.Up(ctx)
```

## Supported objects

| Object                        | SQLite                                          | PostgreSQL              | MySQL / MariaDB                                 |
| ----------------------------- | ----------------------------------------------- | ----------------------- | ----------------------------------------------- |
| Tables                        | ✅                                              | ✅                      | ✅                                              |
| Identity columns              | ➖                                              | ✅                      | ➖                                              |
| Table options                 | ✅ (WITHOUT ROWID, STRICT)                      | ✅ (storage parameters) | ✅ (ENGINE, collation)                          |
| Virtual tables                | ✅                                              | ➖                      | ➖                                              |
| Generated columns             | ✅                                              | ✅                      | ✅                                              |
| Column storage and statistics | ➖                                              | ✅                      | ➖                                              |
| Indexes                       | ✅                                              | ✅                      | ✅                                              |
| Constraints                   | ✅ (foreign keys, primary keys, unique, checks) | ✅                      | ✅ (foreign keys, primary keys, unique, checks) |
| Triggers                      | ✅ (of a table and of a view)                   | ✅ (with the mode)      | ✅                                              |
| Views                         | ✅                                              | ✅                      | ✅                                              |
| Materialized views            | ➖                                              | ✅                      | ➖                                              |
| Rules                         | ➖                                              | ✅                      | ➖                                              |
| Extended statistics           | ➖                                              | ✅                      | ➖                                              |
| Partitioned tables            | ➖                                              | ✅                      | ✅                                              |
| Replica identity              | ➖                                              | ✅                      | ➖                                              |
| Sequences                     | ➖                                              | ✅                      | ✅ (MariaDB)                                    |
| Enum types                    | ➖                                              | ✅                      | ➖ (the column type covers it)                  |
| Domains                       | ➖                                              | ✅                      | ➖                                              |
| Composite types               | ➖                                              | ✅                      | ➖                                              |
| Functions                     | ➖                                              | ✅                      | ✅                                              |
| Procedures                    | ➖                                              | ✅                      | ✅                                              |
| Events                        | ➖                                              | ➖                      | ✅                                              |
| Aggregates                    | ➖                                              | ✅                      | ➖                                              |
| Operators                     | ➖                                              | ✅                      | ➖                                              |
| Casts                         | ➖                                              | ✅                      | ➖                                              |
| Extensions                    | ➖                                              | ✅                      | ➖                                              |
| Comments                      | ➖                                              | ✅                      | ✅ (of a column)                                |
| Row level security            | ➖                                              | ✅                      | ➖                                              |
| Privileges                    | ➖                                              | ✅ (`--privileges`)     | ✅ (`--privileges`)                             |
| Data                          | ✅                                              | ✅                      | ✅                                              |

✅: dbdiff supports the object. ➖: the engine has no such object.

## Limits

- PostgreSQL: dbdiff compares one schema per run. It does not print `CREATE SCHEMA`.
- MySQL: dbdiff compares one database per run. `--privileges` compares only the grants of the database and of the tables.
- The `mysql` client splits the bodies of triggers and routines at each semicolon. To apply such a diff, use `dbdiff migrate up`.
- dbdiff does not write rollback migrations.

## Development

```bash
docker compose up -d     # Start PostgreSQL, MySQL, and MariaDB
go test ./...

# Without the containers
DBDIFF_TEST_SKIP_POSTGRES=1 DBDIFF_TEST_SKIP_MYSQL=1 go test -short ./...
```

## License

[MIT](LICENSE)
