<div align="center">

# dbdiff

**Compare two database schemas and print the SQL that makes one equal to the other.**

![Tests](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml/badge.svg)
![Release](https://img.shields.io/github/v/release/quantumsheep/dbdiff?label=release)
![Go Reference](https://pkg.go.dev/badge/github.com/quantumsheep/dbdiff.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

</div>

```console
$ dbdiff diff current.sqlite desired.sqlite
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

dbdiff needs no C compiler. The SQLite driver supports the virtual table modules `fts5`, `rtree`, and `geopoly`. It does not support `fts3` and `fts4`.

## Usage

| Command                           | Result                                                                        |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `dbdiff diff <current> <desired>` | Print the statements that make the current schema equal to the desired schema |
| `dbdiff migrate generate <name>`  | Write the next migration file                                                 |
| `dbdiff migrate status`           | List each migration and its state                                             |
| `dbdiff migrate preview`          | Print the statements that `up` will run                                       |
| `dbdiff migrate up`               | Apply every pending migration                                                 |
| `dbdiff migrate step`             | Apply every pending migration, and ask before each file                       |
| `dbdiff migrate verify`           | Compare the database against the replay of the applied migrations             |
| `dbdiff migrate repair`           | Make the history table agree with the migration files                         |
| `dbdiff migrate baseline`         | Record every migration file as applied, and run no file                       |

`dbdiff diff` changes no database. To apply its output, send it to the client of the engine:

```bash
dbdiff diff current.sqlite schema.sql | sqlite3 -bail current.sqlite
dbdiff diff postgres://localhost:5432/current postgres://localhost:5432/desired
```

For SQLite, use `-bail`. If a statement of a table recreation fails, `-bail` stops the script, and SQLite rolls back the recreation. Without `-bail`, the script continues after the error.

> [!CAUTION]
> Read the output before you apply it. It can drop tables, columns, and rows.

### Shell completion

`dbdiff completion <shell>` prints the completion script for `bash`, `zsh`, `fish`, or `pwsh`.

```bash
source <(dbdiff completion bash)                         # bash
source <(dbdiff completion zsh)                          # zsh
dbdiff completion fish > ~/.config/fish/completions/dbdiff.fish   # fish
```

### Flags of `diff`

| Flag               | Value                             | Purpose                                                                                                     |
| ------------------ | --------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `--driver`         | `sqlite3`, `postgres`, or `mysql` | Select the engine. dbdiff detects it from the arguments by default.                                         |
| `--schema`         | A schema name                     | Select the PostgreSQL schema. The default is the search path. Only the postgres driver accepts this flag.   |
| `--data`           | none                              | Compare the rows too.                                                                                       |
| `--comments`       | none                              | Print a comment before each change.                                                                         |
| `--privileges`     | none                              | Compare the owners and the privileges (PostgreSQL and MySQL).                                               |
| `--guess-renames`  | none                              | Rename a column if exactly one removed column has the same attributes (SQLite and MySQL).                    |
| `--exit-code`      | none                              | If the schemas differ, exit with code 1.                                                                    |
| `--config`         | A file path                       | Read this configuration file. The default is `dbdiff.yaml` in the working directory.                        |
| `--ignore-table`   | A table name                      | Ignore this table. Repeat the flag for more tables.                                                         |
| `--server-version` | A server version                  | Select the version of the temporary server. Only the postgres driver and the mysql driver accept this flag. |
| `--scratch-database` | A connection URL                | Run the SQL files on this server, not on a temporary server. Only the postgres driver and the mysql driver accept this flag. |

### Flags of `migrate`

| Flag               | Value                                           | Purpose                                                                                                     |
| ------------------ | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `--config`         | A file path                                     | Read this configuration file. The default is `dbdiff.yaml` in the working directory.                        |
| `--migrations`     | A directory path                                | Read the migration files from this directory.                                                               |
| `--desired`        | A `.sql` file, a directory, or a connection URL | Compare the migration files with this schema. Only `generate` accepts this flag.                            |
| `--database`       | A connection URL                                | Read or change this database. Each command except `generate` accepts this flag.                             |
| `--to`             | A migration version                             | For `up` and `baseline`, stop after this version. The default is the last migration.                        |
| `--run`            | none                                            | For `preview`, apply the pending files in one transaction. Then roll back the transaction.                  |
| `--empty`          | none                                            | For `generate`, write a file with no statement. The command does not read the desired schema.               |
| `--guess-renames`  | none                                            | For `generate`, rename a column if exactly one removed column has the same attributes (SQLite and MySQL).   |
| `--driver`         | `sqlite3`, `postgres`, or `mysql`               | Select the engine. dbdiff detects it from the arguments by default.                                         |
| `--schema`         | A schema name                                   | Select the PostgreSQL schema. The default is the search path. Only the postgres driver accepts this flag.   |
| `--server-version` | A server version                                | Select the version of the temporary server. Only the postgres driver and the mysql driver accept this flag. |
| `--scratch-database` | A connection URL                              | Run the SQL files on this server, not on a temporary server. Only the postgres driver and the mysql driver accept this flag. |
| `--ignore-table`   | A table name                                    | Ignore this table. Repeat the flag for more tables.                                                         |

## SQL files

dbdiff runs a `.sql` file, or the `.sql` files of a directory in name order, in a temporary database. For PostgreSQL and MySQL, it downloads a temporary server on the first run, with the version of the other side. The `--server-version` flag sets another version.

Each file runs in one transaction. If a file holds a statement that cannot run in a transaction, for example `CREATE INDEX CONCURRENTLY`, add the line `-- dbdiff:no-transaction` to the file.

### Scratch database

To use an existing server instead of a temporary server, give its connection URL with `--scratch-database`, or with the key `scratch-database`. dbdiff then downloads nothing and starts no server. For each SQL side, dbdiff creates a database with a name that starts with `dbdiff_`. At the end, dbdiff drops the database, also when the diff fails. The user of the URL must have the privilege to create and drop a database. If both sides are SQL files and `--driver` is not set, the scheme of the URL selects the driver. Do not use `--scratch-database` together with `--server-version`.

```bash
dbdiff diff --scratch-database postgres://user:password@localhost:5432/postgres current.sql desired.sql
```

A scratch database needs none of the server requirements below. It also works for MariaDB on Linux with arm64.

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

dbdiff reads every row of the two sides into memory. Use `--data` for small tables, for example reference data and seed data.

## Migrations

`dbdiff migrate` reads `dbdiff.yaml` from the working directory. Each key has the name of its long flag. A flag overrides its key. The keys are `driver`, `migrations`, `desired`, `database`, `schema`, `server-version`, `scratch-database`, and `ignore-table`. `--config`, `--to`, and `--run` have no key.

`dbdiff diff` also reads `dbdiff.yaml`. It uses only the keys `driver`, `schema`, `server-version`, `scratch-database`, and `ignore-table`, and it ignores the other keys. Give the two schemas of `diff` as arguments.

```yaml
driver: postgres
migrations: ./migrations      # migration files
desired: ./schema.sql         # desired schema, for `migrate generate`
database: ${DATABASE_URL}     # database, for the other commands
schema: app
server-version: 17.11.0       # temporary server version
ignore-table:
  - example
```

dbdiff replaces each `${NAME}` in a value of the file with the environment variable `NAME`. If the variable is not set, dbdiff stops with an error. dbdiff replaces only the form `${NAME}`. It does not replace `$NAME`, and it does not replace a value of a flag. A value of the file cannot hold the literal text `${NAME}`.

`migrate generate` writes the difference between the migration files and the desired schema as a new file:

```console
$ dbdiff migrate generate add_created_at
migrations/20260822143000_add_created_at.sql
```

For a data change, add the `--empty` flag. `generate` then writes a file with no statement, and it does not read the desired schema. Write the statements in the file.

```console
$ dbdiff migrate generate --empty backfill_users
migrations/20260822143100_backfill_users.sql
```

The other commands act on the database that `--database` or the `database` key names:

```bash
export DATABASE_URL=postgres://user:password@localhost:5432/production
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
	migrations.WithDatabase(
		drivers.NewConnectionStringDataSource(os.Getenv("DATABASE_URL")),
	),
	migrations.WithMigrations("migrations"),
)

err = migrator.Up(ctx)
```

## Supported objects

| Object                        | SQLite                                          | PostgreSQL              | MySQL / MariaDB                                 |
| ----------------------------- | ----------------------------------------------- | ----------------------- | ----------------------------------------------- |
| Tables                        | ✅                                              | ✅                      | ✅ (with the column order)                      |
| Identity columns              | ➖                                              | ✅                      | ➖                                              |
| Table options                 | ✅ (WITHOUT ROWID, STRICT)                      | ✅ (storage parameters) | ✅ (ENGINE, collation)                          |
| Virtual tables                | ✅                                              | ➖                      | ➖                                              |
| Generated columns             | ✅                                              | ✅                      | ✅                                              |
| Column storage and statistics | ➖                                              | ✅                      | ➖                                              |
| Indexes                       | ✅                                              | ✅                      | ✅                                              |
| Constraints                   | ✅ (foreign keys, primary keys, unique, checks) | ✅                      | ✅ (foreign keys, primary keys, unique, checks) |
| Triggers                      | ✅ (of a table and of a view)                   | ✅ (with the mode)      | ✅                                              |
| Views                         | ✅                                              | ✅                      | ✅ (with the check option and the SQL security) |
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
- SQLite: SQLite cannot change a virtual table. If the module or its arguments change, dbdiff drops the virtual table and creates it again, and the new table holds no row.
- MySQL: dbdiff compares one database per run. `--privileges` compares only the grants of the database and of the tables.
- The `mysql` client splits the bodies of triggers and routines at each semicolon. To apply such a diff, use `dbdiff migrate up`.
- dbdiff has no down migrations. This is a design choice. A rollback is a new up migration that reverses the change. To roll back, change the desired schema back, run `dbdiff migrate generate`, then run `dbdiff migrate up`.

## Development

```bash
docker compose up -d     # Start PostgreSQL, MySQL, and MariaDB
go test ./...

# Without the containers
DBDIFF_TEST_SKIP_POSTGRES=1 DBDIFF_TEST_SKIP_MYSQL=1 go test -short ./...
```

## License

[MIT](LICENSE)
