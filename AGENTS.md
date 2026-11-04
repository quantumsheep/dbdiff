# dbdiff - Agent Context

dbdiff is a command line tool in Go. It reads the schema of two databases and prints the SQL statements that make the source schema equal to the target schema. dbdiff supports SQLite, PostgreSQL, MySQL, and MariaDB. Each side is a database, a `.sql` file, or a directory of `.sql` files. The CLI and the public packages call the two sides current and desired. The engine code still calls them source and target.

This file gives the principles. The code holds the details. Before you change a part of the code, read the files near it and copy their patterns. The code comments hold the engine rules and the order constraints.

## Architecture

- `cmd/dbdiff/` holds the CLI, with one package per top-level command. The subcommands of a command live in the package of that command, with one file for each subcommand.
- `internal/drivers/shared/` holds the engine-free core. `internal/drivers/<engine>/` holds one engine.
- `internal/migrations/` holds the migration runner. It knows no engine.
- `drivers/` and `migrations/` are the public Go packages. Keep their types stable.

Keep these rules:

- One engine package holds one file per schema object, `<engine>_<object>.go`. Do not add a package for a schema object.
- An engine package never imports `internal/drivers` or another engine package.
- A package takes the name of its path with no separator, for example `driverssqlite`. Every import uses that name.

## How a diff works

1. Read the objects of both sides.
2. Walk the target objects. Create each absent object. Alter or recreate each changed object.
3. Walk the source objects. Drop each object that the target does not hold.

If a key match is sufficient, use `DiffByKey` for these steps. Print the additions and the modifications before the removals.

- Build the output as a list of instruction values. Never format SQL at the call site.
- An instruction type renders one SQL statement and quotes its own identifiers. In the driver packages, only an instruction type has a `String()` method.
- The output must be the same on each run. Give every catalog query a stable `ORDER BY`. Keep every stable sort.

## Tests

```bash
docker compose up -d    # PostgreSQL, MySQL, and MariaDB
go test ./...
```

`DBDIFF_TEST_SKIP_POSTGRES=1` and `DBDIFF_TEST_SKIP_MYSQL=1` skip the server tests. Do not set them on Linux, because a silent skip hides a server failure.

- Put the test beside the code. Use `testify/require`.
- Give each new behavior one `t.Run` subtest in the main test function of its package.
- In a driver test, compare the whole instruction list. Then apply the diff to the source. This proves that the SQL runs.
- If the change recreates a table or moves data, insert rows before the diff. Then compare the rows after the diff.
- Compare the whole error message. Never use `require.Contains` on a message.

## Go conventions

- After every change, run `gofmt -w .` and `golangci-lint run ./...`. Correct every finding. Never add a `nolint` comment.
- Name variables with full words: `sourceDatabaseConnection`, not `srcConn`.
- Return errors. Never call `os.Exit` or `panic` outside `main`.
- Close every `*sql.Rows`. After the loop, return the error of `rows.Err()`.
- Pass the context to every query.
- Never initialize a variable in the header of an `if`, `for`, `switch`, or `select`.
- Put a blank line before and after a block. Do not put this line at the start or the end of a body. Do not put it between a block and the assignment that its condition checks.
- In a struct literal, write each field on its own line.
- Before you use a library, read its version in `go.mod`. Then read the documentation of that version with `go doc`.

## Comments

Write no comment by default. Keep a comment only if, without it, a reader can break the code. This applies to an order that a change can break, an engine rule that the code cannot show, and a step that looks unnecessary.

## Writing style

Write comments, commit messages, documentation, CLI help text, and error messages in ASD-STE100 Simplified Technical English.

- Write short sentences in the active voice, with the condition before the command.
- Use only the modal verbs can, will, and must.
- Use one name for one item.
- Do not use a semicolon, a contraction, an em dash, "e.g.", "i.e.", or "etc.".

## Git

- If the user does not ask for a commit, do not run `git commit`.
- Use Conventional Commits with a scope, for example `feat(sqlite): add the view diff`. Write the subject only, in lowercase, with no body and no trailer.
- Update the support table of `README.md` in the same commit as the feature.

## Delegation

**If you are a subagent, do the work yourself and never spawn another subagent.**

- Give each subagent its own file list. Do not let two agents edit one file at the same time.
- Do not let a subagent run a git command that changes the checkout or the index.
- Before a command that changes shared state, for example `gofmt -w .`, make sure that no subagent still runs.
