# @notation/durable/sqlite

`SqliteRuntime` keeps every record in one SQLite file. Several processes can open the same file. See [Running a server](../manual/running-a-server.md) for how to run it.

The runtime has three entry points:

| Entry point | Exports |
| --- | --- |
| `@notation/durable/sqlite/bun` | The exports of `/sqlite`, and `createSqliteDb` and `BunSqliteDriver` for `bun:sqlite` |
| `@notation/durable/sqlite/node` | The exports of `/sqlite`, and `createSqliteDb` and `NodeSqliteDriver` for `node:sqlite` |
| `@notation/durable/sqlite` | `SqliteRuntime`, its client classes and the driver types |

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

export const runtime = new SqliteRuntime(createSqliteDb({ path: "./.db/durable.sqlite" }));
```

## `createSqliteDb(options?)`

Opens the file, and returns a `SqliteDriver`. It creates the file and its directory if they do not exist.

| Option | Default | Does |
| --- | --- | --- |
| `path` | `"./.db/durable.sqlite"` | The database file |
| `wal` | `true` | Puts the database in WAL mode |

Each connection waits up to 5 seconds for another process's lock before it fails.

## `SqliteRuntime`

`new SqliteRuntime(db, { clock? })` creates the tables and indexes that do not exist yet, and returns the runtime. `clock` defaults to `Date.now`.

| Client | Class | Tables |
| --- | --- | --- |
| `executions` | `SqliteExecutionClient` | `executions`, `start_requests` |
| `heap` | `SqliteHeapClient` | `step_responses` |
| `stores` | `SqliteStoreClient` | `stores`, `store_waiters`, `store_applied_steps` |
| `effects` | `SqliteEffectClient` | `effects`, `effect_conflicts`, `effect_waiters` |
| `joins` | `SqliteJoinClient` | `join_waiters` |

**Every write is one `IMMEDIATE` transaction**, so processes that share the file take turns. Writes that must happen together, such as a store update and the wakes of its waiters, are in the same transaction. See [Writes that go together](../runtime-authors/runtimes.md#writes-that-go-together).

Create the client classes through `SqliteRuntime`, so that they share one database and one clock.

## Listing executions

`list` returns executions newest first, by the `rowid` of the `executions` table. SQLite assigns the `rowid` under the write lock, so a later admission always has a higher one, whatever the clocks of the processes.

The indexes on `workflow_id` and `status` serve a `list` filtered by either. No index serves `status: "running"`, because an execution is running only while its lease has not expired. That `list` reads the table from the newest row until it fills the page.

**`VACUUM` can renumber the `rowid`s.** A cursor from before a `VACUUM` can then skip or repeat executions. Start again from the first page after a `VACUUM`.

## Upgrades

`SqliteRuntime` adds missing columns when it opens a file that an earlier version created. It adds them under the write lock, so processes that open the file at once add each column once.

**Every process that shares a file must run a version with the feature before any workflow uses it:**

| Feature | A process on an earlier version |
| --- | --- |
| `step.join` and `step.call` | Ends executions without waking the executions that wait for them |
| `cancel` | Runs deliveries without the cancellation request, and starts children without passing the request on |

## Other drivers

`SqliteRuntime` takes any object that implements `SqliteDriver`:

```ts
interface SqliteDriver {
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  close(): void;
}

interface SqliteStatement<Row> {
  get(parameters?: SqliteParameters): Row | undefined;
  all(parameters?: SqliteParameters): Row[];
  run(parameters?: SqliteParameters): { changes: number | bigint };
}
```

`SqliteParameters` maps names with a `$` prefix, such as `$executionId`, to values. The runtime passes exactly the names that the SQL uses.

**The driver must be synchronous.** `SqliteRuntime` runs each transaction as synchronous calls, so that no other code runs inside it. `createSqliteDb` returns a `BunSqliteDriver` from `/sqlite/bun`, and a `NodeSqliteDriver` from `/sqlite/node`.
