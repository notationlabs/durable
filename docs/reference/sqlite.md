# @notation/durable/sqlite

`@notation/durable/sqlite` stores execution records, step records, stores, effects, and waiters in a single SQLite database file so multi-record guarantees commit atomically in synchronous `IMMEDIATE` transactions.

| Entry point | Exports |
| :--- | :--- |
| `@notation/durable/sqlite/bun` | All `/sqlite` exports plus `createSqliteDb` and `BunSqliteDriver` |
| `@notation/durable/sqlite/node` | All `/sqlite` exports plus `createSqliteDb` and `NodeSqliteDriver` |
| `@notation/durable/sqlite` | `SqliteRuntime`, SQLite client classes, and driver interfaces |

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

export const runtime = new SqliteRuntime(
  createSqliteDb({ path: "./.db/durable.sqlite" })
);
```

## createSqliteDb

`createSqliteDb` is exported by `@notation/durable/sqlite/bun` and `@notation/durable/sqlite/node`. It creates any missing parent directories, opens the SQLite file, sets `PRAGMA busy_timeout = 5000`, and enables WAL mode when configured:

```ts [@notation/durable/sqlite/bun]
export type SqliteDbOpts = {
  path?: string;
  wal?: boolean;
};

export function createSqliteDb(opts?: SqliteDbOpts): BunSqliteDriver;
```

```ts [@notation/durable/sqlite/node]
export function createSqliteDb(opts?: SqliteDbOpts): NodeSqliteDriver;
```

| Option | Description |
| :--- | :--- |
| `path` | Path to the SQLite database file |
| `wal` | Enables Write-Ahead Logging mode when true |

`path` defaults to `./.db/durable.sqlite` and `wal` defaults to true. When concurrent processes enable WAL mode on a new database simultaneously, `createSqliteDb` retries busy errors for up to 5 seconds.

Keep WAL databases on a local filesystem rather than network-attached storage, and include `-wal` and `-shm` companion files in consistent backups as described in [Running a Server](../core/running-a-server.md). For table schemas and index layouts, see [SQLite Storage](../internals/sqlite-storage.md).

## SqliteRuntime

`SqliteRuntime` extends [`Runtime`](./runtime.md#runtime) and initializes all required tables and indexes when constructed:

```ts [@notation/durable/sqlite]
export type SqliteRuntimeOptions = RuntimeOptions;

export class SqliteRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: SqliteExecutionClient;
  readonly effects: SqliteEffectClient;
  readonly heap: SqliteHeapClient;
  readonly joins: SqliteJoinClient;
  readonly stores: SqliteStoreClient;

  constructor(db: SqliteDriver, options?: SqliteRuntimeOptions);
}
```

| Property | Client class |
| :--- | :--- |
| `clock` | Clock function used by every client |
| `executions` | `SqliteExecutionClient` |
| `effects` | `SqliteEffectClient` |
| `heap` | `SqliteHeapClient` |
| `joins` | `SqliteJoinClient` |
| `stores` | `SqliteStoreClient` |

`options.clock` defaults to `() => Date.now()`, evaluated lazily on each call so fake timers installed after construction still apply. Always construct clients through `SqliteRuntime` rather than instantiating them separately so all clients share one `SqliteDriver`, one clock, and one `SqliteExecutionClient`.

## Client classes

`@notation/durable/sqlite` exports each SQLite client class and its option types:

```ts [@notation/durable/sqlite]
export type SqliteExecutionClientOptions = {
  clock?: () => number;
};

export class SqliteExecutionClient implements ExecutionClient {
  constructor(db: SqliteDriver, options?: SqliteExecutionClientOptions);
  // The earliest time claimDue could return an execution, or null.
  nextClaimAt(): number | null;
}

export class SqliteEffectClient implements EffectClient {
  constructor(params: {
    db: SqliteDriver;
    executions?: SqliteExecutionClient;
    clock?: () => number;
  });
}

export class SqliteHeapClient implements HeapClient {
  constructor(db: SqliteDriver, options?: { clock?: () => number });
}

export class SqliteJoinClient implements JoinClient {
  constructor(db: SqliteDriver);
}

export class SqliteStoreClient extends CasStoreClient {
  constructor(params: { db: SqliteDriver; executions?: SqliteExecutionClient });
}
```

When `executions` is omitted on effect or store clients, the constructor creates an execution client automatically on the same driver.

## Listing and upgrades

- **Admission order and cursors** – `executions.list` orders executions newest first by SQLite `rowid`, assigned under the write lock regardless of process clock skew. Running `VACUUM` can renumber `rowid` values and invalidate active pagination cursors; restart pagination from the first page after a `VACUUM`.
- **Filtered queries** – Filtering `executions.list` by workflow ID or non-running status uses indexes, whereas filtering by running status scans newest first because running state derives from lease expiration.
- **Automatic schema migrations** – Opening an existing database adds any missing columns under the write lock, so concurrent processes apply each `ALTER TABLE` once.
- **Cross-process rollouts** – Upgrade every process sharing a SQLite file to a release supporting `step.join`, `step.call`, or `cancel()` before any workflow or caller uses those features; see [Running a Server](../core/running-a-server.md).

## Custom drivers

`SqliteRuntime` accepts any synchronous implementation of `SqliteDriver`:

```ts [@notation/durable/sqlite]
export type SqliteParameterValue =
  | string
  | number
  | bigint
  | boolean
  | Uint8Array
  | null;

export type SqliteParameters = Record<`$${string}`, SqliteParameterValue>;

export type SqliteRunResult = {
  changes: number | bigint;
};

export interface SqliteStatement<Row> {
  get(parameters?: SqliteParameters): Row | undefined;
  all(parameters?: SqliteParameters): Row[];
  run(parameters?: SqliteParameters): SqliteRunResult;
}

export interface SqliteDriver {
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  transaction?<R>(fn: () => R): R;
  close(): void;
}
```

`transaction` is optional. A driver without it gets `BEGIN IMMEDIATE`, `COMMIT` and `ROLLBACK` through `run`. Some databases reject these statements, so their drivers must implement `transaction`. It commits when `fn` returns and rolls back when `fn` throws. The [Durable Object driver](./cloudflare.md) is one such driver.

`@notation/durable/sqlite/bun` and `@notation/durable/sqlite/node` also export their concrete driver classes:

```ts [@notation/durable/sqlite/bun]
export class BunSqliteDriver implements SqliteDriver {
  constructor(database: Database);
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  close(): void;
}
```

```ts [@notation/durable/sqlite/node]
export class NodeSqliteDriver implements SqliteDriver {
  constructor(database: DatabaseSync);
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  close(): void;
}
```

Custom drivers must satisfy three requirements:

- **Synchronous execution** – All methods on `SqliteDriver` and `SqliteStatement` must execute synchronously so nothing awaits between `BEGIN IMMEDIATE` and `COMMIT`, or inside `transaction`.
- **Exact named parameters** – Queries bind `$`-prefixed named keys matching the SQL statement exactly, because native drivers vary on whether unused keys are ignored or throw.
- **Undefined for missing rows** – `SqliteStatement.get` must return `undefined` rather than `null` when no row matches.
