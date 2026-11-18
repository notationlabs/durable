# SQLite Storage

`SqliteRuntime` stores executions, step records, stores, effects, start requests, and all waiter tables in a single SQLite database file. Keeping every client on one database lets multi-table transitions – such as committing a store mutation and waking matching waiters – complete inside one atomic transaction.

## Connection & WAL setup

`@notation/durable/sqlite/bun` and `@notation/durable/sqlite/node` both open SQLite through `createSqliteDriver`. By default it creates missing parent directories for `./.db/durable.sqlite`, sets a 5,000 ms busy timeout, and enables WAL journal mode without overriding synchronous flags.

When two processes open a new database simultaneously, SQLite can return a busy error on WAL activation before the busy handler attaches. Driver startup retries WAL activation every 10 ms until it succeeds or the 5,000 ms timeout expires.

The driver adapters normalize three runtime differences between `bun:sqlite` and `node:sqlite`:

- **Missing rows** – Bun returns null from row lookups while Node returns undefined; the wrapper normalizes both to `undefined`.
- **Boolean bindings** – Node converts JS boolean parameters to integers because `node:sqlite` rejects booleans.
- **Exact named parameters** – Queries use `$`-prefixed bindings and pass only keys referenced in the SQL because native drivers differ on extra keys.

## Transaction discipline

Every write runs inside `runTransaction` in `packages/sqlite-runtime/src/sqlite-transaction.ts`:

```ts [packages/sqlite-runtime/src/sqlite-transaction.ts]
export function runTransaction<R>(db: SqliteDriver, fn: () => R): R {
  db.run("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.run("COMMIT");
    return result;
  } catch (err) {
    db.run("ROLLBACK");
    throw err;
  }
}
```

Because the callback is synchronous and cannot suspend on `await`, queries on a shared connection never interleave between transaction start and commit. Taking the write lock up front with `BEGIN IMMEDIATE` avoids busy lock-upgrade deadlocks and serializes competing processes.

Sharing one database file lets cross-table invariants commit atomically:

- **Step persistence** – `SqliteHeapClient` verifies the execution lease, inserts the step record, and deletes satisfied waiters in one transaction.
- **Store commits** – `SqliteStoreClient` updates the store snapshot, writes the `store_applied_steps` receipt, and wakes matching executions together.
- **Effect & join wakes** – `SqliteEffectClient` and `SqliteExecutionClient` wake waiting executions inside settlement and terminal acknowledgement transactions, while `SqliteJoinClient` reads target state and inserts `join_waiters` atomically.

## Schema overview

Each DAO and `SqliteStoreClient` creates its tables and indexes idempotently at construction time:

| Table | Primary key and role |
| :--- | :--- |
| `executions` | Keyed by `execution_id`; execution state, lease fencing, claim schedule, parent link, and cancellation |
| `step_responses` | Keyed by `execution_id`, `step_key`, `step_attempt`; immutable step attempt JSON and done flag |
| `stores` | Keyed by `instance_id` with unique `store_name`, `store_id`; store JSON state and CAS version |
| `store_applied_steps` | Keyed by `store_name`, `store_id`, `execution_id`, `step_key`; store mutation idempotency receipts |
| `store_waiters` | Keyed by `store_name`, `store_id`, `execution_id`, `step_key`; suspended `store.when` and `store.take` watchers |
| `effects` | Keyed by `effect_id`; external effect operation, canonical input, status, and settled result |
| `effect_conflicts` | Keyed by `effect_id`, `conflicting_result`; duplicate settlement mismatch records |
| `effect_waiters` | Keyed by `effect_id`, `execution_id`, `step_key`; executions waiting on effect settlement |
| `join_waiters` | Keyed by `target_execution_id`, `execution_id`, `step_key`; executions waiting on target completion |
| `start_requests` | Keyed by `caller_execution_id`, `step_key`; idempotent child workflow admission records |

Waiters in `store_waiters` and `effect_waiters` are deleted when their step records a finished result or the execution is force-cancelled, whereas `join_waiters`, `effects`, and `effect_conflicts` remain after target completion so joiners and conflict inspections can still read them.

## Cursor pagination & `VACUUM` caveat

Claim queries use the partial index on `next_claim_at` to fetch due executions first, applying any caller ID filter through `json_each`.

For history listing, `ExecutionsDao.listStates` reads SQLite's implicit `rowid` as a monotonic admission sequence assigned under the write lock, filtering backwards from the cursor and fetching one extra row (default limit 50, cap 1,000) to emit a next-page cursor only when more rows exist.

Because leases can expire without a sweep updating stored status, list queries compute running status dynamically whenever a lease has not yet expired. Because `executions` uses a text primary key rather than an integer alias, running `VACUUM` renumbers implicit row IDs and invalidates open pagination cursors.

## Startup upgrades

Three tables apply additive `ALTER TABLE` upgrades automatically when opening an existing database from an older version:

| Table | Startup column upgrade |
| :--- | :--- |
| `executions` | Adds `parent_detached` and cancellation columns plus `idx_executions_parent` if missing |
| `start_requests` | Adds `detached` column with default `0` if missing |
| `effect_conflicts` | Adds `status` and `conflicting_status` columns with default `'settled'` if missing |

`executions` and `start_requests` re-check `PRAGMA table_info` inside `runTransaction` so concurrent processes opening an older file apply each migration once.
