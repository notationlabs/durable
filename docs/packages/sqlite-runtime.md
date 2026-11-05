# @yieldstar/sqlite-runtime

`SqliteRuntime`: a driver-agnostic SQLite implementation of the `Runtime` contract, with execution records, the heap, and durable stores in one database. Bun and Node connector entry points provide the native database driver.

See [Bun with SQLite](../runtimes/resident-process-bun.md) and [Node with SQLite](../runtimes/resident-process-node.md) for complete resident-process wiring.

## Install

```sh
bun add @yieldstar/sqlite-runtime
```

## `createSqliteDb({ path })`

Creates and returns a `SqliteDriver` at the given file path. Tables are created automatically on first use. Use the `/bun` connector with Bun or the `/node` connector with Node.

```ts
import { createSqliteDb } from "@yieldstar/sqlite-runtime/bun";

const db = createSqliteDb({ path: "./.db/local.sqlite" });
```

The Node connector has the same API:

```ts
import { createSqliteDb } from "@yieldstar/sqlite-runtime/node";

const db = createSqliteDb({ path: "./.db/local.sqlite" });
```

## `SqliteRuntime`

```ts
import { SqliteRuntime, createSqliteDb } from "@yieldstar/sqlite-runtime/bun";

const runtime = new SqliteRuntime(createSqliteDb({ path: "./.db/local.sqlite" }));
```

| Option              | Type           | Default      | Description                                                              |
| ------------------- | -------------- | ------------ | ------------------------------------------------------------------------ |
| `clock`             | `() => number` | `Date.now`   | Time source for every client operation and for delay and retry wake times |

The runtime exposes five clients that share the database:

| Client               | Class                   | Tables                                   |
| -------------------- | ----------------------- | ---------------------------------------- |
| `runtime.executions` | `SqliteExecutionClient` | `executions`, `start_requests`           |
| `runtime.joins`      | `SqliteJoinClient`      | `join_waiters`                           |
| `runtime.effects`    | `SqliteEffectClient`    | `effects`, `effect_conflicts`, `effect_waiters` |
| `runtime.heap`       | `SqliteHeapClient`      | `step_responses`                         |
| `runtime.stores`     | `SqliteStoreClient`     | `stores`, `store_waiters`, `store_applied_steps` |

Keeping them in one database lets every guarantee that spans clients hold in a single SQLite transaction:

- A store commit and the wakes for the waiters it satisfies commit together.
- An effect settlement and the wakes for its waiters commit together.
- A terminal acknowledgement and the wakes for the join waiters on that execution commit together. The waiters stay in `join_waiters`.
- A join reads the target's record and registers its waiter in one transaction, so a waiter registered as the target becomes terminal is still woken.
- The heap write that records a step's result deletes that step's store, effect and join waiters.
- Admission inserts the record already due, so the record and its first wake commit together.
- Admission assigns the record the next `admission_sequence`, which orders `list`, in the same statement as the insert. A trigger on `executions` takes the number from a single-row counter table, `execution_admission_counter`. The insert holds the write lock, so two processes never assign the same number. The counter only increases, so a number is not reissued after a row is deleted.
- An idempotent admission reads an existing record in the same transaction as its insert, so of two concurrent repeats one is accepted and the other sees its record.
- A new `start` inserts the target, already due and with its parent link, and the start request in one transaction. The request is looked up in the same transaction, so concurrent starts with one request ID admit one target.

A database created before `list` has no `admission_sequence` column. `SqliteRuntime` adds it when it opens the file, fills it from each row's `rowid`, which follows insertion order as long as no row was deleted, and starts the counter above the largest value. The column allows `NULL` and has no default. A process still on an earlier version inserts rows without the column, and the trigger gives each of them the next number, so old and new processes can share the file during a rolling upgrade.

A row in `join_waiters` can stay for good when no step result follows it: a stale holder registers after the step was recorded, or a deploy removes a join step that was still waiting. Its only effect is one extra delivery of the joiner when the target finishes, and `getWaiters` may list it. The runtime does not remove such rows.

Every process that shares one database must run a version with join support before any workflow uses `step.join` or `step.call`. A process on an earlier version acknowledges a terminal outcome without waking the join waiters on that execution, so their joiners wait until their deadline, or for good when they have none.

Every write runs as one synchronous `IMMEDIATE` transaction. Separate processes that open the same file, such as the app and its worker subprocesses, therefore serialize on SQLite's lock and see the same execution records.

Updaters must be synchronous, deterministic, and side-effect-free. They run
against a snapshot outside the SQLite transaction; if the conditional commit
loses a version race, the base store client runs the updater again against the
latest snapshot.

`SqliteExecutionClient`, `SqliteEffectClient`, `SqliteHeapClient`, `SqliteJoinClient`, and `SqliteStoreClient` remain exported for adapter tests. Construct them through `SqliteRuntime` in applications, so the store client requests wakes from the same execution client and clock.
