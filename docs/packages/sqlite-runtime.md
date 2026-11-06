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
| `requireStepKeys`   | `boolean`      | `false`      | Rejects a keyless step that has no record with `WorkflowReplayError` (`STEP_KEY_REQUIRED`). See [`requireStepKeys`](../manual/replay-stability.md#requirestepkeys) |

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
- `list` orders executions by the `rowid` of the `executions` table. SQLite assigns the `rowid` when the row is inserted, one more than the largest `rowid` in the table. The insert holds the write lock, so two processes never assign the same number, and a later admission always has a larger one, whatever the processes' clocks.
- An idempotent admission reads an existing record in the same transaction as its insert, so of two concurrent repeats one is accepted and the other sees its record.
- A new `start` inserts the target, already due and with its parent link, and the start request in one transaction. The request is looked up in the same transaction, so concurrent starts with one request ID admit one target.
- `cancel` records the request on the execution and on every non-terminal, attached descendant in one transaction. A forced cancel's `cancelled` outcome, the deletion of the waiters the execution holds, and the wakes for its join waiters commit in the same transaction.
- `recordCancellation` checks the lease, writes the step records, deletes those steps' store, effect and join waiters, and sets `cancellation_delivered_at` in one transaction.
- A step write, or `recordCancellation`, records the delivery's workflow version in `workflow_version` in the same transaction, when the execution has none. A `diverged` acknowledgement records the version and the `divergence_*` columns, and sets `next_claim_at` to `NULL`, in one transaction.

`SqliteRuntime` creates indexes on `executions(workflow_id)` and `executions(status)` when it opens the file, so a `list` filtered by `workflowId` or by a status other than `running` reads only the matching rows. Each index entry carries the `rowid`, so the matching rows are already in `list` order. No index serves `list({ status: "running" })`. Whether an execution is running depends on whether its lease has expired, which `list` decides from `lease_expires_at` and the current time, so SQLite reads the table in `rowid` order, newest first, and checks each row until it has filled the page. When a large table has few running executions, one page can read most of the table. Executions from earlier versions already have a `rowid` in insertion order, so they need no migration.

`VACUUM` may renumber the `rowid`s of the `executions` table, because the table has no `INTEGER PRIMARY KEY`. A `list` cursor obtained before a `VACUUM` may then skip or repeat executions. Start paging again from the first page after a `VACUUM`.

Cancellation adds columns to two tables. `executions` gains `parent_detached` (`INTEGER NOT NULL DEFAULT 0`) and the request's `cancellation_reason`, `cancellation_source`, `cancellation_forced`, `cancellation_requested_at` and `cancellation_delivered_at`; `cancellation_requested_at` is `NULL` when no request is recorded. `start_requests` gains `detached` (`INTEGER NOT NULL DEFAULT 0`). An index on `executions(parent_execution_id)` finds an execution's children. `SqliteRuntime` adds the columns and the index when it opens a database created before cancellation, under the write lock, so processes that open the file together add each column once. Existing rows read as attached children and as requests that were not detached, with no cancellation request.

Replay stability adds columns to two tables. `executions` gains `workflow_version` and the divergence's `divergence_code`, `divergence_step_key`, `divergence_workflow_version` and `divergence_detected_at`; `divergence_code` is `NULL` when the execution has no divergence. `step_responses` gains `step_kind`. `SqliteRuntime` adds the columns when it opens a database created before them, under the write lock, so processes that open the file together add each column once. Existing executions read as having no recorded version and no divergence, and existing step records have no kind, so a delivery does not compare their kind on replay.

A row in `join_waiters` can stay for good when no step result follows it: a stale holder registers after the step was recorded, or a deploy removes a join step that was still waiting. Its only effect is one extra delivery of the joiner when the target finishes. The runtime does not remove such rows.

Every process that shares one database must run a version with join support before any workflow uses `step.join` or `step.call`. A process on an earlier version acknowledges a terminal outcome without waking the join waiters on that execution, so their joiners wait until their deadline, or for good when they have none.

Every process that shares one database must also run a version with cancellation support before anyone calls `cancel`. A worker on an earlier version does not receive the request with its delivery, so it runs the workflow as if no request were recorded. A process on an earlier version that starts a child does not propagate the parent's pending request to it, and one that acknowledges a delivery or updates a row leaves the cancellation columns as they are.

Every process that shares one database must run a version with replay stability support before any execution is parked by a divergence. A process on an earlier version does not read the `divergence_*` columns. When it next writes the row, for example to record a wake, it computes `next_claim_at` without the divergence, so the parked execution is delivered again, by a process that does not check replay against the history.

Every write runs as one synchronous `IMMEDIATE` transaction. Separate processes that open the same file, such as the app and its worker subprocesses, therefore serialize on SQLite's lock and see the same execution records.

Updaters must be synchronous, deterministic, and side-effect-free. They run
against a snapshot outside the SQLite transaction; if the conditional commit
loses a version race, the base store client runs the updater again against the
latest snapshot.

`SqliteExecutionClient`, `SqliteEffectClient`, `SqliteHeapClient`, `SqliteJoinClient`, and `SqliteStoreClient` remain exported for adapter tests. Construct them through `SqliteRuntime` in applications, so the store client requests wakes from the same execution client and clock.
