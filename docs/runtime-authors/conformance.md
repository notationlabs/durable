# Conformance

`@notation/durable-conformance` has [Vitest](https://vitest.dev) suites that check a runtime against the contracts on these pages. Each suite registers its tests in the file that calls it:

| Suite | Checks | Page |
| --- | --- | --- |
| `registerExecutionClientConformance` | Admission, `start`, `list`, wakes, claims, leases, fencing and acknowledgement | [Execution client](./execution-client.md), [Heap client](./heap-client.md) |
| `registerStoreClientConformance` | Store creation, updates, conflicts, applied steps, waiters and their wakes | [Store client](./store-client.md) |
| `registerEffectClientConformance` | `begin`, settlements and their conflicts, and watches | [Effect client](./effect-client.md) |
| `registerJoinClientConformance` | `join`, and the wakes of an execution's end | [Join client](./join-client.md) |
| `registerCancellationClientConformance` | `cancel`, its propagation to children, and `recordCancellation` | [Execution client](./execution-client.md#cancelling), [Heap client](./heap-client.md#cancellation) |

## Installation

The suites need Vitest 4.1.12 or newer:

```sh
@pm.dev("@notation/durable-conformance vitest")
```

## Registering the suites

Each suite takes a name and a `create` function. Each test calls `create`, so it must return a runtime over an empty database.

This file runs every suite against `SqliteRuntime`, with a new database file for each test:

```ts [test/conformance.test.ts]
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@notation/durable-conformance";
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/node";

function sqlite(options?: { clock: () => number }) {
  const dir = mkdtempSync(join(tmpdir(), "conformance-"));
  const path = join(dir, "durable.sqlite");
  const dbs = [createSqliteDb({ path })];

  const open = () => {
    const db = createSqliteDb({ path });
    dbs.push(db);
    return new SqliteRuntime(db, options);
  };

  return {
    runtime: new SqliteRuntime(dbs[0]!, options),
    createPeer: open,
    restart: open,
    dispose() {
      for (const db of dbs) db.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

registerStoreClientConformance({ name: "sqlite", create: () => sqlite() });
registerExecutionClientConformance({ name: "sqlite", create: sqlite });
registerEffectClientConformance({ name: "sqlite", create: sqlite });
registerJoinClientConformance({ name: "sqlite", create: sqlite });
registerCancellationClientConformance({ name: "sqlite", create: sqlite });
```

Vitest runs the file:

```sh
vitest run test/conformance.test.ts
```

## Harness

`create` returns a harness. Only `runtime` is required, but each optional field turns on tests that are otherwise skipped or weaker:

| Field | Returns | Without it |
| --- | --- | --- |
| `runtime` | The runtime under test | Required |
| `createPeer` | Another runtime over the same database, as another process would open | Tests that race two runtimes run both sides on `runtime`, which proves less |
| `restart` | A new runtime over the same database, as after a restart | Tests that records survive a restart are skipped |
| `dispose` | Nothing. Closes the database | Nothing is closed |

**Return `createPeer` if several processes can share the database.** Races between two connections, such as two claims of one execution, are the cases where a runtime is most likely to be wrong.

Every suite except the store suite passes `create` a `clock`. Build every runtime on it, including peers, so that the suite controls time, for example to make a lease expire.

## Failure hooks

Some tests check that a write commits all of its parts or none. They need a way to make a write fail after it has started. The harness provides it with one of these fields, which returns a function that stops the failures:

| Field | Suite | Makes fail |
| --- | --- | --- |
| `failStartCommits()` | Execution | Every new `start` on `runtime` |
| `failEffectWatches()` | Effect | Every `watch` on `runtime` |
| `failExecutionWrites(executionId)` | Join, cancellation | Every write to that execution's record on `runtime` |

Without the field, those tests are skipped. Durable's own tests make SQLite fail with a temporary trigger, for example:

```ts
failExecutionWrites(executionId) {
  db.run(`
    CREATE TEMP TRIGGER fail_execution_write BEFORE UPDATE ON executions
    WHEN OLD.execution_id = '${executionId.replaceAll("'", "''")}'
    BEGIN SELECT RAISE(ABORT, 'injected execution write failure'); END
  `);
  return () => db.run("DROP TRIGGER temp.fail_execution_write");
},
```

## What the suites do not check

The suites check the runtime alone. The workflow runner, the dispatcher and the worker invoker are the same for every runtime, and Durable's repository tests them.
