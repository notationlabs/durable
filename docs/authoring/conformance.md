# Conformance

`@notation/durable-conformance` provides five [Vitest](https://vitest.dev) qualification suites that verify any runtime adapter against Notation Durable's execution, heap, store, effect, join, and cancellation contracts.

## Installation

Install `@notation/durable-conformance` alongside `vitest` version `4.1.12` or newer and `@notation/durable` at the matching minor version:

```sh [Terminal]
@pm.dev("@notation/durable-conformance vitest")
```

## Conformance suites

Each registration function registers its test cases directly with Vitest inside the file that invokes it:

| Suite | Verifies |
| :--- | :--- |
| `registerExecutionClientConformance` | Admission, `start`, `list`, wakes, claims, leases, fencing, and `acknowledge` |
| `registerStoreClientConformance` | Lazy creation, CAS updates, applied receipts, waiters, and path wakes |
| `registerEffectClientConformance` | `begin`, `settle` conflicts, `watch` wakes, invariants, and durability |
| `registerJoinClientConformance` | `join`, terminal wakes, parent links, waiter deletion, and durability |
| `registerCancellationClientConformance` | `cancel`, descendant propagation, `detached` children, and `recordCancellation` |

## Registering the suites

Pass `{ name, create }` to each suite. Vitest calls `create` once per test case, so `create` must initialize a clean runtime over an empty database.

This file registers all five suites against `SqliteRuntime` using temporary per-test SQLite files:

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

function createSqliteHarness(options?: { clock: () => number }) {
  const dir = mkdtempSync(join(tmpdir(), "durable-conformance-"));
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

registerStoreClientConformance({
  name: "sqlite",
  create: () => createSqliteHarness(),
});
registerExecutionClientConformance({
  name: "sqlite",
  create: createSqliteHarness,
});
registerEffectClientConformance({
  name: "sqlite",
  create: createSqliteHarness,
});
registerJoinClientConformance({
  name: "sqlite",
  create: createSqliteHarness,
});
registerCancellationClientConformance({
  name: "sqlite",
  create: createSqliteHarness,
});
```

Run the suite with Vitest:

```sh [Terminal]
npx vitest run test/conformance.test.ts
```

## Harness options

`create` returns a harness object whose only required property is `runtime`. Optional properties enable multi-connection race tests, crash-recovery checks, and resource cleanup:

| Option | Purpose |
| :--- | :--- |
| `runtime` | Primary `Runtime` instance under test |
| `createPeer` | Returns another `Runtime` connected to the same backing store |
| `restart` | Reopens the same persisted state in a fresh `Runtime` instance |
| `dispose` | Closes connections and removes temporary test resources after each case |

When `createPeer` is omitted, race tests fall back to calling `runtime` concurrently, which skips true multi-connection lock contention. When `restart` is omitted, persistence-across-restart tests are skipped.

Every suite except store conformance passes a deterministic `clock` into `create`. Forward that clock to the primary runtime and every peer or restarted instance so suites can test lease expiry and retry delays cleanly.

## Failure hooks

Three optional failure hooks verify all-or-nothing rollback when a multi-part transaction fails mid-write. Each hook makes the targeted write fail on `runtime` and returns a restore function that clears the fault:

| Hook | Target |
| :--- | :--- |
| `failStartCommits` | Every new `ExecutionClient.start` write on `runtime` in the execution suite |
| `failEffectWatches` | Every `EffectClient.watch` write on `runtime` in the effect suite |
| `failExecutionWrites` | Every write to a named `executionId` on `runtime` in the join and cancellation suites |

If a hook is omitted from the harness, its corresponding atomicity test is skipped. For SQLite adapters, temporary triggers provide clean mid-transaction fault injection:

```ts [test/failure-hooks.ts]
import type { SqliteDriver } from "@notation/durable/sqlite";

export function sqliteFailureHooks(db: SqliteDriver) {
  return {
    failStartCommits() {
      db.run(`
        CREATE TEMP TRIGGER fail_start_commit BEFORE INSERT ON start_requests
        BEGIN SELECT RAISE(ABORT, 'injected start failure'); END
      `);
      return () => db.run("DROP TRIGGER temp.fail_start_commit");
    },
    failEffectWatches() {
      db.run(`
        CREATE TEMP TRIGGER fail_effect_watch BEFORE INSERT ON effect_waiters
        BEGIN SELECT RAISE(ABORT, 'injected watch failure'); END
      `);
      return () => db.run("DROP TRIGGER temp.fail_effect_watch");
    },
    failExecutionWrites(executionId: string) {
      db.run(`
        CREATE TEMP TRIGGER fail_execution_write BEFORE UPDATE ON executions
        WHEN OLD.execution_id = '${executionId.replaceAll("'", "''")}'
        BEGIN SELECT RAISE(ABORT, 'injected execution write failure'); END
      `);
      return () => db.run("DROP TRIGGER temp.fail_execution_write");
    },
  };
}
```

## Gaps not covered by the suites

The conformance suites test the five runtime clients directly and leave transport, process, and schema-evolution concerns outside their scope:

- **Runner, dispatcher, and worker invoker** – `WorkflowRunner`, `ExecutionDispatcher`, and worker IPC invokers are runtime-agnostic and tested separately in Durable's core suites.
- **Multi-process clock drift** – Execution, effect, join, and cancellation suites share one deterministic test clock across the runtime and peers rather than simulating skewed host clocks.
- **Abrupt process kills and IPC drops** – `restart` reopens storage cleanly after client promises settle; it does not send `SIGKILL` mid-IPC or corrupt partial filesystem flushes.
- **Schema migrations** – Every test starts from an empty database and does not verify upgrading tables created by earlier runtime versions.
