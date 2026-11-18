# @notation/durable-conformance

`@notation/durable-conformance` provides five Vitest suites that verify a custom `Runtime` implementation against Notation Durable's execution, store, effect, join, and cancellation guarantees.

```sh [Terminal]
@pm.dev("@notation/durable-conformance vitest")
```

`@notation/durable-conformance` requires `@notation/durable` at a matching minor version and `vitest` `^4.1.12` as peer dependencies. For guidance on authoring an adapter before running the suites, see [Conformance](../authoring/conformance.md).

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

## Suite registration

Each registration function registers a `describe` block with Vitest in the calling test file. Pass `name` to label the test suite and `create` to build an isolated harness per test case:

```ts [@notation/durable-conformance]
export function registerExecutionClientConformance(
  options: ExecutionConformanceOptions
): void;

export function registerStoreClientConformance(
  options: StoreConformanceOptions
): void;

export function registerEffectClientConformance(
  options: EffectConformanceOptions
): void;

export function registerJoinClientConformance(
  options: JoinConformanceOptions
): void;

export function registerCancellationClientConformance(
  options: CancellationConformanceOptions
): void;
```

| Function | Verifies |
| :--- | :--- |
| `registerExecutionClientConformance` | Admission, `start`, `list`, wakes, claims, leases, fencing, and acknowledgement |
| `registerStoreClientConformance` | Store lifecycle, CAS updates, applied-step receipts, waiters, and commit wakes |
| `registerEffectClientConformance` | Effect `begin`, `settle`, conflicts, `watch`, wakes, and error invariants |
| `registerJoinClientConformance` | Target reads, join waiter registration, terminal wakes, and waiter cleanup |
| `registerCancellationClientConformance` | Graceful and forced `cancel`, descendant propagation, `detached`, and `recordCancellation` |

## Harness contracts

Each suite defines its own options, factory, and harness types:

```ts [@notation/durable-conformance]
export type ExecutionConformanceHarnessOptions = {
  clock: () => number;
};

export type ExecutionConformanceHarness = {
  runtime: Runtime;
  createPeer?(): Runtime | Promise<Runtime>;
  restart?(): Runtime | Promise<Runtime>;
  failStartCommits?():
    | (() => void | Promise<void>)
    | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type ExecutionConformanceOptions = {
  name: string;
  create(
    options: ExecutionConformanceHarnessOptions
  ): ExecutionConformanceHarness | Promise<ExecutionConformanceHarness>;
};

export type StoreConformanceHarness = {
  runtime: Runtime;
  createPeer?(): Runtime | Promise<Runtime>;
  restart?(): Runtime | Promise<Runtime>;
  dispose?(): void | Promise<void>;
};

export type StoreConformanceFactory = () =>
  | StoreConformanceHarness
  | Promise<StoreConformanceHarness>;

export type StoreConformanceOptions = {
  name: string;
  create: StoreConformanceFactory;
};

export type EffectConformanceHarnessOptions = {
  clock: () => number;
};

export type EffectConformanceHarness = {
  runtime: Runtime;
  createPeer?(): Runtime | Promise<Runtime>;
  restart?(): Runtime | Promise<Runtime>;
  failEffectWatches?():
    | (() => void | Promise<void>)
    | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type EffectConformanceOptions = {
  name: string;
  create(
    options: EffectConformanceHarnessOptions
  ): EffectConformanceHarness | Promise<EffectConformanceHarness>;
};

export type JoinConformanceHarnessOptions = {
  clock: () => number;
};

export type JoinConformanceHarness = {
  runtime: Runtime;
  createPeer?(): Runtime | Promise<Runtime>;
  restart?(): Runtime | Promise<Runtime>;
  failExecutionWrites?(
    executionId: string
  ): (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type JoinConformanceOptions = {
  name: string;
  create(
    options: JoinConformanceHarnessOptions
  ): JoinConformanceHarness | Promise<JoinConformanceHarness>;
};

export type CancellationConformanceHarnessOptions = {
  clock: () => number;
};

export type CancellationConformanceHarness = {
  runtime: Runtime;
  createPeer?(): Runtime | Promise<Runtime>;
  restart?(): Runtime | Promise<Runtime>;
  failExecutionWrites?(
    executionId: string
  ): (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type CancellationConformanceOptions = {
  name: string;
  create(
    options: CancellationConformanceHarnessOptions
  ): CancellationConformanceHarness | Promise<CancellationConformanceHarness>;
};
```

`registerStoreClientConformance` invokes `create()` with no arguments. The other four suites pass `{ clock }`, which the runtime under test must pass into its `RuntimeOptions` so lease expiry, due times, and redelivery backoff are deterministic.

| Hook | Purpose |
| :--- | :--- |
| `runtime` | Primary `Runtime` instance for the test case |
| `createPeer` | Returns a second `Runtime` connected to the same backend |
| `restart` | Reopens the same durable backend in a new `Runtime` instance |
| `dispose` | Cleans up connections and temporary state after each test |
| `failStartCommits` | Injects a failure before `start` commits and returns a restore callback |
| `failEffectWatches` | Injects a failure before `watch` commits and returns a restore callback |
| `failExecutionWrites` | Injects a failure on writes to `executionId` and returns a restore callback |

`runtime` is required; all other hooks are optional:

- **Missing `createPeer`** – Concurrent race tests fall back to `runtime`, which skips verifying cross-client serialization.
- **Missing `restart`** – Persistence-across-restart tests are skipped.
- **Missing failure hooks** – Partial-commit rollback tests across `start`, `watch`, `join`, and `cancel` are skipped when failure hooks are omitted.
- **Per-test cleanup** – `dispose()` runs in a `finally` block after every test case.
