# Custom Runtimes

A `Runtime` persists workflow executions, step records, stores, external effects, and waiters across servers and worker processes.

Notation Durable ships `MemoryRuntime` for single-process tests and `SqliteRuntime` for single-node SQLite files. Implement a custom `Runtime` when multiple server or worker processes must share a remote database such as Postgres, Redis, or DynamoDB.

## Extending `Runtime`

Extend the abstract `Runtime` class from `@notation/durable/runtime` and expose a `clock` function alongside five client instances:

```ts [src/postgres-runtime.ts]
import { Runtime, type RuntimeOptions } from "@notation/durable/runtime";
import type { Pool } from "pg";
import { PostgresEffectClient } from "./postgres-effect";
import { PostgresExecutionClient } from "./postgres-execution";
import { PostgresHeapClient } from "./postgres-heap";
import { PostgresJoinClient } from "./postgres-join";
import { PostgresStoreClient } from "./postgres-store";

export class PostgresRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: PostgresExecutionClient;
  readonly effects: PostgresEffectClient;
  readonly heap: PostgresHeapClient;
  readonly joins: PostgresJoinClient;
  readonly stores: PostgresStoreClient;

  constructor(pool: Pool, options: RuntimeOptions = {}) {
    super();
    this.clock = options.clock ?? (() => Date.now());
    this.executions = new PostgresExecutionClient(pool, this.clock);
    this.effects = new PostgresEffectClient(pool, this.clock);
    this.heap = new PostgresHeapClient(pool, this.clock);
    this.joins = new PostgresJoinClient(pool);
    this.stores = new PostgresStoreClient(pool, this.clock);
  }
}
```

Default `clock` lazily with `() => Date.now()` rather than storing `Date.now` directly so test suites that install fake timers after constructing the runtime still control time across every client.

| Client | Responsibility |
| :--- | :--- |
| `executions` | Execution records, admission, leases, wakes, and cancellation state |
| `heap` | Step attempt records and fenced cancellation delivery |
| `stores` | Versioned store snapshots, applied-step receipts, and state waiters |
| `effects` | Effect state, conflict reports, and effect waiters |
| `joins` | Waiters for `step.join` and `step.call` |

Each client has its own interface or abstract base class exported from `@notation/durable/runtime`.

## Callers and multi-process sharing

Different parts of Durable call different subsets of the runtime, often from separate processes sharing the same backing database:

| Caller | Methods invoked |
| :--- | :--- |
| SDK and HTTP routes | `executions.admit`, `executions.get`, `executions.list`, `executions.cancel` |
| Execution dispatcher | `executions.claimDue`, `executions.get`, `executions.renew`, `executions.release`, `executions.acknowledge` |
| Workflow runner | `heap`, `stores`, `effects`, `joins`, `executions.start` |
| External store handles | `stores` |

Worker processes instantiate their own `Runtime` over the same database rather than tunneling every client call over IPC. Every write operation must serialize concurrent mutations across separate `Runtime` instances and processes.

## Atomic multi-client writes

Several operations in one client require a companion write in another client. If the primary write commits and a crash drops the companion write, waiting executions can stall permanently or fire against deleted steps.

| Triggering write | Required companion write |
| :--- | :--- |
| Store mutation satisfies a waiter | Request a wake on the waiting execution |
| Effect settles or fails | Request a wake on every watching execution |
| Execution reaches terminal status | Request a wake on every joining execution |
| Heap records a finished step | Delete store, effect, and join waiters for the step and `deleteWaitersOf` |
| Execution is cancelled | Propagate to non-detached children; force-cancel also deletes waiters and wakes joiners |

When all five clients share one transactional database such as Postgres or SQLite, execute both writes inside the same database transaction:

```ts [src/postgres-join.ts]
import {
  readJoinTarget,
  type JoinClient,
  type JoinResult,
  type JoinWaiter,
} from "@notation/durable/runtime";
import type { Pool } from "pg";
import { withTransaction } from "./postgres-tx";

export class PostgresJoinClient implements JoinClient {
  constructor(private pool: Pool) {}

  async join(waiter: JoinWaiter): Promise<JoinResult> {
    return withTransaction(this.pool, async (tx) => {
      const target = await tx.getExecutionStateForUpdate(waiter.targetExecutionId);
      const result = readJoinTarget(target);
      if (result.status === "waiting") {
        await tx.upsertJoinWaiter(waiter);
      }
      return result;
    });
  }
}
```

Putting `getExecutionStateForUpdate` and `upsertJoinWaiter` in one transaction prevents a terminal `acknowledge` from committing between reading the target's status and inserting the waiter.

If a runtime spreads clients across separate storage engines that cannot share one transaction, commit a durable outbox record alongside the primary write. An outbox worker can then apply companion wakes or waiter deletions idempotently.

Separating execution records from step records also changes how fencing works. Enforce the fencing token check atomically with the heap write itself instead of reading the execution record from a second database.

## Using state helpers

`@notation/durable/runtime` exports pure state-transition functions for executions, lists, effects, and joins alongside `CasStoreClient` for stores. Each helper accepts the stored state and returns the updated state and response without performing I/O.

Implementing a client method follows a consistent three-step transaction:

1. Validate or prepare the request before opening a transaction using helpers such as `prepareExecutionAdmission` or `prepareEffectBegin`.
2. Read the current stored state under a write lock inside the transaction, call the pure transition helper such as `claimExecution` or `settleEffect`, and persist the updated state only when it changed.
3. Run any required companion writes in the same transaction before committing.

Using the shared helpers keeps edge cases identical across backends – including canonical event encoding, fencing token checks, idempotent admission matching, and redelivery backoff.

## Verifying with conformance

Validate any custom runtime against the behavioral suites in `@notation/durable-conformance` (see [Conformance](./conformance.md)), or inspect the built-in memory and SQLite adapters in the repository.
