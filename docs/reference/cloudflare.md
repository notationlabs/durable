# @notation/durable/cloudflare

`@notation/durable/cloudflare` runs the SQLite runtime on a Durable Object's SQLite storage and drives deliveries from the object's alarm. See [Cloudflare Workers](../core/cloudflare.md) for setup.

```ts [src/worker.ts]
import { DurableObject } from "cloudflare:workers";
import { createWorkflowObject } from "@notation/durable/cloudflare";
import { router } from "./workflows/router";

export class Workflows extends DurableObject {
  durable = createWorkflowObject(this.ctx, { router });

  fetch(request: Request) {
    return this.durable.fetch(request);
  }

  alarm() {
    return this.durable.alarm();
  }
}
```

## createWorkflowObject

```ts [@notation/durable/cloudflare]
export function createWorkflowObject<Router extends WorkflowRouter>(
  ctx: DurableObjectStateLike,
  options: WorkflowObjectOptions<Router>
): {
  runtime: SqliteRuntime;
  invoker: WorkflowInvoker;
  fetch(request: Request): Promise<Response>;
  alarm(): Promise<void>;
};

export type WorkflowObjectOptions<Router extends WorkflowRouter> = {
  router: Router;
  logger?: Logger;
  basePath?: `/${string}`;
  middleware?: MiddlewareFunction[];
  observationMiddleware?: ObservationMiddlewareFunction[];
  maxWait?: number;
  concurrency?: number;
  leaseDuration?: number;
  maxExpiredDeliveries?: number;
};
```

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `router` | required | The workflows the object runs |
| `logger` | writes to `console` | A pino-compatible logger |
| `basePath` | `""` | Path prefix of the HTTP routes |
| `middleware` | `[]` | Runs before admission on `/trigger` |
| `observationMiddleware` | `[]` | Runs before `/poll`, `/wait`, `/list`, `/events` and `/cancel` |
| `maxWait` | `8000` | Longest a `/wait` request stays open, in milliseconds |
| `concurrency` | `10` | Most deliveries in flight at once |
| `leaseDuration` | `30_000` | Lease length in milliseconds |
| `maxExpiredDeliveries` | no limit | Consecutive deliveries of one execution that may end with an expired lease before the execution is recorded `failed` |

| Member | Behavior |
| :--- | :--- |
| `runtime` | The `SqliteRuntime` over the object's storage, for the local SDK, stores and effects |
| `invoker` | The in-process invoker; pass it to `createLocalSdk` so `wait` returns as soon as an outcome is recorded |
| `fetch` | Serves the [HTTP routes](./http.md). Responds `404` to other paths and `405` to methods other than `POST` |
| `alarm` | Claims due executions, and resolves when no delivery is in flight or after five minutes. If claiming fails, it sets the alarm to run again; it rejects only when it cannot set the alarm |

`createWorkflowObject` creates the runtime's tables when it is called, so call it in the Durable Object's constructor or a field initializer.

## DurableObjectSqliteDriver

`createWorkflowObject` builds its own driver. Construct one directly to use `SqliteRuntime` on Durable Object storage with your own dispatching:

```ts [@notation/durable/cloudflare]
export class DurableObjectSqliteDriver implements SqliteDriver {
  constructor(storage: DurableObjectStorageLike);
  onCommit: (() => void) | undefined;
  run(sql: string): void;
  query<Row>(sql: string): SqliteStatement<Row>;
  transaction<R>(fn: () => R): R;
  close(): void;
}

export function createSqliteDb(storage: DurableObjectStorageLike): DurableObjectSqliteDriver;
```

The driver differs from the Bun and Node drivers in three ways:

- **Positional parameters** – Durable Object SQL binds by position, so the driver rewrites each `$name` to `?` and orders the values to match.
- **Transactions** – Durable Object SQL rejects `BEGIN` and `COMMIT`, so `transaction` calls `ctx.storage.transactionSync`.
- **`onCommit`** – Called after each transaction commits and after each write outside a transaction, before the caller's next `await`. `createWorkflowObject` uses it to set the alarm in the same turn as the write.

## Storage types

`DurableObjectStateLike` and `DurableObjectStorageLike` list the members of `ctx` and `ctx.storage` that the package uses. `DurableObjectState` and `DurableObjectStorage` from `@cloudflare/workers-types` satisfy them, so the package does not depend on those types:

```ts [@notation/durable/cloudflare]
export interface DurableObjectStorageLike {
  sql: { exec(query: string, ...bindings: any[]): { toArray(): any[] } };
  transactionSync<R>(closure: () => R): R;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
}

export interface DurableObjectStateLike {
  storage: DurableObjectStorageLike;
  waitUntil(promise: Promise<unknown>): void;
}
```
