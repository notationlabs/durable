# @notation/durable/cloudflare

`@notation/durable/cloudflare` runs the SQLite runtime on a Durable Object's SQLite storage, claims executions on the object's alarm, and runs each delivery in another instance of the same class. See [Cloudflare Workers](../core/cloudflare.md) for setup.

```ts [src/worker.ts]
import { WorkflowObject } from "@notation/durable/cloudflare";
import { router } from "./workflows/router";

export class Workflows extends WorkflowObject({ router }) {}
```

## WorkflowObject

```ts [@notation/durable/cloudflare]
export function WorkflowObject<Router extends WorkflowRouter>(
  options: WorkflowObjectOptions<Router>
): new (ctx: DurableObjectStateLike, env: any) => DurableObject & {
  readonly durable: ReturnType<typeof createWorkflowObject<Router>>;
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
  isolateDeliveries?: boolean;
};
```

Returns a Durable Object class that extends `DurableObject` from `cloudflare:workers`. Export the class, or a class that extends it, under the `class_name` in `wrangler.jsonc`.

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
| `isolateDeliveries` | `true` | Runs each delivery in another instance of the class. With `false`, deliveries run in the isolate of the object that claims them |

| Member | Behavior |
| :--- | :--- |
| `durable` | The host that [`createWorkflowObject`](#createworkflowobject) returns, such as `this.durable.runtime` for the local SDK. The first read creates the runtime's tables |
| `fetch` | Serves the [HTTP routes](./http.md). Responds `404` to other paths and `405` to methods other than `POST`. A subclass can override it to serve other paths, and call `super.fetch` for the routes |
| `alarm` | Claims due executions, and resolves when no delivery is in flight or after five minutes. If claiming fails, it sets the alarm to run again; it rejects only when it cannot set the alarm |

### Instances that run deliveries

The object that receives requests is the one that holds the runtime and claims executions. It runs each delivery in another instance of the same class:

- **Names** – The object finds the namespace of its own class in `ctx.exports` or `env`, and gets each instance with `idFromName` from its own ID and a slot number. An object in a jurisdiction creates its instances in the same one.
- **Pool** – A slot runs one delivery at a time, and the object uses the lowest free slot. So the number of instances is the most deliveries that have run at once, which `concurrency` limits.
- **Storage** – An instance that runs a delivery reads and writes nothing in its own storage. Every runtime call goes to the object that claimed the delivery.
- **Stopping** – The object aborts the instance when the delivery loses its lease. The instance aborts itself when the object that claimed the delivery has stopped.

The class has two more methods, `durableRunDelivery` and `durableStopDelivery`, which the object calls on these instances. A subclass must not define methods with these names.

A subclass's constructor and field initializers run in every instance, the ones that run deliveries included. Code there that writes to `this.ctx.storage` creates that storage in each of them.

When the object cannot find the namespace of its own class, the first request fails with an error that says so. Every class in `durable_objects.bindings` of the Worker that exports it is found.

## createWorkflowObject

Builds the host inside `WorkflowObject`, without the instances that run deliveries: every delivery runs in the isolate of the object that calls it. Call it directly only to write the Durable Object class yourself. The class must forward `fetch` and `alarm` to it.

```ts [@notation/durable/cloudflare]
export function createWorkflowObject<Router extends WorkflowRouter>(
  ctx: DurableObjectStateLike,
  options: Omit<WorkflowObjectOptions<Router>, "isolateDeliveries">
): {
  runtime: SqliteRuntime;
  invoker: WorkflowInvoker;
  fetch(request: Request): Promise<Response>;
  alarm(): Promise<void>;
};
```

| Member | Behavior |
| :--- | :--- |
| `runtime` | The `SqliteRuntime` over the object's storage, for the local SDK, stores and effects |
| `invoker` | The object's invoker; pass it to `createLocalSdk` so `wait` returns as soon as an outcome is recorded |
| `fetch` | As on the class |
| `alarm` | As on the class |

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
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
}

export interface DurableObjectStateLike {
  storage: DurableObjectStorageLike;
  waitUntil(promise: Promise<unknown>): void;
  // The three below are used only to run deliveries in other instances.
  id?: { toString(): string; jurisdiction?: string };
  exports?: object;
  abort?(reason?: string): void;
}
```
