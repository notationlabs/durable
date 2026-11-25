# Cloudflare Workers

Notation Durable runs on Cloudflare in Durable Objects of one class. One object holds the runtime in its SQLite storage, serves the routes, and claims executions when its alarm fires. It runs each delivery in another instance of the same class, so every delivery has an isolate and a CPU limit to itself. Workflow and router files are the same as on Bun and Node.

## How the pieces map

A Bun or Node deployment has a server process, worker processes and a SQLite file. On Cloudflare, Durable Objects of one class do all three jobs:

| Bun or Node | Cloudflare |
| :--- | :--- |
| SQLite file opened with `createSqliteDb` | The object's SQLite storage (`ctx.storage.sql`) |
| `ExecutionDispatcher` polling every 10 ms | The object's alarm, set to the next time an execution is due |
| Worker processes forked by `createWorkflowInvoker` | Other instances of the same class, one for each delivery in flight |
| `createRoutes` mounted in `Bun.serve` or `node:http` | The object's `fetch` handler |

The third row behaves like worker processes do. A step that fails, waits or uses a lot of CPU time affects its own delivery and no other, and the object can stop one delivery without stopping the rest. [Where deliveries run](#where-deliveries-run) describes these instances.

## Setup

`WorkflowObject` from `@notation/durable/cloudflare` returns a Durable Object class. The class builds the runtime, the dispatcher and the HTTP routes from the object's `ctx`, serves the routes from `fetch`, and resumes executions from `alarm`. The Worker forwards requests to one instance of it:

::code-group

```ts [src/worker.ts]
import { WorkflowObject } from "@notation/durable/cloudflare";
import { ownExecutionsOnly, requireUser } from "./auth";
import { router } from "./workflows/router";

export class Workflows extends WorkflowObject({
  router,
  basePath: "/durable",
  middleware: [requireUser], // runs on /trigger
  observationMiddleware: [ownExecutionsOnly], // runs on /poll, /wait, /list, /events and /cancel
  maxExpiredDeliveries: 10,
}) {}

export default {
  fetch: (request, env) => env.WORKFLOWS.getByName("default").fetch(request),
} satisfies ExportedHandler<Env>;
```

```jsonc [wrangler.jsonc]
{
  "name": "my-app",
  "main": "src/worker.ts",
  "compatibility_date": "2026-10-15",
  "compatibility_flags": ["nodejs_compat"],
  "durable_objects": {
    "bindings": [{ "name": "WORKFLOWS", "class_name": "Workflows" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Workflows"] }]
}
```

::

What each setting is for:

- **`middleware` and `observationMiddleware`** – These decide who may call the routes. The Worker forwards every request to the object, so without them anyone who can reach the Worker's URL can start workflows, read every execution's params and results with `/list`, and cancel executions. `requireUser` and `ownExecutionsOnly` are the functions from [HTTP Server](./http-server.md#auth-and-execution-privacy).
- **`maxExpiredDeliveries`** – This ends an execution as `failed` when its deliveries keep being interrupted, instead of delivering it again forever. It is optional. See [Limiting redelivery](#limiting-redelivery).
- **`nodejs_compat`** – The SDK imports `node:events` and `node:crypto`.
- **`new_sqlite_classes`** – The class must be SQLite-backed. A key-value-backed Durable Object has no `ctx.storage.sql`.

The instances that run deliveries need no setup of their own. They are instances of `Workflows`, and the object finds them through the `WORKFLOWS` binding.

## Triggering executions

The object serves the same routes as [`createRoutes`](./http-server.md) under `basePath`: `/trigger`, `/poll`, `/wait`, `/list`, `/events` and `/cancel`. The [HTTP SDK](./http-sdk.md) calls them over the public URL, or from another Worker through the object's stub by passing `fetch`:

```ts [src/signup.ts]
import { createHttpSdkFactory } from "@notation/durable/client";
import type { Router } from "./workflows/router";

const createSdk = createHttpSdkFactory<Router>();

export async function signup(env: Env, email: string, sessionToken: string) {
  const object = env.WORKFLOWS.getByName("default");
  const sdk = createSdk({
    url: "http://workflows/durable", // only the path is used
    fetch: (input, init) => object.fetch(input, init),
    fetchOptions: { headers: { Authorization: `Bearer ${sessionToken}` } },
  });

  return sdk.trigger({ workflowId: "onboarding", params: { email } });
}
```

A request through the stub still runs the object's middleware, so it needs the same credentials as a request from outside.

The class extends `DurableObject` from `cloudflare:workers`, so a subclass can add RPC methods. They can call the [local SDK](./local-sdk.md) on `this.durable.runtime`:

```ts [src/worker.ts]
import { createLocalSdk } from "@notation/durable";

export class Workflows extends WorkflowObject({ router, basePath: "/durable" }) {
  startOnboarding(email: string) {
    const sdk = createLocalSdk<Router>(this.durable.runtime, { invoker: this.durable.invoker });
    return sdk.trigger({ workflowId: "onboarding", params: { email } });
  }
}
```

Another Worker then calls `env.WORKFLOWS.getByName("default").startOnboarding(email)`. The local SDK writes to the runtime directly and runs no middleware. Any write to the runtime, from a route or from the local SDK, sets the alarm and starts deliveries, so there is nothing else to call.

## When deliveries run

The object does not poll. It claims executions at three moments:

1. **After a write.** A trigger, a cancel, a store commit or an effect settlement commits to SQLite. In the same turn the object makes sure its alarm covers the earliest time any execution is due, and claims what is due now.
2. **While deliveries are in flight.** The object claims again when a delivery settles and when the next due time arrives.
3. **When the alarm fires.** Once no delivery is in flight the object stops. The alarm starts it again at the next due time, such as the end of a `step.delay`.

Cloudflare bills each alarm write as a row written, so the object writes the alarm only when it has to:

- **An execution that is due now** – The object claims it at once, and the alarm is needed only if the object stops first. The object sets the alarm one lease ahead (`leaseDuration`, 30 seconds by default), which is also where the alarm must be while the delivery runs. If every delivery slot is busy, it sets the alarm to the current time instead.
- **An execution that is due later** – The object sets the alarm to that time.
- **Nothing left to claim** – The object leaves an alarm that is at most a lease ahead. It fires once, finds nothing, and sets no other alarm. That call to `alarm` costs less than the write that would delete the alarm, and an execution that arrives before it fires needs no alarm write. The object deletes an alarm that is further ahead.

A delivery can run longer than one alarm handler. Cloudflare stops an alarm handler after 15 minutes, so `alarm` returns after five minutes and the delivery carries on. In a test on a deployed Worker, a 17-minute step finished on its first attempt with no requests to the object.

## Where deliveries run

The object that the Worker addresses, `getByName("default")` in the setup above, holds the runtime. For each delivery it calls another instance of `Workflows`:

```text
Worker ── fetch ──▶ Workflows "default"            runtime, routes, alarm
                      ├── RPC ──▶ Workflows "<id>/delivery/0"    one delivery
                      ├── RPC ──▶ Workflows "<id>/delivery/1"    one delivery
                      └── …
```

- **One delivery at a time** – An instance runs one delivery, and the object uses the lowest-numbered instance that is free. So the object uses as many instances as deliveries have run at once, and `concurrency` limits that to 10 by default.
- **Its own isolate** – Each instance runs in an isolate of its own. In a test on a deployed Worker, five steps that waited at the same time ran in five isolates, none of them the object's.
- **No storage** – The instance runs the workflow function and its step functions. It sends each step result, store update and effect to the object, which writes it to SQLite. The instance writes nothing to its own storage.
- **Stopped with its delivery** – The object aborts the instance when the delivery loses its lease. The instance aborts itself when the object stops, at most a third of the lease duration later.

**The constructor runs in these instances too.** A subclass constructor or field initializer that writes to `this.ctx.storage` creates that storage in every instance that runs a delivery. Keep such code in the methods that use it.

### What it costs

- **A call for each write** – A step result is one call from the instance to the object, which answers when the write is durable. In tests on a deployed Worker, the next step started 12 to 21 ms after a step function returned, against under 1 ms with deliveries in the object. Reading a finished step costs no call: the object sends the execution's step records with each delivery.
- **Durable Object duration** – Cloudflare bills each instance as a Durable Object while it runs a delivery, in addition to the object that claimed it.

### Running deliveries in the object

`isolateDeliveries: false` runs every delivery in the object's own isolate, with no other instances:

```ts [src/worker.ts]
export class Workflows extends WorkflowObject({ router, isolateDeliveries: false }) {}
```

Steps that await I/O take turns in one isolate, so this works for workflows whose steps only wait on other services. It removes the call for each write and the second object's duration. In exchange, the deliveries share one CPU limit, and the object cannot stop one of them:

| | Other instances (default) | In the object |
| :--- | :--- | :--- |
| A step computes without awaiting | Other deliveries and requests carry on | Every delivery and request waits |
| A delivery exceeds the CPU limit | Cloudflare resets that delivery's instance | Cloudflare resets the object, with every delivery in it |
| A forced cancellation during a step | The step stops at its `await` | The step function runs to its end |

In a test on a deployed Worker, one step spun past the CPU limit next to five steps that waited two minutes, with `maxExpiredDeliveries: 3`:

| | Other instances (default) | In the object |
| :--- | :--- | :--- |
| The five waiting executions | All `completed`, and each step function started once | All `failed` with `DeliveryLimitError`, after three deliveries each |
| The CPU-bound execution | `failed` with `DeliveryLimitError` | `failed` with `DeliveryLimitError` |
| Requests to the object, sent one after another | 145 sent, and none took over 2 seconds | 26 sent, and two of them took 35 and 37 seconds |
| Instances of the object that Cloudflare started | 1 | 7 |

## When a step runs again

`step.run` runs a step body at least once on every host. If the process stops while a step is running, the step does not finish. Its lease expires, and the next delivery replays the steps that finished and **runs the interrupted step from the start**. [Running a Server](./running-a-server.md#graceful-shutdown) describes this for a server crash on Bun and Node.

So the practice is the same on every host: **write every `step.run` body so that running it twice is safe.** Use [`step.effect`](./external-effects.md) for an operation that must not happen twice, such as charging a card.

On Cloudflare a step is interrupted in three situations:

| Event | Steps that run again |
| :--- | :--- |
| A deploy | The steps that are still running when the old code stops. The object and its instances keep running the old code for a while first, so short steps finish |
| Cloudflare restarts the object or an instance | The steps that the restarted instance was running. When the object restarts, the instances that run its deliveries stop within a third of the lease duration |
| A delivery exceeds the CPU limit | The step that was running in that delivery. See [CPU-bound steps](#cpu-bound-steps) |

An interrupted step starts again after its lease expires and the redelivery delay passes. With the default 30-second lease, the next attempt starts about 30 seconds after the interruption.

One interruption is shorter. When the call to an instance fails with an error that Cloudflare marks as transient, such as `Network connection lost`, the object stops that instance and releases the lease, and the next attempt starts after the redelivery delay of one second. In a test of 11,342 executions on a deployed Worker, one delivery lost its connection this way, and no execution finished more than 4.6 seconds late.

For example, in a test on a deployed Worker, `wrangler deploy` finished 28 seconds into a step:

- **Steps of 60 and 180 seconds** finished on the old code, on their first attempt.
- **Steps of 600 seconds** stopped about 295 seconds after the deploy. They ran again from the start on the new code, 31 seconds later. Each of those step functions started twice and finished once.

No request to the object failed during the deploy. These times come from one test, so do not rely on the old code running for a fixed time after a deploy.

### A forced cancellation stops a step that is waiting

A delivery can also lose its lease while the object keeps running, for example after a forced cancellation. On Bun or Node the dispatcher kills the worker process. On Cloudflare the object aborts the instance that runs the delivery, which stops a step function at the `await` it is waiting on.

For example, `sdk.cancel(id, { force: true })` during a `step.run("charge", …)` that is waiting for a `fetch` ends the execution `cancelled`, and the code after the `await` never runs. The payment service may still process the request that was already sent.

In a test on a deployed Worker, a step that waited 40 seconds was cancelled after one second. Its step function started once and never finished. With [deliveries in the object](#running-deliveries-in-the-object), the same step function ran to its end.

Two limits apply:

- **The object aborts the instance at the next lease renewal.** The execution is `cancelled` at once, and the step can carry on for up to a third of the lease duration: 10 seconds with the default lease.
- **A step in a synchronous loop is not waiting**, so the abort reaches it only when the loop ends or the delivery exceeds the CPU limit.

## CPU-bound steps

A step is CPU-bound when it computes for a long time without awaiting I/O, for example when it parses a large file in a loop. Time that a step spends waiting for `fetch`, a timer or another service is not CPU time. So a step that waits ten minutes for an API is not CPU-bound.

Cloudflare gives each delivery 30 seconds of CPU time by default. The limit counts every step function the delivery runs, one after another, until the workflow ends or waits for a delay, a store, a join or an effect.

**A delivery that passes the limit affects only itself.** Cloudflare resets the instance that runs it. The object logs the reset and leaves the delivery's lease to expire, and the next delivery runs the interrupted step again:

```text
Worker exited without replying; the lease will expire and the execution will be redelivered
  WorkerExitedError: The Durable Object that ran the delivery stopped before replying:
  Durable Object exceeded its CPU time limit and was reset.
```

What to do depends on the step:

- **Several steps that together pass the limit** need no change. The steps that finished are not run again, so each delivery carries on from the last finished step. In a test on a deployed Worker, six steps of about 10 seconds of CPU time each completed in 89 seconds with one reset, and only the step that the reset interrupted ran twice.
- **One step that needs more than 30 seconds** needs a higher limit. The maximum is five minutes:

  ```jsonc [wrangler.jsonc]
  {
    "limits": { "cpu_ms": 300000 }
  }
  ```

  With this limit, steps that spun for 53 and 279 seconds finished on their first attempt in tests on a deployed Worker. None of the requests sent to the object in that time was slow or failed.
- **One step that needs more than five minutes** cannot run in a Worker. Run the computation in another service, and call it from the step.
- **A step that passes the limit on every attempt** is delivered again until `maxExpiredDeliveries` ends its execution. See [Limiting redelivery](#limiting-redelivery).

## Limiting redelivery

By default the object delivers an execution again every time its lease expires. The delay before each new delivery doubles from one second up to five minutes. After a deploy or a restart this is what recovers the execution, because the next delivery finishes the step.

The default does not help a step that passes the CPU limit on every attempt. That step is delivered again about every five minutes for as long as the execution exists.

`maxExpiredDeliveries` sets how many consecutive deliveries of one execution may end with an expired lease:

```ts [src/worker.ts]
export class Workflows extends WorkflowObject({ router, maxExpiredDeliveries: 10 }) {}
```

The claim that would reach the limit records the execution `failed` with a `DeliveryLimitError`, and no workflow code runs. A completed step or a suspension resets the count, so only an execution that makes no progress reaches the limit.

**Choose a limit with deploys in mind.** A CPU reset adds one to the count of the execution whose delivery was reset, and to no other. A deploy or a restart of the object adds one to the count of every execution that has a step running at that moment. An execution with one long step can meet several deploys during that step, so set the limit above the number of deploys and restarts that your longest step is likely to meet.

## Capacity

One object holds every execution in one database, and claims every delivery:

| Limit | Value |
| :--- | :--- |
| Deliveries at once | `concurrency`, 10 by default. Each one runs in its own instance |
| Throughput | A three-minute test on a deployed Worker kept up with 50 triggers per second: executions finished a median of 0.4 seconds and at most 4.6 seconds behind schedule. At 100 per second it fell behind, with a median of 13 seconds. With [deliveries in the object](#running-deliveries-in-the-object), a two-minute test kept up with 100 per second, a median of 25 ms behind |
| Storage | Cloudflare limits a SQLite-backed object to 10 GB. Finished executions stay in the database. A one-hour test stored about 1.3 KB per execution |

For more throughput or storage, give each tenant its own object with `getByName(tenantId)`. Each object has its own instances for deliveries. Executions in different objects cannot join each other or share stores.
