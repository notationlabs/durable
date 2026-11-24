# Cloudflare Workers

Notation Durable runs on Cloudflare inside one Durable Object. The object's SQLite storage holds the runtime, the object's alarm takes the place of the dispatcher's poll loop, and every delivery runs in the object's own isolate. Workflow and router files are the same as on Bun and Node.

Steps follow the same rule as on Bun and Node: a step that is interrupted runs again from the start. There is one difference to plan for. Every delivery shares the object's isolate, so a step that computes for longer than the CPU limit also interrupts the other steps running in the object. [CPU-bound steps](#cpu-bound-steps) shows how to recognise such a step and what to do about it.

## How the pieces map

A Bun or Node deployment has a server process, worker processes and a SQLite file. On Cloudflare one Durable Object does all three jobs:

| Bun or Node | Cloudflare |
| :--- | :--- |
| SQLite file opened with `createSqliteDb` | The object's SQLite storage (`ctx.storage.sql`) |
| `ExecutionDispatcher` polling every 10 ms | The object's alarm, set to the next time an execution is due |
| Worker processes forked by `createWorkflowInvoker` | Deliveries run in the object's isolate |
| `createRoutes` mounted in `Bun.serve` or `node:http` | The object's `fetch` handler |

The third row is the main difference. On Bun or Node each delivery runs in its own worker process. On Cloudflare every delivery runs in the object's isolate, next to the routes. Steps that await I/O take turns in the isolate, so most workflows run the same way on both. A step that computes for a long time without awaiting does not take turns, and [CPU-bound steps](#cpu-bound-steps) covers that case.

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
- **`maxExpiredDeliveries`** – This ends an execution as `failed` when its deliveries keep being interrupted, instead of delivering it again forever. It is optional, and worth setting on Cloudflare. See [Limiting redelivery](#limiting-redelivery).
- **`nodejs_compat`** – The SDK imports `node:events` and `node:crypto`.
- **`new_sqlite_classes`** – The class must be SQLite-backed. A key-value-backed Durable Object has no `ctx.storage.sql`.

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

Code inside the Durable Object class can also call the [local SDK](./local-sdk.md) on `this.durable.runtime`. The local SDK writes to the runtime directly and runs no middleware. Any write to the runtime, from a route or from the local SDK, sets the alarm and starts deliveries, so there is nothing else to call.

## When deliveries run

The object does not poll. It claims executions at three moments:

1. **After a write.** A trigger, a cancel, a store commit or an effect settlement commits to SQLite. In the same turn the object sets its alarm to the earliest time any execution is due, and claims what is due now.
2. **While deliveries are in flight.** The object claims again when a delivery settles and when the next due time arrives.
3. **When the alarm fires.** Once no delivery is in flight the object stops. The alarm starts it again at the next due time, such as the end of a `step.delay`.

A delivery can run longer than one alarm handler. Cloudflare stops an alarm handler after 15 minutes, so `alarm` returns after five minutes and the delivery carries on. In a test on a deployed Worker, a 17-minute step finished on its first attempt with no requests to the object.

## When a step runs again

`step.run` runs a step body at least once on every host. If the process stops while a step is running, the step does not finish. Its lease expires, and the next delivery replays the steps that finished and **runs the interrupted step from the start**. [Running a Server](./running-a-server.md#graceful-shutdown) describes this for a server crash on Bun and Node.

So the practice is the same on every host: **write every `step.run` body so that running it twice is safe.** Use [`step.effect`](./external-effects.md) for an operation that must not happen twice, such as charging a card.

On Cloudflare the object stops in three situations. Each one interrupts a different set of steps:

| Event | Steps that run again |
| :--- | :--- |
| A deploy | Steps that are still running when the old code stops. The object keeps running the old code for a while first, so short steps finish |
| Cloudflare restarts the object | Steps that are running at that moment. The object's alarm keeps it in memory while a delivery is in flight: in a test, a 17-minute step finished on its first attempt |
| A step exceeds the CPU limit | Every step that is running in the object, not only the CPU-bound one. See [CPU-bound steps](#cpu-bound-steps) |

An interrupted step starts again after its lease expires and the redelivery delay passes. With the default 30-second lease, the next attempt starts about 30 seconds after the object stopped.

For example, in a test on a deployed Worker, `wrangler deploy` ran 18 seconds into a step:

- **Steps of 60 and 180 seconds** finished on the old code, on their first attempt.
- **Steps of 600 seconds** stopped about 300 seconds after the deploy. They ran again from the start on the new code, 31 seconds after their last lease renewal. Each of those step bodies started twice and finished once.

These times come from one test, so do not rely on the old code running for a fixed time after a deploy.

### A lost lease does not stop a step

A delivery can also lose its lease while the object keeps running, for example after a forced cancellation. On Bun or Node the dispatcher kills the worker process. On Cloudflare nothing can stop the `await` inside a step body, so the body runs to its end. Its abort signal stops the workflow before the next step, and its lease token makes the runtime reject every later write.

For example, `sdk.cancel(id, { force: true })` during a `step.run("charge", …)` ends the execution `cancelled`, but the charge request still completes.

## CPU-bound steps

A step is CPU-bound when it computes for a long time without awaiting I/O, for example when it parses a large file in a loop. Time that a step spends waiting for `fetch`, a timer or another service is not CPU time. So a step that waits ten minutes for an API is not CPU-bound, and it lets other deliveries run while it waits.

A CPU-bound step affects the rest of the object in two ways:

- **Nothing else runs while it computes.** The isolate runs one thing at a time, so the other deliveries and every request to the object wait until the step ends or awaits.
- **Cloudflare resets the object when the step passes the CPU limit.** The limit is 30 seconds by default. The reset interrupts every step that is running in the object, and each of them [runs again](#when-a-step-runs-again). The CPU-bound step runs again too, and passes the limit again unless something changes.

A reset writes this message to the Worker's logs: "Durable Object exceeded its CPU time limit and was reset." In a test on a deployed Worker, a step that spun for about 40 seconds was reset at 30 seconds of CPU time. Its attempts started at 0.7, 35.1 and 67.1 seconds, and requests sent to the object during a spin returned HTTP 500 or 503.

Three things help, the most effective first:

1. **Move the computation out of the object.** Put it in a separate Worker or service, and call that from the step. The step then awaits I/O, so it does not block the other deliveries and does not use the object's CPU time.
2. **Raise the CPU limit** when a step needs a little more than 30 seconds. The maximum is five minutes. The step still blocks the other deliveries while it computes.
3. **Set [`maxExpiredDeliveries`](#limiting-redelivery)**, so that a step that passes the limit on every attempt ends as `failed` and stops resetting the object.

The CPU limit is set in `wrangler.jsonc`:

```jsonc [wrangler.jsonc]
{
  "limits": { "cpu_ms": 300000 }
}
```

With this limit, spins of up to 56 seconds finished on their first attempt in tests.

## Limiting redelivery

By default the object delivers an execution again every time its lease expires. The delay before each new delivery doubles from one second up to five minutes. After a deploy or a restart this is what recovers the execution, because the next delivery finishes the step.

The default does not help a step that passes the CPU limit on every attempt. That step is delivered again about every five minutes, and each attempt resets the object.

`maxExpiredDeliveries` sets how many consecutive deliveries of one execution may end with an expired lease:

```ts [src/worker.ts]
export class Workflows extends WorkflowObject({ router, maxExpiredDeliveries: 10 }) {}
```

The claim that would reach the limit records the execution `failed` with a `DeliveryLimitError`, and no workflow code runs. A completed step or a suspension resets the count, so only an execution that makes no progress reaches the limit.

**Choose a limit with long steps in mind.** A reset, a deploy or a restart adds one to the count of every execution that has a step running at that moment, not only the execution that caused it. An execution with short steps finishes a step between interruptions, and its count returns to zero. An execution with one long step can be interrupted several times during that step, and can reach the limit without any fault of its own. So set the limit well above the number of deploys, restarts and resets that your longest step is likely to meet.

## Capacity

One object runs every execution in one isolate and one database:

| Limit | Value |
| :--- | :--- |
| Deliveries at once | `concurrency`, 10 by default |
| Throughput | A test on a deployed Worker kept up with 50 triggers per second, and fell behind at 100 per second with a median lag of 37 seconds |
| Storage | Cloudflare limits a SQLite-backed object to 10 GB. Finished executions stay in the database. A one-hour test stored about 1.3 KB per execution |

For more throughput or storage, give each tenant its own object with `getByName(tenantId)`. A separate object also has its own isolate, so a CPU reset in one tenant's object does not interrupt another tenant's steps. Executions in different objects cannot join each other or share stores.
