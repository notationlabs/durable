# Cloudflare Workers

Notation Durable runs on Cloudflare inside one Durable Object. The object's SQLite storage holds the runtime, the object's alarm takes the place of the dispatcher's poll loop, and every delivery runs in the object's own isolate. Workflow and router files are the same as on Bun and Node.

**Read [When a step runs again](#when-a-step-runs-again) before you deploy.** On Cloudflare, a deploy, an eviction or one CPU-bound step stops every step that is running in the object, and each of those steps later runs again from the start.

## How the pieces map

A Bun or Node deployment has a server process, worker processes and a SQLite file. On Cloudflare one Durable Object does all three jobs:

| Bun or Node | Cloudflare |
| :--- | :--- |
| SQLite file opened with `createSqliteDb` | The object's SQLite storage (`ctx.storage.sql`) |
| `ExecutionDispatcher` polling every 10 ms | The object's alarm, set to the next time an execution is due |
| Worker processes forked by `createWorkflowInvoker` | Deliveries run in the object's isolate |
| `createRoutes` mounted in `Bun.serve` or `node:http` | The object's `fetch` handler |

The last two rows matter most. On Bun or Node each delivery runs in a worker process, and the dispatcher can kill that process. On Cloudflare every delivery shares one isolate with the routes, so anything that stops the isolate stops all of them at once.

## Setup

`createWorkflowObject` from `@notation/durable/cloudflare` builds the runtime, the dispatcher and the HTTP routes from the object's `ctx`. The Durable Object class forwards `fetch` and `alarm` to it:

::code-group

```ts [src/worker.ts]
import { DurableObject } from "cloudflare:workers";
import { createWorkflowObject } from "@notation/durable/cloudflare";
import { ownExecutionsOnly, requireUser } from "./auth";
import { router } from "./workflows/router";

export class Workflows extends DurableObject {
  durable = createWorkflowObject(this.ctx, {
    router,
    basePath: "/durable",
    middleware: [requireUser], // runs on /trigger
    observationMiddleware: [ownExecutionsOnly], // runs on /poll, /wait, /list, /events and /cancel
    maxExpiredDeliveries: 10,
  });

  fetch(request: Request) {
    return this.durable.fetch(request);
  }

  alarm() {
    return this.durable.alarm();
  }
}

export default {
  fetch(request: Request, env: Env) {
    const object = env.WORKFLOWS.get(env.WORKFLOWS.idFromName("default"));
    return object.fetch(request);
  },
};
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

Each of these settings is required:

- **`alarm`** – Without the `alarm` handler nothing resumes an execution after a `step.delay`, a retry or an eviction.
- **`middleware` and `observationMiddleware`** – The Worker forwards every request to the object. Without middleware, anyone who can reach the Worker's URL can start workflows, read every execution's params and results with `/list`, and cancel executions. `requireUser` and `ownExecutionsOnly` are the functions from [HTTP Server](./http-server.md#auth-and-execution-privacy).
- **`maxExpiredDeliveries`** – Without a limit, a step that exceeds the CPU limit runs again forever, and each attempt stops every other delivery in the object. See [Limiting redelivery](#limiting-redelivery).
- **`nodejs_compat`** – The SDK imports `node:events` and `node:crypto`.
- **`new_sqlite_classes`** – The class must be SQLite-backed. A key-value-backed Durable Object has no `ctx.storage.sql`.

## Triggering executions

The object serves the same routes as [`createRoutes`](./http-server.md) under `basePath`: `/trigger`, `/poll`, `/wait`, `/list`, `/events` and `/cancel`. The [HTTP SDK](./http-sdk.md) calls them over the public URL, or from another Worker through the object's stub by passing `fetch`:

```ts [src/signup.ts]
import { createHttpSdkFactory } from "@notation/durable/client";
import type { Router } from "./workflows/router";

const createSdk = createHttpSdkFactory<Router>();

export async function signup(env: Env, email: string, sessionToken: string) {
  const object = env.WORKFLOWS.get(env.WORKFLOWS.idFromName("default"));
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

A step that is running when the object stops does not finish. Its lease expires, and the alarm delivers the execution again. The new delivery replays the steps that finished and **runs the interrupted step from the start**.

This is the same at-least-once behaviour as a worker crash on Bun or Node. On Cloudflare it happens more often, and it happens to every running step in the object at once:

| Event | Effect on running steps |
| :--- | :--- |
| Eviction or reset by Cloudflare | Every running step in the object stops |
| A deploy | The object keeps running the old code for a while, then stops. Every step still running at that point stops |
| One delivery exceeds the CPU limit | Cloudflare resets the object. Every running step in the object stops, not only the CPU-bound one |

The interrupted step starts again after its lease expires and the redelivery delay passes. With the default 30-second lease, the next attempt starts about 30 seconds after the object stopped.

In tests on a deployed Worker:

- **Deploy** – `wrangler deploy` ran 18 seconds into a step. Steps of 60 and 180 seconds finished on the old code, on their first attempt. Steps of 600 seconds stopped about 300 seconds after the deploy, and ran again from the start on the new code 31 seconds after their last lease renewal. Each of those step bodies started twice and finished once.
- **CPU limit** – A step that spun for about 40 seconds was reset at 30 seconds of CPU time, with the log message "Durable Object exceeded its CPU time limit and was reset." Its attempts started at 0.7, 35.1 and 67.1 seconds. Requests sent to the object during a spin returned HTTP 500 or 503.

**Write every `step.run` body so that running it twice is safe.** Use [`step.effect`](./external-effects.md) for an operation that must not happen twice, such as charging a card.

### A lost lease does not stop a step

A delivery can also lose its lease while the object keeps running, for example after a forced cancellation. On Bun or Node the dispatcher kills the worker process. On Cloudflare nothing can stop the `await` inside a step body, so the body runs to its end. Its abort signal stops the workflow before the next step, and its lease token makes the runtime reject every later write.

For example, `sdk.cancel(id, { force: true })` during a `step.run("charge", …)` ends the execution `cancelled`, but the charge request still completes.

## CPU-bound steps

A Durable Object has a CPU limit of 30 seconds by default. The isolate runs one thing at a time, so a step that computes without awaiting I/O also blocks every other delivery and every request to the object until it ends.

Raise the limit in `wrangler.jsonc` when a step needs more CPU time. The maximum is five minutes:

```jsonc [wrangler.jsonc]
{
  "limits": { "cpu_ms": 300000 }
}
```

With this limit, spins of up to 56 seconds finished on their first attempt in tests. Work that needs more CPU time, or that should not block other executions, belongs in a separate Worker or service that a step calls.

## Limiting redelivery

By default the object delivers an execution again every time its lease expires, with a delay that doubles from one second up to five minutes. So a step that always exceeds the CPU limit is delivered again forever, about every five minutes once the delay reaches its maximum. Each attempt uses up to the full CPU limit and stops every other running step in the object.

`maxExpiredDeliveries` sets how many consecutive deliveries of one execution may end with an expired lease:

```ts [src/worker.ts]
export class Workflows extends DurableObject {
  durable = createWorkflowObject(this.ctx, { router, maxExpiredDeliveries: 10 });
  // ...
}
```

The claim that would reach the limit records the execution `failed` with a `DeliveryLimitError`, and no workflow code runs. A completed step or a suspension resets the count, so only an execution that makes no progress reaches the limit.

A reset caused by one execution also expires the leases of the other executions running in the object, and adds one to each of their counts. Deploys and evictions add to the counts in the same way. So an execution with a long step can reach the limit without any fault of its own. Choose a limit well above the number of deploys, evictions and resets that one step is likely to meet.

## Capacity

One object runs every execution in one isolate and one database:

| Limit | Value |
| :--- | :--- |
| Deliveries at once | `concurrency`, 10 by default |
| Throughput | A test on a deployed Worker kept up with 50 triggers per second, and fell behind at 100 per second with a median lag of 37 seconds |
| Storage | Cloudflare limits a SQLite-backed object to 10 GB. Finished executions stay in the database. A one-hour test stored about 1.3 KB per execution |

For more throughput or storage, give each tenant its own object with `idFromName(tenantId)`. Executions in different objects cannot join each other or share stores.
