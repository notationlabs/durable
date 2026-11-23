# Cloudflare Workers

Notation Durable runs on Cloudflare inside one Durable Object. The object's SQLite storage holds the runtime, the object's alarm takes the place of the dispatcher's poll loop, and workflows run in the object's own isolate. Workflow and router files are the same as on Bun and Node.

## How the pieces map

A Bun or Node deployment has a server process, worker processes and a SQLite file. On Cloudflare one Durable Object does all three jobs:

| Bun or Node | Cloudflare |
| :--- | :--- |
| SQLite file opened with `createSqliteDb` | The object's SQLite storage (`ctx.storage.sql`) |
| `ExecutionDispatcher` polling every 10 ms | The object's alarm, set to the next time an execution is due |
| Worker processes forked by `createWorkflowInvoker` | Deliveries run in the object's isolate |
| `createRoutes` mounted in `Bun.serve` or `node:http` | The object's `fetch` handler |

## Setup

`createWorkflowObject` from `@notation/durable/cloudflare` builds the runtime, the dispatcher and the HTTP routes from the object's `ctx`. The Durable Object class forwards `fetch` and `alarm` to it:

::code-group

```ts [src/worker.ts]
import { DurableObject } from "cloudflare:workers";
import { createWorkflowObject } from "@notation/durable/cloudflare";
import { router } from "./workflows/router";

export class Workflows extends DurableObject {
  durable = createWorkflowObject(this.ctx, { router, basePath: "/durable" });

  fetch(request: Request) {
    return this.durable.fetch(request);
  }

  alarm() {
    return this.durable.alarm();
  }
}

export default {
  fetch(request: Request, env: Env) {
    // Authenticate the request here.
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

Two settings in `wrangler.jsonc` are required:

- **`nodejs_compat`** – The SDK imports `node:events` and `node:crypto`.
- **`new_sqlite_classes`** – The class must be SQLite-backed. A key-value-backed Durable Object has no `ctx.storage.sql`.

**The `alarm` handler is required.** Without it nothing resumes an execution after a `step.delay`, a retry or an eviction.

## Triggering executions

The object serves the same routes as [`createRoutes`](./http-server.md) under `basePath`: `/trigger`, `/poll`, `/wait`, `/list`, `/events` and `/cancel`. The [HTTP SDK](./http-sdk.md) calls them over the public URL, or from another Worker through the object's stub by passing `fetch`:

```ts [src/signup.ts]
import { createHttpSdkFactory } from "@notation/durable/client";
import type { Router } from "./workflows/router";

const createSdk = createHttpSdkFactory<Router>();

export async function signup(env: Env, email: string) {
  const object = env.WORKFLOWS.get(env.WORKFLOWS.idFromName("default"));
  const sdk = createSdk({
    url: "http://workflows/durable", // only the path is used
    fetch: (input, init) => object.fetch(input, init),
  });

  return sdk.trigger({ workflowId: "onboarding", params: { email } });
}
```

Code inside the Durable Object class can also call the [local SDK](./local-sdk.md) on `this.durable.runtime`. Any write to the runtime, from a route or from the local SDK, sets the alarm and starts deliveries, so there is nothing else to call.

## When deliveries run

The object does not poll. It claims executions at three moments:

1. **After a write.** A trigger, a cancel, a store commit or an effect settlement commits to SQLite. In the same turn the object sets its alarm to the earliest time any execution is due, and claims what is due now.
2. **While deliveries are in flight.** The object claims again when a delivery settles and when the next due time arrives.
3. **When the alarm fires.** Once no delivery is in flight the object stops. The alarm starts it again at the next due time, such as the end of a `step.delay`.

If Cloudflare evicts the object during a delivery, the delivery's lease expires and the alarm delivers the execution again after the [redelivery delay](../internals/leases-and-dispatcher.md). The step that was running runs again, as it does after a worker crash on Bun or Node.

A new instance of the object reads the alarm when it is constructed. If an execution is due and no alarm is set, which happens when an alarm write failed before an eviction, the instance sets the alarm.

If claiming fails inside `alarm`, the object logs the error and sets the alarm to run again. The first retry is 0.5 seconds later, and each further failure doubles the wait, up to 60 seconds. `alarm` rejects only if it cannot set the alarm. Cloudflare runs a rejected alarm handler again six times and then drops the alarm.

## Limiting redelivery

By default the object delivers an execution again every time its lease expires, with a delay that doubles from one second up to five minutes. **A step that exceeds the Worker's CPU limit is therefore delivered again without end**: Cloudflare resets the object, the lease expires, and the alarm delivers the step again. Each delivery uses CPU time up to the limit.

`maxExpiredDeliveries` sets how many consecutive deliveries of one execution may end with an expired lease:

```ts [src/worker.ts]
export class Workflows extends DurableObject {
  durable = createWorkflowObject(this.ctx, { router, maxExpiredDeliveries: 10 });
  // ...
}
```

The claim that would reach the limit records the execution `failed` with a `DeliveryLimitError`, and no workflow code runs. A completed step or a suspension resets the count, so only an execution that makes no progress reaches the limit.

Every execution in the object shares one isolate, so a reset also expires the leases of the other deliveries in flight, and each of those executions gets one count. An execution with a long step that keeps overlapping with the failing one can reach the limit too. Choose a limit well above the number of deploys and evictions one step is likely to meet.

## Differences from Bun and Node

- **No process isolation.** A delivery whose lease is lost cannot be killed. Its abort signal stops it before the next step, and its lease token rejects every later write.
- **One object, one thread.** Every execution in the object shares one isolate and one database. For more throughput, route by tenant with `idFromName(tenantId)`; executions in different objects cannot join each other or share stores.
- **Long deliveries.** The object stays in memory while a step is pending, and Cloudflare bills that time. `alarm` returns after five minutes if deliveries are still in flight, because Cloudflare stops an alarm handler after 15 minutes; the deliveries carry on.

## Keyless steps and deploys

**Give every step an explicit key in a workflow that can be suspended across a deploy.** A step without a key is keyed by the file, line and column of its call site, and in a Worker that is a position in the bundled output. A deploy that moves the position changes the key. An execution that was suspended before the deploy then finds no record under the new key:

| Step | After a deploy that moves its call site |
| :--- | :--- |
| Keyed `step.run("charge", …)` | Replays its recorded result |
| Keyless `step.run(…)` | Runs again |
| Keyless `step.delay(300_000)` | Starts a new delay of the full length |

The execution does not fail and logs no error. `wrangler dev` has the same effect on every restart, because the bundle path in the call site includes a temporary directory.
