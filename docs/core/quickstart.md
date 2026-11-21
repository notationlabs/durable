# Quickstart

This guide installs `@notation/durable`, wires up a SQLite runtime, worker, and dispatcher on Bun, and runs an onboarding workflow that sends two emails three seconds apart.

## Install

Install `@notation/durable` and `pino`:

```sh [Terminal]
@pm.add("@notation/durable pino")
```

Notation Durable logs through a `pino` logger created and passed by your application, so your project installs `pino` directly.

| Runtime | Minimum version |
| :--- | :--- |
| Bun | 1.3 |
| Node | 22.13 |

Durable uses each runtime's built-in SQLite module – `bun:sqlite` on Bun and `node:sqlite` on Node – so no separate SQLite driver package is needed. Node 22.13 through 22.17 require `--experimental-strip-types` to execute TypeScript files directly, while Bun 1.3 and Node 22.18+ run them without flags. See [Bun & Node](./runtimes.md) for Node import and TypeScript configuration differences.

## Project setup

Create the email helper, workflow, router, Durable runtime wiring, and entry point across these files:

::code-group

```ts [src/workflows/onboarding.ts]
import { workflow } from "@notation/durable";
import { sendEmail } from "../email";

type Params = {
  email: string;
};

export const onboarding = workflow<Params, void>(async function* (step, event) {
  const { email } = event.params;

  yield* step.run("send-welcome", async () => {
    await sendEmail(email, "Welcome to Acme");
  });

  yield* step.delay("wait", 3_000);

  yield* step.run("send-tips", async () => {
    await sendEmail(email, "Three tips for your first week");
  });
});
```

```ts [src/workflows/router.ts]
import { createWorkflowRouter } from "@notation/durable";
import { onboarding } from "./onboarding";

export const router = createWorkflowRouter({
  onboarding,
});

export type Router = typeof router;
```

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

```ts [src/durable/worker.ts]
import pino from "pino";
import { WorkflowRunner } from "@notation/durable";
import { createWorkflowWorker } from "@notation/durable/worker";
import { router } from "../workflows/router";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

const runner = new WorkflowRunner(runtime, {
  router,
  logger,
});

createWorkflowWorker(runner, logger).listen();
```

```ts [src/durable/client.ts]
import pino from "pino";
import { ExecutionDispatcher, createLocalSdk } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import type { Router } from "../workflows/router";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

const invoker = createWorkflowInvoker({
  workerPath: new URL("./worker.ts", import.meta.url).href,
  logger,
});

export const dispatcher = new ExecutionDispatcher(runtime, {
  invoker,
  logger,
});

export const sdk = createLocalSdk<Router>(runtime, {
  invoker,
});
```

```ts [src/main.ts]
import { dispatcher, sdk } from "./durable/client";

dispatcher.start();

try {
  await sdk.triggerAndWait({
    workflowId: "onboarding",
    params: { email: "ada@example.com" },
  });

  console.log("Onboarding finished");
} finally {
  await dispatcher.stop();
}
```

```ts [src/email.ts]
export async function sendEmail(to: string, subject: string) {
  console.log(`Sending "${subject}" to ${to}`);
}
```

::

The pieces in `src/durable/` each handle one responsibility:

| Component | Role |
| :--- | :--- |
| `runtime` | Opens `./.db/durable.sqlite` and persists all execution records, step outcomes, and stores |
| `router` | Maps string workflow IDs to workflow functions and supplies `Router` types to the SDK |
| `invoker` | Spawns a worker child process from `src/durable/worker.ts` for each delivery |
| `dispatcher` | Polls SQLite for due executions and passes them to the invoker |
| `sdk` | Admits new executions and reads their outcomes |

Notice that `sdk.triggerAndWait` does not run workflow code inside `src/main.ts` – it admits the execution record into SQLite and waits for a terminal outcome while `dispatcher` drives deliveries through the worker.

## Why workers isolate deliveries

`createWorkflowInvoker` runs every delivery in a dedicated child process instead of executing workflow generators inside your main application process:

- **Fault isolation** – An uncaught crash, memory spike, or CPU-heavy step inside a workflow cannot take down your server or corrupt other in-flight deliveries.
- **Clean interruption** – If a delivery stalls or the parent process shuts down, the invoker terminates the worker process directly so step code stops immediately and cannot write stale results.
- **Zero idle processes** – Once a delivery reaches `step.delay` or another wait point, it reports `suspended` and the worker exits. During the three-second delay in `onboarding`, no worker process exists in memory; when the delay elapses, the dispatcher starts a new worker that replays `send-welcome` from its saved record and runs `send-tips`.

## Run the workflow

Execute `src/main.ts` with Bun:

```sh [Terminal]
bun src/main.ts
```

```text [Output]
Sending "Welcome to Acme" to ada@example.com
Sending "Three tips for your first week" to ada@example.com
Onboarding finished
```

The second email prints three seconds after the first. All execution history and step records remain stored in `./.db/durable.sqlite`.
