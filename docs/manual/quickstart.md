# Quick start

This guide builds a small application on Bun. When a user signs up, the application sends a welcome email, waits, and then sends a second email with tips. For Node, see [Node](./node.md).

## Install

```sh
@pm.add("@notation/durable pino")
```

## Project layout

The application code knows nothing about how workflows are stored or run. That setup lives in its own folder:

```
src/
  email.ts              ← application code
  workflows/
    onboarding.ts       ← application code
    router.ts           ← application code
  durable/
    runtime.ts          ← Durable setup
    worker.ts           ← Durable setup
    client.ts           ← Durable setup
  main.ts               ← application entry point
```

## 1. Write the workflow

`email.ts` stands in for a real email service:

```ts [src/email.ts]
export async function sendEmail(to: string, subject: string) {
  console.log(`Sending "${subject}" to ${to}`);
}
```

The workflow sends one email, waits, and sends another:

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

The type arguments of `workflow` are the workflow's params and its result. A real application would wait days rather than three seconds.

The router gives each workflow an ID:

```ts [src/workflows/router.ts]
import { createWorkflowRouter } from "@notation/durable";
import { onboarding } from "./onboarding";

export const router = createWorkflowRouter({
  onboarding,
});

export type Router = typeof router;
```

Here the ID of `onboarding` is `"onboarding"`. The `Router` type lets TypeScript check the ID and the params wherever the application starts a workflow.

## 2. Set up Durable

The runtime stores Durable's records in a SQLite file:

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

`createSqliteDb` creates the file if it does not exist, and `SqliteRuntime` creates its tables.

The worker is a separate process that runs workflow code:

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

The client is what the application imports:

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

| Object | Role |
| --- | --- |
| `invoker` | Starts a worker process from `worker.ts` each time a workflow needs to run |
| `dispatcher` | Finds workflows that are due to run, and hands each one to the invoker |
| `sdk` | Starts workflows and reads their results |

## 3. Start the workflow

```ts [src/main.ts]
import { dispatcher, sdk } from "./durable/client";

dispatcher.start();

await sdk.triggerAndWait({
  workflowId: "onboarding",
  params: { email: "ada@example.com" },
});

console.log("Onboarding finished");

await dispatcher.stop();
```

Each start of a workflow is an execution. `triggerAndWait` records a new execution of `onboarding` and waits until it finishes. The SDK does not run workflow code itself. The dispatcher must be running for the execution to make progress.

## 4. Run it

```sh
bun src/main.ts
```

The output is:

```
Sending "Welcome to Acme" to ada@example.com
Sending "Three tips for your first week" to ada@example.com
Onboarding finished
```

The second email arrives three seconds after the first. During those three seconds, no worker process is running. The dispatcher starts a new worker when the delay is over. That worker skips `send-welcome`, because its result is already recorded, and runs `send-tips`.

The records stay in `.db/durable.sqlite`.
