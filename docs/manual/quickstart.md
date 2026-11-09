# Quick start

This guide builds an application that runs one workflow on Bun. The workflow builds a greeting, waits one second, then adds punctuation. For Node, see [Node](./node.md).

The application has three files:

| File | Contains |
| --- | --- |
| `shared.ts` | The workflow, and the storage that both processes open |
| `worker.ts` | The worker process, which runs workflow code |
| `app.ts` | The application, which starts the workflow and waits for its result |

## Install

```sh
@pm.add("@notation/durable pino")
```

## 1. Define the workflow

```ts [shared.ts]
import { workflow, createWorkflowRouter } from "@notation/durable";
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

export const greet = workflow<{ name: string }, string>(async function* (step, event) {
  const greeting = yield* step.run("greeting", () => `Hello, ${event.params.name}`);
  yield* step.delay("pause", 1000);
  return yield* step.run("punctuate", () => `${greeting}!`);
});

export const router = createWorkflowRouter({ greet });
export type Router = typeof router;

export const runtime = new SqliteRuntime(createSqliteDb({ path: "./.db/local.sqlite" }));
```

The two type arguments of `workflow` are the workflow's params and its result.

`createWorkflowRouter` gives each workflow an ID. Here the ID of `greet` is `"greet"`. The `Router` type lets TypeScript check the ID and params wherever the workflow is started.

`SqliteRuntime` is Durable's storage, and it keeps every record in one SQLite file. `createSqliteDb` opens the file, creating it and its folder if needed, and `SqliteRuntime` creates its tables.

## 2. Create the worker

```ts [worker.ts]
import pino from "pino";
import { WorkflowRunner } from "@notation/durable";
import { createWorkflowWorker } from "@notation/durable/worker";
import { router, runtime } from "./shared";

const logger = pino();
const runner = new WorkflowRunner(runtime, { router, logger });

createWorkflowWorker(runner, logger).listen();
```

The worker runs workflow code. It opens its own `SqliteRuntime` over the same file as the application, and waits for the application to send it work.

## 3. Start the workflow

```ts [app.ts]
import pino from "pino";
import { ExecutionDispatcher, createLocalSdk } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import { runtime, type Router } from "./shared";

const logger = pino();
const workerPath = new URL("./worker.ts", import.meta.url).href;
const invoker = createWorkflowInvoker({ workerPath, logger });

const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger });
dispatcher.start();

const sdk = createLocalSdk<Router>(runtime, { invoker });
const result = await sdk.triggerAndWait({
  workflowId: "greet",
  params: { name: "World" },
});

console.log(result); // "Hello, World!"
await dispatcher.stop();
```

The application uses three objects:

| Object | Role |
| --- | --- |
| `invoker` | Starts a worker process from `worker.ts` each time the workflow needs to run |
| `dispatcher` | Finds workflows that are due to run, and hands each one to the invoker |
| `sdk` | Starts workflows and reads their results |

Each start of a workflow is an execution. `sdk.triggerAndWait` writes a record for a new execution of `greet`, then waits until the record holds a result. The SDK never runs workflow code itself, so a dispatcher must be running.

## 4. Run it

```sh
bun app.ts
```

The application prints `Hello, World!` after about one second. This is what happens:

1. The dispatcher finds the new execution and starts a worker.
2. The worker runs `greeting`, stores its result, and stops at `pause`. The invoker then stops the worker process.
3. One second later, the dispatcher starts a new worker.
4. The new worker replays `greeting` from its stored result, passes `pause`, runs `punctuate`, and stores the workflow's result.
5. `triggerAndWait` reads the result and returns it.

The records stay in `.db/local.sqlite`.
