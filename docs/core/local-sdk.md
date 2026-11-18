# Local SDK

`createLocalSdk<Router>(runtime, options?)` creates an SDK that reads and writes execution records directly against a runtime. Use it in API servers, CLI commands, or background jobs that run in the same process as the dispatcher or share the same SQLite database file.

The local SDK only admits and updates execution records – it does not run workflow code. A running `ExecutionDispatcher` attached to the same runtime must be active for admitted executions to progress.

## Creating and using the SDK

Pass your workflow `Router` type as a type parameter to `createLocalSdk` so TypeScript checks `workflowId`, `params`, and `triggerAndWait` return types without importing workflow code into every caller:

::code-group

```ts [src/durable/client.ts]
import pino from "pino";
import { ExecutionDispatcher, createLocalSdk } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import type { Router } from "../workflows/router";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

export const invoker = createWorkflowInvoker({
  workerPath: new URL("./worker.ts", import.meta.url).href,
  logger,
});

export const dispatcher = new ExecutionDispatcher(runtime, {
  invoker,
  logger,
});

export const sdk = createLocalSdk<Router>(runtime, { invoker });
```

```ts [src/api/exports.ts]
import { sdk } from "../durable/client";

export async function startExport(accountId: string, month: string) {
  const { executionId } = await sdk.trigger({
    workflowId: "export-report",
    executionId: `export:${accountId}:${month}`,
    idempotent: true,
    params: { accountId, month },
  });

  return executionId;
}

export async function getExport(executionId: string) {
  const execution = await sdk.poll(executionId);
  if (!execution) return { status: "not-found" };

  if (execution.status === "completed") {
    const { url } = execution.result as { url: string };
    return { status: "ready", url };
  }

  return { status: execution.status };
}

export async function runExportNow(accountId: string, month: string) {
  return sdk.triggerAndWait({
    workflowId: "export-report",
    params: { accountId, month },
  });
}

export async function cancelExport(executionId: string) {
  return sdk.cancel(executionId, { reason: "cancelled by user" });
}
```

::

## Options

| Option | Behavior |
| :--- | :--- |
| `invoker` | Wakes in-process waits immediately when the local dispatcher finishes an execution |
| `pollInterval` | Sets how often `wait` reads the execution record in milliseconds |

`pollInterval` defaults to `25` milliseconds and throws `RangeError` if it is not a finite number greater than zero.

### In-process invoker fast path

When an `ExecutionDispatcher` runs in the same process, passing `{ invoker }` connects `wait` and `triggerAndWait` to the invoker's completion notifications so they read the execution record as soon as an outcome is written instead of waiting for the next `pollInterval` tick.

Passing `invoker` is strictly an optimization. Outcomes always come from the durable execution record, so omitting `invoker` – or waiting on an execution delivered by another process – produces the same result at the next `pollInterval` read.

## Methods

The local SDK implements `ExecutionObserver & ExecutionControl` along with `trigger` and `triggerAndWait`:

| Method | Role |
| :--- | :--- |
| `trigger` | Admits an execution and returns `TriggerAck` |
| `triggerAndWait` | Admits an execution and resolves with its typed return value |
| `poll` | Reads the current `ExecutionView` once |
| `wait` | Waits for a terminal outcome, timeout, or `AbortSignal` |
| `list` | Pages through `ExecutionView` records newest first |
| `cancel` | Records a graceful or forced cancellation request |
