# @notation/durable/worker

`@notation/durable/worker` isolates workflow execution in child processes. The server process creates a `WorkflowInvoker` with `createWorkflowInvoker`, and the worker script creates a listener with `createWorkflowWorker`.

::code-group

```ts [src/durable/client.ts]
import pino from "pino";
import { ExecutionDispatcher } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

const invoker = createWorkflowInvoker({
  workerPath: new URL("./worker.ts", import.meta.url).href,
  logger,
});

export const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger });
```

```ts [src/durable/worker.ts]
import pino from "pino";
import { WorkflowRunner } from "@notation/durable";
import { createWorkflowWorker } from "@notation/durable/worker";
import { router } from "../workflows/router";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

const runner = new WorkflowRunner(runtime, { router, logger });

createWorkflowWorker(runner, logger).listen();
```

::

## createWorkflowInvoker

`createWorkflowInvoker` returns a `WorkflowInvoker` for `ExecutionDispatcher`:

```ts [@notation/durable/worker]
export function createWorkflowInvoker(params: {
  workerPath: string;
  execPath?: string;
  handshakeTimeout?: number;
  logger: Logger;
}): WorkflowInvoker;
```

| Option | Description |
| :--- | :--- |
| `workerPath` | Worker module path or `file:` URL |
| `logger` | Pino logger instance |
| `execPath` | Executable path used to fork the child process |
| `handshakeTimeout` | Milliseconds to wait for the worker ready message |

`execPath` defaults to the current executable path and `handshakeTimeout` defaults to 5,000 ms. Server and worker must run on matching JavaScript runtimes because IPC uses advanced serialization (see [Bun & Node](../core/runtimes.md) for cross-runtime and `DURABLE_WORKER_PATH` details).

### Delivery lifecycle

Each `deliver` call receives `event` and `{ lease, signal, cancellation? }`:

1. If the abort signal already fired, `deliver` rejects immediately with `InfrastructureError` without forking.
2. Otherwise it forks `workerPath` with inherited standard streams, advanced serialization, and an IPC channel.
3. After the worker signals readiness, the invoker encodes event parameters and context via `encodeExecutionEvent` and sends the delivery payload with lease and cancellation state.
4. When the worker replies with an outcome frame, `deliver` sends `SIGTERM` to the worker and resolves with the decoded `WorkflowRunOutcome`.

### Rejection behavior

| Condition | Rejection |
| :--- | :--- |
| Signal already aborted before fork | Rejects immediately with `InfrastructureError` |
| Signal aborts mid-delivery | Kills worker with `SIGKILL` and rejects with `InfrastructureError` |
| Handshake timeout expires before ready | Kills worker with `SIGTERM` and rejects with `Error` |
| Child process exits before replying | Rejects with `WorkerExitedError` |
| Worker replies with an error frame | Sends `SIGTERM` and rejects with the decoded `Error` |
| Process spawn or payload encoding fails | Rejects with the spawn or encoding `Error` |

`ExecutionDispatcher` leaves the lease to expire when `deliver` rejects with `WorkerExitedError`, because the exited worker's last storage writes may still be completing, and releases the lease on all other rejections.

## createWorkflowWorker

`createWorkflowWorker` creates the IPC listener used in the worker entry point:

```ts [@notation/durable/worker]
export const WORKER_ORPHANED_EXIT_CODE = 75;

export function createWorkflowWorker(
  workflowRunner: WorkflowRunner<any>,
  logger: Logger
): {
  listen(): void;
};
```

Calling `listen()` attaches IPC message and disconnect listeners and sends readiness to the parent process. For each delivery, it decodes the event, populates request context for `getContext()`, runs `workflowRunner.run`, and replies with the serialized outcome or error.

If a completed workflow returns a non-durable value, `listen()` sends a failed outcome carrying `WorkflowValueError` (`INVALID_STEP_VALUE`) at the workflow result step key.

When the IPC channel to the parent process closes on `disconnect`, `listen()` logs an error and exits with `WORKER_ORPHANED_EXIT_CODE`, which is `75`. Exiting immediately ensures an orphaned worker whose lease can no longer be renewed makes no further fenced writes.

## getContext

```ts [@notation/durable/worker]
export const getContext: () => EventContext;
```

`getContext()` returns the `EventContext` map for the active delivery inside the worker process, or `undefined` before the first delivery arrives, letting helpers read context without threading `event.context` everywhere.
