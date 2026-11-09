# @notation/durable/worker

`@notation/durable/worker` runs each delivery of an execution in a separate worker process. The server process creates the invoker, and the worker script creates the worker. The [Quick start](../manual/quickstart.md) has both files.

```ts
import { createWorkflowInvoker, createWorkflowWorker } from "@notation/durable/worker";
```

## `createWorkflowInvoker(options)`

Returns a `WorkflowInvoker` for an `ExecutionDispatcher`. For each delivery, the invoker forks a new process that runs the worker script, sends it the event and the lease, and waits for the outcome. It kills the process after the reply.

| Option | Default | Does |
| --- | --- | --- |
| `workerPath` | Required | The worker script, as a path or a `file:` URL |
| `logger` | Required | A pino logger |
| `execPath` | `process.execPath` | The executable that runs the worker script |
| `handshakeTimeout` | `5000` | Milliseconds to wait for the worker's first message |

**The worker must run on the same runtime as the server**: Node with Node, or Bun with Bun. Bun and Node send messages between processes in different formats. A worker that does not answer within `handshakeTimeout` is killed, and the delivery fails. See [Node](../manual/node.md#worker-processes) and [Bun](../manual/bun.md#worker-processes).

`deliver` rejects, and the delivery ends without an outcome, in these cases:

| Case | Rejects with |
| --- | --- |
| The worker exits before it replies | `WorkerExitedError` |
| The worker replies with an infrastructure error | That error |
| The process cannot start, or does not answer in time | An `Error` |
| The delivery's lease is lost | An `InfrastructureError`. The invoker kills the worker with `SIGKILL` first, so even a running step function stops |

See [Running workflows](./runtime.md#running-workflows) for what the dispatcher does with each.

## `createWorkflowWorker(runner, logger)`

Returns a worker for the worker script. `runner` is a `WorkflowRunner`. `listen()` tells the invoker that the worker is ready, and runs each delivery that the invoker sends with `runner.run`:

```ts [src/durable/worker.ts]
createWorkflowWorker(runner, logger).listen();
```

**The worker exits as soon as its connection to the server process closes.** If the server process dies, nothing renews the delivery's lease, and another server can claim the execution. The worker exits with code `75`, `WORKER_ORPHANED_EXIT_CODE`, so that it makes no more writes.

## `getContext()`

Returns the `context` map of the event that the worker process runs, or `undefined` before the first delivery. Workflow code can read the same map as `event.context`. See [Event](../manual/workflows.md#event).
