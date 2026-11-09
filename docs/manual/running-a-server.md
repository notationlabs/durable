# Running a server

This page runs Durable inside a long-running server on Bun. The server is the [Quick start](./quickstart.md) application, changed so that a signup request starts the onboarding workflow and returns at once. The workflow, `runtime.ts`, `worker.ts` and `client.ts` stay as they are in the Quick start:

```ts [src/server.ts]
import { dispatcher, sdk } from "./durable/client";

dispatcher.start();

const server = Bun.serve({
  port: 3000,
  routes: {
    "/signup": {
      POST: async (req) => {
        const { email } = await req.json();

        const { executionId } = await sdk.trigger({
          workflowId: "onboarding",
          params: { email },
        });

        return Response.json({ executionId }, { status: 202 });
      },
    },
  },
});

process.on("SIGTERM", async () => {
  await server.stop();
  await dispatcher.stop();
  process.exit(0);
});
```

The server runs with `bun src/server.ts`. For the same server on Node, see [Node](./node.md).

## Processes

The server process starts a worker process for each delivery:

| Process | Runs | Lives |
| --- | --- | --- |
| Server | `server.ts`: the HTTP routes, the SDK and the dispatcher | Until it is stopped |
| Worker | `worker.ts`: the workflow code, for one delivery | Until the delivery suspends or ends |

The invoker starts each worker with the same executable as the server, `bun` or `node`. Both processes open the SQLite file, and the worker writes each step's record to it.

**Each delivery starts a new worker process.** A worker does not keep variables, caches or connections from one delivery to the next. Code that `worker.ts` imports runs again in each new worker, including code at the top level of a module.

A worker writes to the server's standard output and error, so its logs appear with the server's.

## SQLite file

`createSqliteDb` opens the file with the runtime's SQLite driver, `bun:sqlite` or `node:sqlite`. It creates the file and its folder if they do not exist.

The file is in WAL mode by default, so SQLite keeps two more files next to it, `durable.sqlite-wal` and `durable.sqlite-shm`. All three belong to the database. Keep them on a local disk: SQLite's WAL mode does not work on a network file system.

## Several servers on one file

Several server processes on the same machine can share one SQLite file. Each process runs its own dispatcher, and each dispatcher runs up to 10 deliveries at once.

A dispatcher takes a lease on an execution before it delivers it, so only one process delivers an execution at a time. Any process can deliver any execution, not only the one that started it.

For example, two servers took 10 signups between them, and sent 20 emails. Each step ran once, and each server ran about half of the deliveries.

A process that writes waits for another process's write to finish, for up to 5 seconds, before it fails with `database is locked`. See [Delivery guarantees](../runtime-authors/delivery-guarantees.md) for how leases decide which process delivers.

## Stopping the server

`dispatcher.stop()` stops taking new deliveries, and waits until the deliveries in progress have ended. The example calls it when the server receives `SIGTERM`, so a running step finishes and is recorded before the server exits.

**Send `SIGTERM` to the server process only.** A worker that receives the signal exits before it replies, and its delivery does not end. systemd, for example, sends `SIGTERM` to every process of a service by default. Set `KillMode=mixed` to send it to the main process only.

A delivery whose server process dies ends in these steps:

1. The worker sees that the server has gone, and exits without writing anything more.
2. Nothing renews the delivery's lease. The lease expires 30 seconds after its last renewal.
3. Another dispatcher, or the same server after a restart, delivers the execution again.

The new delivery replays the recorded steps, and runs the step that was in progress again from its start. See [a step can run twice](./effects.md#problem-a-step-can-run-twice).
