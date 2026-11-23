# Running a Server

A Notation Durable application runs in one codebase split across two process roles: a long-lived server process that runs your HTTP handlers, SDK, and `ExecutionDispatcher`, and short-lived worker processes that execute workflow deliveries in isolation against a shared SQLite database file.

Using the `onboarding` workflow and Durable setup from [Quickstart](./quickstart.md), mount an HTTP endpoint that triggers executions and drains in-flight deliveries on shutdown:

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

Run the server with `bun src/server.ts`. See [Bun & Node](./runtimes.md) for the `node:http` server equivalent.

## Server vs. worker processes

`ExecutionDispatcher` claims due executions and delegates each delivery to `createWorkflowInvoker`, which forks a fresh child process running `worker.ts`:

| Process | Role |
| :--- | :--- |
| Server | Runs HTTP routes, SDK calls, and `ExecutionDispatcher` until stopped |
| Worker | Runs workflow code for a single delivery until the execution suspends or finishes |

Because each delivery starts a new worker process, module-level variables, in-memory caches, and database connection pools in `worker.ts` do not persist between deliveries, and top-level module initialization runs on every delivery. Workers inherit the server's standard output and standard error streams, so worker logs appear alongside server logs.

## SQLite file requirements

`createSqliteDb` opens the database file (`./.db/durable.sqlite` by default, creating parent directories automatically) and enables WAL mode (`wal: true`) with a 5-second busy timeout.

In WAL mode, SQLite maintains two companion files next to the database file: `durable.sqlite-wal` and `durable.sqlite-shm`. Keep all three files on a local filesystem – never on NFS or another network-attached filesystem that does not support POSIX shared memory and file locking for WAL.

When taking backups or moving the database, copy all three files consistently or use SQLite's online backup API.

## Sharing one SQLite file across processes

Multiple server processes on the same machine can share a single SQLite database file. Each process runs its own `ExecutionDispatcher` (claiming up to 10 concurrent deliveries by default), and any server sharing the file can claim and deliver any due execution – even an execution originally admitted by another process.

Concurrent writes across processes wait up to 5 seconds for the SQLite write lock before failing with `database is locked`, including when multiple processes open the database simultaneously and switch it to WAL mode.

### Scoping work with `filter`

If a dedicated process or dispatcher should execute only specific executions in a shared SQLite file, pass `filter: { executionIds }` to `ExecutionDispatcher`:

```ts [src/scoped-dispatcher.ts]
import pino from "pino";
import { ExecutionDispatcher } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import { runtime } from "./durable/runtime";

const logger = pino({ level: "warn" });

const invoker = createWorkflowInvoker({
  workerPath: new URL("./durable/worker.ts", import.meta.url).href,
  logger,
});

const dispatcher = new ExecutionDispatcher(runtime, {
  invoker,
  logger,
  filter: { executionIds: ["order-42", "order-43"] },
});
```

`filter.executionIds` restricts claims to matching IDs so unrelated executions in the same SQLite file are left unclaimed without accumulating redelivery backoff. Passing an empty `executionIds` array throws a `RangeError`.

## Graceful shutdown

Stop the HTTP server before calling `dispatcher.stop()`. `dispatcher.stop()` stops claiming new work and waits for every active delivery to finish and record its outcome before your process exits.

**Send `SIGTERM` to the server process only.** Killing a worker mid-delivery prevents it from replying to the server and forces redelivery after its lease expires. Under `systemd`, the default `control-group` kill mode sends `SIGTERM` to every process in the service's cgroup, including forked workers. Switch `KillMode` to `mixed` so only the main server receives the stop signal:

```ini [durable.service]
[Service]
ExecStart=/usr/local/bin/bun /srv/app/src/server.ts
KillMode=mixed
TimeoutStopSec=60
```

If a server process crashes or is killed forcefully before draining:

1. Each active worker detects that its IPC channel closed and exits immediately without writing further steps.
2. The execution's 30-second lease stops renewing and expires.
3. Another server sharing the SQLite file – or the restarted server – claims the execution, replays recorded steps, and runs the interrupted step again.

Because an interrupted step function may have already called an external API before the crash, wrap non-idempotent side effects with `step.effect` as described in [External Effects](./external-effects.md).

## Rolling-upgrade safety

When upgrading an application that has active executions in SQLite:

- **Deploy matching server and worker code** – Run your server and worker entrypoints from the same `@notation/durable` version and workflow build so step codecs and worker messages match.
- **Keep replay paths compatible** – When a suspended execution resumes on new code, Durable replays it from line 1 against recorded step results. Preserve existing step keys and result shapes for in-flight workflows, or drain active executions before shipping breaking workflow changes.
- **Upgrade all processes before calling `cancel()`** – `SqliteRuntime` automatically adds cancellation columns when opening an existing database, and existing rows read as non-cancelled attached children. Because cancellation metadata is delivered to workers over IPC, every server and worker sharing the SQLite file must run a cancellation-aware version before anything calls `sdk.cancel()`.
- **Upgrade all processes before setting `maxExpiredDeliveries`** – `SqliteRuntime` adds the `expired_delivery_count` column when it opens an existing database, and existing rows read as 0. A process from before this version does not reset the count when it records progress, so an execution it runs can reach the limit early.
- **Upgrade all processes before using `step.join` or `step.call`** – Waiting joins persist join wait records in SQLite. Ensure every process sharing the database runs a version that supports joins – including timed join deadlines if used – before admitting workflows that call `step.join` or `step.call`.
- **Drain across major record-format upgrades** – Drain active executions before upgrading across major releases that change the step-record encoding format.
