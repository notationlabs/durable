# Bun

Bun runs Durable's TypeScript files directly, so the [Quick start](./quickstart.md) and the server in [Running a server](./running-a-server.md) run on Bun as they are. Durable needs Bun 1.3 or newer.

## SQLite

`createSqliteDb` and `SqliteRuntime` come from `@notation/durable/sqlite/bun`, which uses Bun's built-in `bun:sqlite`:

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

## Worker processes

The invoker starts each worker with the executable that runs the server, `process.execPath`. The `execPath` option of `createWorkflowInvoker` chooses a different Bun executable.

**The worker must run on Bun too.** Bun and Node send messages between processes in different formats. A Node worker started by a Bun server cannot read the server's messages, so it exits without running the workflow. The dispatcher then delivers the execution again later, with the same result.

## Bundling the server

`client.ts` finds the worker from its own location, with `new URL("./worker.ts", import.meta.url)`. A bundled `server.js` has a different location, and no `worker.ts` next to it.

To bundle the server, build `worker.ts` as a second entry point:

```sh
bun build src/server.ts src/durable/worker.ts --outdir dist --target bun
```

Then pass the bundled worker's path to the invoker, for example from an environment variable:

```ts [src/durable/client.ts]
const invoker = createWorkflowInvoker({
  workerPath: process.env.DURABLE_WORKER_PATH ?? new URL("./worker.ts", import.meta.url).href,
  logger,
});
```

The server then runs from the bundle:

```sh
DURABLE_WORKER_PATH=dist/durable/worker.js bun dist/server.js
```
