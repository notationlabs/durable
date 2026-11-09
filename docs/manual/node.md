# Node

Node 22.18 and newer run TypeScript files directly, as Bun does. Node only removes the types, though, and resolves imports as it does for JavaScript. So the code from the [Quick start](./quickstart.md) and [Running a server](./running-a-server.md) needs these changes to run on Node:

| Change | Why |
| --- | --- |
| Relative imports end in `.ts`, as in `"./runtime.ts"` | Node does not add a file extension to an import |
| `package.json` has `"type": "module"` | Node and TypeScript then read `.ts` files as ES modules |
| `createSqliteDb` and `SqliteRuntime` come from `@notation/durable/sqlite/node` | It uses Node's built-in `node:sqlite` |
| The server uses `node:http` instead of `Bun.serve` | `Bun.serve` exists only in Bun |

Types are imported with `import type`, as the Quick start already does. **Node removes `import type` lines, but keeps a plain import of a type.** The import then fails, because the module has no export with that name at runtime.

## Code

The runtime uses the Node connector:

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/node";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

The worker imports the router and the runtime with their extensions:

```ts [src/durable/worker.ts]
import pino from "pino";
import { WorkflowRunner } from "@notation/durable";
import { createWorkflowWorker } from "@notation/durable/worker";
import { router } from "../workflows/router.ts";
import { runtime } from "./runtime.ts";

const logger = pino({ level: "warn" });

const runner = new WorkflowRunner(runtime, {
  router,
  logger,
});

createWorkflowWorker(runner, logger).listen();
```

The server reads the request body from `node:http`, and starts the workflow as the Bun server does:

```ts [src/server.ts]
import { createServer } from "node:http";
import { dispatcher, sdk } from "./durable/client.ts";

dispatcher.start();

const server = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/signup") {
    res.writeHead(404).end();
    return;
  }

  let body = "";
  for await (const chunk of req) body += chunk;
  const { email } = JSON.parse(body);

  const { executionId } = await sdk.trigger({
    workflowId: "onboarding",
    params: { email },
  });

  res.writeHead(202, { "content-type": "application/json" });
  res.end(JSON.stringify({ executionId }));
});

server.listen(3000);

process.on("SIGTERM", async () => {
  server.close();
  await dispatcher.stop();
  process.exit(0);
});
```

`client.ts`, `onboarding.ts` and `router.ts` change only in their relative imports.

## Node versions

| Node | Command | Output |
| --- | --- | --- |
| 24 or newer | `node src/server.ts` | |
| 22.18 or a newer 22 release | `node src/server.ts` | SQLite's `ExperimentalWarning`, from the server and from each worker |
| 22.13 to 22.17 | `node --experimental-strip-types src/server.ts` | The same warning |

The invoker starts each worker with the flags that started the server, so a flag passed to `node` applies to the workers too. `node --disable-warning=ExperimentalWarning src/server.ts` hides the warning in every process.

## TypeScript configuration

This `tsconfig.json` lets `tsc` check the code as Node runs it. `"types": ["node"]` needs `@types/node` installed:

```json [tsconfig.json]
{
  "compilerOptions": {
    "module": "nodenext",
    "target": "esnext",
    "strict": true,
    "noEmit": true,
    "allowImportingTsExtensions": true,
    "verbatimModuleSyntax": true,
    "erasableSyntaxOnly": true,
    "types": ["node"]
  }
}
```

| Option | Does |
| --- | --- |
| `allowImportingTsExtensions` | Accepts imports that end in `.ts`. It requires `noEmit` |
| `verbatimModuleSyntax` | Reports a plain import of a type, which fails in Node |
| `erasableSyntaxOnly` | Reports enums, namespaces and parameter properties, which Node cannot run without a build step |

## Compiling to JavaScript

To run compiled JavaScript instead, `tsc` writes the files to `dist`, and changes the `.ts` imports to `.js`:

```sh
tsc --noEmit false --rewriteRelativeImportExtensions --outDir dist
```

`client.ts` finds the worker with `new URL("./worker.ts", import.meta.url)`, and `tsc` does not change that path. Pass the compiled worker's path to the invoker instead, for example from an environment variable:

```ts [src/durable/client.ts]
const invoker = createWorkflowInvoker({
  workerPath: process.env.DURABLE_WORKER_PATH ?? new URL("./worker.ts", import.meta.url).href,
  logger,
});
```

The server then runs from `dist`:

```sh
DURABLE_WORKER_PATH=dist/durable/worker.js node dist/server.js
```

## Worker processes

The invoker starts each worker with the executable that runs the server, `process.execPath`. The `execPath` option of `createWorkflowInvoker` chooses a different Node executable.

**The worker must run on Node too.** Bun and Node send messages between processes in different formats. A Bun worker started by a Node server does not answer the server's first message. The server waits 5 seconds, the delivery fails, and the dispatcher delivers the execution again later, with the same result.
