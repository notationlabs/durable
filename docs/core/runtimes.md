# Bun & Node

Notation Durable runs on Bun 1.3 or newer and Node 22.13 or newer. Workflow and router files are identical on both runtimes; only the SQLite connector, HTTP server API, and TypeScript execution rules differ.

## Setup on Bun vs. Node

Swap `@notation/durable/sqlite/bun` for `@notation/durable/sqlite/node`, replace `Bun.serve` with `node:http`, and include `.ts` extensions on relative imports when running directly on Node:

::code-group

```ts [src/durable/runtime.ts (Bun)]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

```ts [src/durable/runtime.ts (Node)]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/node";

const db = createSqliteDb({
  path: "./.db/durable.sqlite",
});

export const runtime = new SqliteRuntime(db);
```

```ts [src/server.ts (Bun)]
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

```ts [src/server.ts (Node)]
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

```ts [src/durable/worker.ts (Node)]
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

::

`createRoutes` from `@notation/durable/http` works with Web `Request` and `Response` objects, so it mounts into Bun directly or into Node through any HTTP adapter bridging Node streams.

## Worker-executable consistency

`createWorkflowInvoker` forks each worker with `process.execPath` by default, or with the `execPath` option if provided, using advanced IPC serialization.

**The server and its workers must use the same runtime.** Bun and Node serialize IPC frames in incompatible formats:

- **Bun server** – Must launch Bun workers. A Node worker cannot decode the server's IPC frames and exits before replying.
- **Node server** – Must launch Node workers. A Bun worker never answers the `ready` handshake, causing the invoker to time out after 5 seconds by default and retry the delivery repeatedly.

## Bundling a Bun server

`client.ts` typically resolves the worker with `new URL("./worker.ts", import.meta.url).href`, which fails once `server.ts` is bundled into `dist/server.js`.

Pass `DURABLE_WORKER_PATH` (or another environment variable) to `createWorkflowInvoker` so the server can locate the built worker file:

```ts [src/durable/client.ts]
import pino from "pino";
import { ExecutionDispatcher, createLocalSdk } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import type { Router } from "../workflows/router";
import { runtime } from "./runtime";

const logger = pino({ level: "warn" });

const invoker = createWorkflowInvoker({
  workerPath: process.env.DURABLE_WORKER_PATH ?? new URL("./worker.ts", import.meta.url).href,
  logger,
});

export const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger });
export const sdk = createLocalSdk<Router>(runtime, { invoker });
```

Bundle `src/server.ts` and `src/durable/worker.ts` as separate entry points, then launch the bundled server with `DURABLE_WORKER_PATH` set:

```sh [Terminal]
bun build src/server.ts src/durable/worker.ts --outdir dist --target bun
DURABLE_WORKER_PATH=dist/durable/worker.js bun dist/server.js
```

## Running and compiling on Node

### Node versions

| Node version | Command |
| :--- | :--- |
| Node 24 and newer | `node src/server.ts` |
| Node 22.18 to 23 | `node src/server.ts` |
| Node 22.13 to 22.17 | `node --experimental-strip-types src/server.ts` |

On Node 22, `node:sqlite` prints an `ExperimentalWarning` from both the server and every forked worker. Because `createWorkflowInvoker` forwards parent Node flags to child workers, pass `--disable-warning=ExperimentalWarning` once to silence it everywhere:

```sh [Terminal]
node --disable-warning=ExperimentalWarning src/server.ts
```

### Node TypeScript requirements

Node executes `.ts` files by stripping types in place rather than transpiling or rewriting imports:

- **Set `"type": "module"`** – Declare `"type": "module"` in `package.json` so Node treats `.ts` files as ES modules.
- **Include `.ts` extensions on relative imports** – Write `"./runtime.ts"` and `"../workflows/router.ts"`, because Node does not resolve extensionless relative imports.
- **Use `import type` for types** – Node removes `import type` statements at runtime, whereas a value import of a type name fails with a missing export error.
- **Stick to erasable syntax** – Avoid TypeScript `enum`, `namespace`, and constructor parameter properties unless you compile with `tsc`.

Configure `tsconfig.json` so `tsc` enforces these same rules during type-checking (with `@types/node` installed):

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

| Option | Behavior |
| :--- | :--- |
| `allowImportingTsExtensions` | Allows `.ts` extensions on relative imports when `noEmit` is enabled |
| `verbatimModuleSyntax` | Flags type imports written without `import type` |
| `erasableSyntaxOnly` | Rejects enums, namespaces, and parameter properties that Node cannot strip |

### Compiling with `tsc`

To deploy compiled JavaScript instead of running `.ts` files directly, compile with `--rewriteRelativeImportExtensions` so `tsc` turns `.ts` import specifiers into `.js`:

```sh [Terminal]
tsc --noEmit false --rewriteRelativeImportExtensions --outDir dist
```

Because `tsc` does not rewrite string paths passed to `new URL("./worker.ts", import.meta.url)`, use the same `DURABLE_WORKER_PATH` fallback in `src/durable/client.ts` and start the compiled server from `dist`:

```sh [Terminal]
DURABLE_WORKER_PATH=dist/durable/worker.js node dist/server.js
```
