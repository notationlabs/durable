import { statSync } from "node:fs";
import { createServer } from "node:http";
import pino from "pino";
import { ExecutionDispatcher } from "@durable/core";
import { createRoutes, type RouteHandler } from "@durable/http-server";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createWorkflowInvoker } from "@durable/worker-invoker";
import { openDb } from "./sqlite-db";

const logger = pino({ level: process.env.LOAD_LOG_LEVEL ?? "error" });
const db = await openDb();
const runtime = new SqliteRuntime(db);
const invoker = createWorkflowInvoker({
  // The Node caller points this at the bundled worker.
  workerPath: process.env.LOAD_WORKER_PATH ?? new URL("sqlite-worker.ts", import.meta.url).href,
  logger,
});
const concurrency = process.env.LOAD_CONCURRENCY ? Number(process.env.LOAD_CONCURRENCY) : undefined;
new ExecutionDispatcher(runtime, { invoker, logger, concurrency }).start();

const routes: Record<string, RouteHandler> = Object.fromEntries(
  Object.entries(createRoutes({ runtime, invoker, logger })).map(([path, { POST }]) => [path, POST])
);
routes["/load/probe"] = () => {
  const one = (sql: string) => Object.values(db.query<Record<string, number>>(sql).get() ?? {})[0] ?? 0;
  const size = (path: string) => {
    try {
      return statSync(path).size;
    } catch {
      return 0;
    }
  };
  return Response.json({
    executions: one("SELECT count(*) FROM executions"),
    databaseKb: Math.round((one("PRAGMA page_count") * one("PRAGMA page_size")) / 1024),
    walKb: Math.round(size(`${process.env.LOAD_DB}-wal`) / 1024),
  });
};

const notFound = () => new Response("Not found", { status: 404 });

// The parent process reads the port from the first line of stdout.
if (process.versions.bun) {
  const server = Bun.serve({
    port: 0,
    fetch: (request) => (routes[new URL(request.url).pathname] ?? notFound)(request),
  });
  console.log(JSON.stringify({ port: server.port }));
} else {
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const url = new URL(req.url ?? "/", "http://localhost");
    const request = new Request(url.href, {
      method: req.method,
      headers: req.headers as Record<string, string>,
      body: req.method === "GET" || req.method === "HEAD" ? undefined : Buffer.concat(chunks),
    });
    const response = await (routes[url.pathname] ?? notFound)(request);
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(Buffer.from(await response.arrayBuffer()));
  });
  server.listen(0, () => {
    const address = server.address();
    console.log(JSON.stringify({ port: typeof address === "object" && address ? address.port : 0 }));
  });
}
