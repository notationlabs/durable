import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Logger } from "pino";
import { expect, vi } from "vitest";
import {
  ExecutionDispatcher,
  WorkflowRunner,
  type ExecutionObserver,
  type MiddlewareFunction,
  type Runtime,
  type TriggerAck,
  type WorkflowInvoker,
  type WorkflowRouter,
} from "@yieldstar/core";
import { createRoutes, type ObservationMiddlewareFunction } from "@yieldstar/http-server";
import { createWorkflowInvoker } from "@yieldstar/test-invoker";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { createHttpSdkFactory, createLocalSdk, registerErrorClass } from "yieldstar";

const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@yieldstar/sqlite-runtime/bun")
    : await import("@yieldstar/sqlite-runtime/node");

export const logger = { info() {}, error() {}, warn() {} } as unknown as Logger;

export const LEASE = 30_000;

export type BackendKind = "memory" | "sqlite";
export type SdkKind = "local" | "http";

export const backendKinds: BackendKind[] = ["memory", "sqlite"];
export const sdkKinds: SdkKind[] = ["local", "http"];

const cleanups: (() => void | Promise<void>)[] = [];

/** Runs every cleanup registered since the last call. Call it from `afterEach`. */
export async function cleanup() {
  for (const fn of cleanups.splice(0).reverse()) await fn();
  vi.unstubAllGlobals();
  vi.useRealTimers();
}

export type Backend = {
  kind: BackendKind;
  runtime: Runtime;
  /** Another client on the same backend, as another process would open it. */
  peer: Runtime;
  /** Opens another client, for SQLite with an optional clock. */
  open(options?: { clock?: () => number }): Runtime;
  /** The SQLite database file, for processes spawned by the test. */
  path?: string;
};

export function openBackend(kind: BackendKind): Backend {
  if (kind === "memory") {
    const runtime = new MemoryRuntime();
    return { kind, runtime, peer: runtime, open: () => runtime };
  }
  const dir = mkdtempSync(join(tmpdir(), "yieldstar-observation-"));
  const path = join(dir, "executions.db");
  const dbs: ReturnType<typeof createSqliteDb>[] = [];
  const open = (options?: { clock?: () => number }) => {
    const db = createSqliteDb({ path, wal: true });
    dbs.push(db);
    return new SqliteRuntime(db, options);
  };
  cleanups.push(() => {
    for (const db of dbs) db.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return { kind, runtime: open(), peer: open(), open, path };
}

type TriggerRequest = {
  workflowId: string;
  executionId?: string;
  params?: unknown;
  context?: Record<string, unknown>;
  idempotent?: boolean;
};

/** The surface both SDKs share, plus `triggerAndWait` built from each SDK's own API. */
export type TestSdk = ExecutionObserver & {
  kind: SdkKind;
  trigger(event: TriggerRequest): Promise<TriggerAck>;
  triggerAndWait(event: TriggerRequest): Promise<unknown>;
  /** Every HTTP response, in order. Empty for the local SDK. */
  responses: { route: string; status: number }[];
};

export type SdkOptions = {
  invoker?: Pick<WorkflowInvoker, "workflowEndEmitter">;
  middleware?: MiddlewareFunction[];
  observationMiddleware?: ObservationMiddlewareFunction[];
  waitCap?: number;
};

const hosts = new Map<string, ReturnType<typeof createRoutes>>();
let hostCount = 0;

async function routeFetch(input: string | URL | Request, init?: RequestInit) {
  const request = new Request(input, init);
  const url = new URL(request.url);
  const routes = hosts.get(url.host);
  const route = routes?.[url.pathname];
  if (!route) return new Response("Not found", { status: 404 });
  return route.POST(request);
}

/** Routes for `runtime`, reachable through `fetch` at the returned URL. */
export function serveRoutes(runtime: Runtime, options: SdkOptions = {}) {
  if (hosts.size === 0) {
    cleanups.push(() => hosts.clear());
  }
  vi.stubGlobal("fetch", routeFetch);
  const host = `yieldstar-${++hostCount}.test`;
  const routes = createRoutes({
    runtime,
    logger,
    invoker: options.invoker,
    middleware: options.middleware,
    observationMiddleware: options.observationMiddleware,
    waitCap: options.waitCap,
  });
  hosts.set(host, routes);
  return { url: `http://${host}`, routes };
}

export function createTestSdk(
  kind: SdkKind,
  runtime: Runtime,
  options: SdkOptions = {}
): TestSdk {
  const responses: TestSdk["responses"] = [];
  if (kind === "local") {
    const sdk = createLocalSdk<any>(runtime, {
      invoker: options.invoker,
      pollInterval: 5,
    });
    return {
      kind,
      responses,
      trigger: (event) => sdk.trigger(event as never),
      triggerAndWait: (event) => sdk.triggerAndWait(event as never),
      poll: (executionId) => sdk.poll(executionId),
      wait: (executionId, waitOptions) => sdk.wait(executionId, waitOptions),
      list: (request) => sdk.list(request),
    };
  }
  const { url, routes } = serveRoutes(runtime, options);
  for (const [path, route] of Object.entries(routes)) {
    const handler = route.POST;
    route.POST = async (request) => {
      const response = await handler(request);
      responses.push({ route: path.slice(1), status: response.status });
      return response;
    };
  }
  const sdk = createHttpSdkFactory<any>()({ url });
  return {
    kind,
    responses,
    trigger: async (event) => {
      const { executionId, status } = await sdk.trigger(event as never);
      return { executionId, status };
    },
    triggerAndWait: async (event) => (await sdk.trigger(event as never)).waitForResult(),
    poll: (executionId) => sdk.poll(executionId),
    wait: (executionId, waitOptions) => sdk.wait(executionId, waitOptions),
    list: (request) => sdk.list(request),
  };
}

/** Runs `router` against `runtime` in this process until the test ends. */
export function startDispatcher(runtime: Runtime, router: WorkflowRouter) {
  const runner = new WorkflowRunner(runtime, { router, logger });
  const invoker = createWorkflowInvoker({ runner, logger });
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    pollInterval: 1,
    keepAlive: false,
  });
  dispatcher.start();
  cleanups.push(() => dispatcher.stop());
  return { invoker, dispatcher };
}

/**
 * Starts `test/fixtures/observation/dispatcher.mjs` in its own process. It runs
 * a dispatcher against the SQLite file until it is killed.
 */
export function spawnDispatcher(path: string): ChildProcess {
  const child = spawn(
    process.execPath,
    [new URL("./fixtures/observation/dispatcher.mjs", import.meta.url).pathname, path],
    { stdio: ["ignore", "inherit", "inherit"] }
  );
  cleanups.push(() => {
    child.kill("SIGKILL");
  });
  return child;
}

export async function admit(
  runtime: Runtime,
  executionId: string,
  workflowId = "charge-order",
  params: Record<string, unknown> = { orderId: 42 }
) {
  const admitted = await runtime.executions.admit({
    executionId,
    workflowId,
    params,
    context: new Map([["token", "secret"]]),
  });
  expect(admitted).toEqual({ status: "accepted", executionId });
}

export async function claim(runtime: Runtime, executionId: string) {
  const leases = await runtime.executions.claimDue({
    limit: 1,
    duration: LEASE,
    filter: { executionIds: [executionId] },
  });
  expect(leases).toHaveLength(1);
  return leases[0];
}

export async function suspend(runtime: Runtime, executionId: string) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: { status: "suspended" },
  });
}

export async function complete(runtime: Runtime, executionId: string, result: unknown) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: { status: "completed", result },
  });
}

export async function fail(runtime: Runtime, executionId: string, error: Error) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: { status: "failed", error },
  });
}

export async function elapsed<T>(fn: () => Promise<T>) {
  const start = Date.now();
  const value = await fn();
  return { value, ms: Date.now() - start };
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class CardDeclinedError extends Error {
  code = "CARD_DECLINED";
  constructor(message: string) {
    super(message);
    this.name = "CardDeclinedError";
  }
}

registerErrorClass("CardDeclinedError", CardDeclinedError);
