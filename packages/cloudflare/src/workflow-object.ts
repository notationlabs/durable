import { EventEmitter } from "node:events";
import type { Logger } from "pino";
import {
  DEFAULT_DISPATCHER_CONCURRENCY,
  ExecutionDispatcher,
  WorkflowRunner,
  type MiddlewareFunction,
  type WorkflowInvoker,
  type WorkflowRouter,
} from "@durable/core";
import {
  createRoutes,
  type ObservationMiddlewareFunction,
} from "@durable/http-server";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { DurableObjectSqliteDriver } from "./do-sqlite-driver";
import type { DurableObjectStateLike } from "./storage";

export type WorkflowObjectOptions<Router extends WorkflowRouter> = {
  router: Router;
  /** @default a logger that writes to `console` */
  logger?: Logger;
  /** The path prefix of the HTTP routes, such as `/durable`. @default "" */
  basePath?: `/${string}`;
  /** Runs before admission on `/trigger`. */
  middleware?: MiddlewareFunction[];
  /** Runs before `/poll`, `/wait`, `/list`, `/events` and `/cancel` read any record. */
  observationMiddleware?: ObservationMiddlewareFunction[];
  /** The longest the object holds a `/wait` request open, in milliseconds. @default 8000 */
  maxWait?: number;
  /** The maximum number of deliveries the object runs at once. @default 10 */
  concurrency?: number;
  /** How long each lease lasts unless renewed, in milliseconds. @default 30_000 */
  leaseDuration?: number;
};

/** The shortest wait before claiming again while deliveries are in flight. */
const MIN_CLAIM_DELAY = 10;

/**
 * The longest `alarm` waits for deliveries. Cloudflare stops an alarm handler
 * after 15 minutes. Deliveries still in flight carry on after it returns.
 */
const MAX_ALARM_WAIT = 5 * 60_000;

/**
 * An alarm this close to the clock is written again when the claim time
 * moves, instead of being left to fire early.
 */
const ALARM_MARGIN = 1_000;

const consoleLogger = {
  info: (...args: unknown[]) => console.log(...args),
  warn: (...args: unknown[]) => console.warn(...args),
  error: (...args: unknown[]) => console.error(...args),
} as unknown as Logger;

/**
 * Runs workflows inside one Durable Object. The object's SQLite storage holds
 * the runtime, and deliveries run in the object's own isolate.
 *
 * The object does not poll. After every write to the runtime it sets the
 * object's alarm to the earliest time an execution can be claimed, in the same
 * turn as the write, and claims what is due. While deliveries are in flight
 * it claims again when one settles, when a write changes that time, and when
 * that time arrives. When none is in flight it stops, and the alarm starts it
 * again. If the object is evicted during a delivery, the delivery's lease
 * expires and the alarm delivers the execution again.
 *
 * Call it from a Durable Object class:
 *
 * ```ts
 * export class Workflows extends DurableObject {
 *   durable = createWorkflowObject(this.ctx, { router });
 *   fetch(request: Request) { return this.durable.fetch(request); }
 *   alarm() { return this.durable.alarm(); }
 * }
 * ```
 */
export function createWorkflowObject<Router extends WorkflowRouter>(
  ctx: DurableObjectStateLike,
  options: WorkflowObjectOptions<Router>
) {
  const logger = options.logger ?? consoleLogger;
  const concurrency = options.concurrency ?? DEFAULT_DISPATCHER_CONCURRENCY;
  const { storage } = ctx;

  const db = new DurableObjectSqliteDriver(storage);
  const runtime = new SqliteRuntime(db);
  const runner = new WorkflowRunner(runtime, { router: options.router, logger });
  const invoker: WorkflowInvoker = {
    workflowEndEmitter: new EventEmitter(),
    deliver: (event, delivery) => runner.run(event, delivery),
  };
  // Never started: the pump below calls `tick` instead of the poll loop.
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    concurrency,
    leaseDuration: options.leaseDuration,
  });
  const routes = createRoutes({
    basePath: options.basePath,
    runtime,
    invoker,
    logger,
    middleware: options.middleware,
    observationMiddleware: options.observationMiddleware ?? [],
    maxWait: options.maxWait,
  });

  // The time the alarm is set to, null when it is deleted, and undefined when
  // unknown: after a restart, a failed write, or the alarm firing.
  let armedAt: number | null | undefined;
  // The earliest claim time when it was last read.
  let claimAt: number | null | undefined;
  let requested = false;
  let nudge: (() => void) | undefined;
  let pumping: Promise<void> | undefined;

  // Sets the alarm to the earliest claim time, and returns whether that time
  // has changed since the last call. An alarm that is already set, earlier
  // than the claim time and still ahead of the clock is left alone: each lease
  // renewal moves the claim time later, and the alarm sets itself again when
  // it fires. An alarm at or behind the clock is always written, because one
  // set inside the alarm handler to the handler's own time did not fire.
  const arm = () => {
    const at = runtime.executions.nextClaimAt();
    const changed = at !== claimAt;
    claimAt = at;
    const covered =
      at === null
        ? armedAt === null
        : typeof armedAt === "number" &&
          armedAt <= at &&
          (armedAt === at || armedAt > Date.now() + ALARM_MARGIN);
    if (!covered) {
      armedAt = at;
      const write =
        at === null ? storage.deleteAlarm() : storage.setAlarm(Math.max(at, Date.now()));
      write.catch((err) => {
        armedAt = undefined;
        logger.error({ err }, "Failed to set the Durable Object alarm");
      });
    }
    return changed;
  };

  const pump = () => {
    requested = true;
    nudge?.();
    pumping ??= run().finally(() => {
      pumping = undefined;
    });
    return pumping;
  };

  const run = async () => {
    // Other writes may be mid-turn when a commit starts the pump.
    await Promise.resolve();
    while (requested || dispatcher.activeDeliveries > 0) {
      requested = false;
      await dispatcher.tick();
      arm();
      if (requested || dispatcher.activeDeliveries === 0) continue;
      let timer: ReturnType<typeof setTimeout> | undefined;
      await new Promise<void>((resolve) => {
        nudge = resolve;
        void dispatcher.settled().then(resolve);
        // With every slot busy, nothing can be claimed until a delivery settles.
        if (dispatcher.activeDeliveries >= concurrency) return;
        const at = runtime.executions.nextClaimAt();
        if (at !== null) {
          timer = setTimeout(resolve, Math.max(MIN_CLAIM_DELAY, at - Date.now()));
        }
      });
      clearTimeout(timer);
      nudge = undefined;
    }
  };

  // The hook is attached after the runtime has created its tables.
  db.onCommit = () => {
    if (!arm()) return;
    const starting = !pumping;
    const pumped = pump();
    if (!starting) return;
    // If this fails, the alarm set above runs the pump again.
    ctx.waitUntil(
      pumped.catch((err) => {
        logger.error({ err }, "Failed to claim due executions");
      })
    );
  };

  return {
    runtime,
    invoker,

    /** Serves the trigger, poll, wait, list, events and cancel routes. */
    async fetch(request: Request): Promise<Response> {
      const route = routes[new URL(request.url).pathname];
      if (!route) return new Response("Not found", { status: 404 });
      if (request.method !== "POST") {
        return new Response("Method not allowed", {
          status: 405,
          headers: { allow: "POST" },
        });
      }
      return route.POST(request);
    },

    /**
     * Claims what is due, and resolves when no delivery is in flight or after
     * `MAX_ALARM_WAIT`, so the object stays in memory while deliveries run. A
     * rejection makes Cloudflare run the alarm again.
     */
    async alarm(): Promise<void> {
      // Cloudflare clears the alarm before it calls the handler.
      armedAt = undefined;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          pump(),
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, MAX_ALARM_WAIT);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
      arm();
    },
  };
}
