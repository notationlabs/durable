import { EventEmitter } from "node:events";
import { DurableObject } from "cloudflare:workers";
import type { Logger } from "pino";
import {
  DEFAULT_DISPATCHER_CONCURRENCY,
  ExecutionDispatcher,
  WorkflowRunner,
  type MiddlewareFunction,
  type Runtime,
  type WorkflowInvoker,
  type WorkflowRouter,
} from "@durable/core";
import {
  createRoutes,
  type ObservationMiddlewareFunction,
} from "@durable/http-server";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import {
  createIsolatedDeliver,
  runDelivery,
  type DeliveryReply,
  type DeliveryRequest,
  type DeliveryRunner,
} from "./delivery";
import { DurableObjectSqliteDriver } from "./do-sqlite-driver";
import type { RuntimeCall } from "./remote-runtime";
import type { DurableObjectNamespaceLike, DurableObjectStateLike } from "./storage";

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
  /**
   * The number of consecutive deliveries of one execution that may end with
   * an expired lease, such as after an eviction or a CPU limit reset. The
   * claim that would reach it records the execution `failed` with a
   * `DeliveryLimitError` instead of delivering it again.
   * @default no limit
   */
  maxExpiredDeliveries?: number;
  /**
   * Whether each delivery runs in another instance of the class, with an
   * isolate and a CPU limit to itself. With `false`, every delivery runs in
   * the isolate of the object that claims it, where one step that exceeds
   * the CPU limit resets the object and every delivery in it.
   * @default true
   */
  isolateDeliveries?: boolean;
};

/** `createWorkflowObject` runs every delivery in the object's own isolate. */
export type WorkflowHostOptions<Router extends WorkflowRouter> = Omit<
  WorkflowObjectOptions<Router>,
  "isolateDeliveries"
>;

type Deliver = WorkflowInvoker["deliver"];

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

/**
 * The wait before the alarm runs again after `alarm` fails. Each failure in a
 * row doubles the wait, up to `MAX_ALARM_RETRY_DELAY`.
 */
const ALARM_RETRY_DELAY = 500;
const MAX_ALARM_RETRY_DELAY = 60_000;

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
 * `WorkflowObject` builds a class on it and runs each delivery in its own
 * instance. Call `createWorkflowObject` directly only to write the class
 * yourself and run every delivery in the object's own isolate:
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
  options: WorkflowHostOptions<Router>
) {
  return createHost(ctx, options);
}

function createHost<Router extends WorkflowRouter>(
  ctx: DurableObjectStateLike,
  options: WorkflowHostOptions<Router>,
  // Runs each delivery somewhere other than this isolate.
  createDeliver?: (runtime: Runtime) => Deliver
) {
  const logger = options.logger ?? consoleLogger;
  const concurrency = options.concurrency ?? DEFAULT_DISPATCHER_CONCURRENCY;
  const { storage } = ctx;

  const db = new DurableObjectSqliteDriver(storage);
  const runtime = new SqliteRuntime(db);
  const invoker: WorkflowInvoker = {
    workflowEndEmitter: new EventEmitter(),
    deliver: createDeliver?.(runtime) ?? inlineDeliver(runtime, options.router, logger),
  };
  // Never started: the pump below calls `tick` instead of the poll loop.
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    concurrency,
    leaseDuration: options.leaseDuration,
    maxExpiredDeliveries: options.maxExpiredDeliveries,
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
  // After `alarm` fails, the alarm is not set earlier than this.
  let retryAt = 0;
  let alarmFailures = 0;
  // The earliest claim time when it was last read.
  let claimAt: number | null | undefined;
  let requested = false;
  let nudge: (() => void) | undefined;
  let pumping: Promise<void> | undefined;

  // Sets the alarm to the earliest claim time, and returns whether that time
  // has changed since the last call. An alarm that is already set, earlier
  // than the claim time and still ahead of the clock is left alone: each lease
  // renewal moves the claim time later, and the alarm sets itself again when
  // it fires. An alarm at or behind the clock is always written, and never at
  // the clock itself: on Cloudflare an alarm set inside the alarm handler to
  // the handler's own scheduled time fires up to a minute late, and the clock
  // in the handler often reads that time. One millisecond later fires on time.
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
        at === null
          ? storage.deleteAlarm()
          : storage.setAlarm(Math.max(at, Date.now() + 1, retryAt));
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
      // Claim again after every wait. A delivery that settles frees a slot,
      // and an execution that was due with every slot busy is still due.
      requested = true;
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

  // A new instance does not know the alarm time. If an alarm write failed
  // before an eviction, executions are due and no alarm is set, and without
  // this read nothing would claim them until the next write.
  storage
    .getAlarm()
    .then((current) => {
      // A commit or the alarm handler has set the alarm since.
      if (armedAt !== undefined || pumping) return;
      armedAt = current;
      arm();
    })
    .catch((err) => {
      logger.error({ err }, "Failed to read the Durable Object alarm");
    });

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
     * `MAX_ALARM_WAIT`, so the object stays in memory while deliveries run.
     *
     * Cloudflare runs a rejected alarm handler again six times and then drops
     * the alarm. So a failure here is logged and the alarm is set to a later
     * time, and `alarm` rejects only if that write fails too.
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
        retryAt = 0;
        alarmFailures = 0;
        arm();
      } catch (err) {
        const delay = Math.min(
          MAX_ALARM_RETRY_DELAY,
          ALARM_RETRY_DELAY * 2 ** alarmFailures++
        );
        logger.error({ err, delay }, "The alarm failed, and is set to run again");
        retryAt = Date.now() + delay;
        // Unknown, so that the next write sets the alarm from the claim time.
        armedAt = undefined;
        await storage.setAlarm(retryAt);
      } finally {
        clearTimeout(timer);
      }
    },
  };
}

function inlineDeliver(runtime: Runtime, router: WorkflowRouter, logger: Logger): Deliver {
  const runner = new WorkflowRunner(runtime, { router, logger });
  return (event, delivery) => runner.run(event, delivery);
}

// A service binding answers every property with an RPC method, so a namespace
// is recognised by its class: `DurableObjectNamespace` in `env` and
// `LoopbackDurableObjectNamespace` in `ctx.exports`.
function isNamespace(value: unknown): value is DurableObjectNamespaceLike {
  return (
    typeof value === "object" &&
    value !== null &&
    /DurableObjectNamespace$/.test(value.constructor?.name ?? "")
  );
}

/**
 * Finds the namespace of the object's own class in `ctx.exports` or `env`. A
 * namespace accepts an ID string only when the ID belongs to its class.
 */
function findOwnNamespace(
  ctx: DurableObjectStateLike,
  env: unknown
): DurableObjectNamespaceLike | undefined {
  const id = ctx.id?.toString();
  if (!id) return undefined;
  for (const bindings of [ctx.exports, env]) {
    if (typeof bindings !== "object" || bindings === null) continue;
    for (const value of Object.values(bindings)) {
      if (!isNamespace(value)) continue;
      try {
        value.idFromString(id);
      } catch {
        continue;
      }
      // An object in a jurisdiction runs its deliveries in the same one.
      const { jurisdiction } = ctx.id!;
      return jurisdiction && value.jurisdiction ? value.jurisdiction(jurisdiction) : value;
    }
  }
  return undefined;
}

export interface WorkflowObjectInstance<Router extends WorkflowRouter> {
  /**
   * The host: `durable.runtime` for the local SDK, stores and effects, and
   * `durable.invoker`. The first read creates the runtime's tables.
   */
  readonly durable: ReturnType<typeof createWorkflowObject<Router>>;
  /** Serves the trigger, poll, wait, list, events and cancel routes. */
  fetch(request: Request): Promise<Response>;
  /** Claims the executions that are due. */
  alarm(): Promise<void>;
}

export type WorkflowObjectClass<Router extends WorkflowRouter> = new (
  ctx: DurableObjectStateLike,
  env: any
) => DurableObject & WorkflowObjectInstance<Router>;

/**
 * Returns a Durable Object class that runs workflows. Export it under the
 * `class_name` that `wrangler.jsonc` binds:
 *
 * ```ts
 * export class Workflows extends WorkflowObject({ router }) {}
 * ```
 *
 * The class serves the HTTP routes from `fetch`, resumes executions from
 * `alarm`, and exposes the host as `durable`, such as `this.durable.runtime`
 * for the local SDK.
 *
 * The object that receives requests claims executions, and runs each delivery
 * in another instance of the same class. It keeps a pool of these instances,
 * one for each delivery that runs at once, and names them after its own ID.
 * Each instance has its own isolate and runs one delivery at a time, so a
 * step that exceeds the CPU limit resets only its own delivery. An instance
 * that runs a delivery writes nothing to its own storage: every step it
 * records goes to the object that claimed the delivery.
 */
export function WorkflowObject<Router extends WorkflowRouter>(
  options: WorkflowObjectOptions<Router>
): WorkflowObjectClass<Router> {
  const { isolateDeliveries = true, ...hostOptions } = options;

  class WorkflowObject extends DurableObject implements DeliveryRunner {
    #host: ReturnType<typeof createWorkflowObject<Router>> | undefined;
    #delivering = false;

    // Created on first use, so that an instance that only runs a delivery
    // never creates the runtime's tables.
    get durable() {
      return (this.#host ??= createHost(this.ctx, hostOptions, this.#createDeliver()));
    }

    #createDeliver() {
      if (!isolateDeliveries) return undefined;
      const namespace = findOwnNamespace(this.ctx, this.env);
      if (!namespace) {
        throw new Error(
          "WorkflowObject could not find the Durable Object namespace of its own class, " +
            "which it needs to run each delivery in its own instance. Bind the class under " +
            "durable_objects.bindings in the Worker that exports it, or pass " +
            "isolateDeliveries: false to run deliveries in the object's own isolate."
        );
      }
      const id = this.ctx.id!.toString();
      return (runtime: Runtime) =>
        createIsolatedDeliver({
          runtime,
          // A stub stays broken after its instance aborts, so each call gets one.
          getRunner: (slot) =>
            namespace.get(namespace.idFromName(`${id}/delivery/${slot}`)) as DeliveryRunner,
        });
    }

    fetch(request: Request) {
      return this.durable.fetch(request);
    }

    alarm() {
      return this.durable.alarm();
    }

    async durableRunDelivery(
      request: DeliveryRequest,
      call: RuntimeCall
    ): Promise<DeliveryReply> {
      this.#delivering = true;
      try {
        return await runDelivery(request, call, {
          router: options.router,
          logger: options.logger ?? consoleLogger,
        });
      } finally {
        this.#delivering = false;
      }
    }

    async durableStopDelivery() {
      if (this.#delivering) this.ctx.abort?.("The delivery was stopped");
    }
  }

  return WorkflowObject as unknown as WorkflowObjectClass<Router>;
}
