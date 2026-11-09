import type { Logger } from "pino";
import type {
  ExecutionAdmissionErrorCode,
  ExecutionClient,
  ExecutionControl,
  ExecutionListRequest,
  ExecutionObserver,
  ExecutionRecord,
  ExecutionStatus,
  WorkflowInvoker,
} from "@durable/core";
import {
  ExecutionListRequestError,
  FreezableMap,
  MiddlewareEvent,
  MiddlewareFunction,
  WorkflowCancelledError,
  createExecutionControl,
  createExecutionObserver,
  durableValueCodec,
  encodeThrownValue,
  isDuration,
  isPositiveInterval,
  prepareExecutionCancel,
  prepareExecutionListRequest,
} from "@durable/core";
import { executeMiddlewareChain } from "./middleware";

export type RouteHandler = (request: Request) => Response | Promise<Response>;
export type Routes = Record<string, { POST: RouteHandler }>;

/** Below Bun's default 10-second idle timeout. */
const DEFAULT_MAX_WAIT = 8_000;

export type ObservationErrorCode =
  | "EXECUTION_NOT_FOUND"
  | "INVALID_OBSERVATION_REQUEST"
  | "INVALID_LIST_REQUEST"
  | "INVALID_CANCEL_REQUEST"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "INTERNAL_ERROR";

/** What an observation route or `/cancel` passes to observation middleware. */
export type ObservationRequest =
  | {
      operation: "poll" | "wait" | "events";
      executionId: string;
      /**
       * Reads the record. It resolves `undefined` for an unknown ID, so a
       * policy that calls it can reveal whether the execution exists.
       */
      load(): Promise<ExecutionRecord | undefined>;
    }
  | {
      operation: "list";
      filter: { workflowId?: string; status?: ExecutionStatus };
    }
  | {
      operation: "cancel";
      executionId: string;
      force: boolean;
      /**
       * Reads the record. It resolves `undefined` for an unknown ID, so a
       * policy that calls it can reveal whether the execution exists.
       */
      load(): Promise<ExecutionRecord | undefined>;
    };

/**
 * Allows or denies an observation or cancel request. It runs before the
 * route reads any record. A denial responds `401` or `403`, never `404`.
 */
export type ObservationMiddlewareFunction = (
  req: Request,
  request: ObservationRequest,
  logger: Logger
) => Promise<{ allow: true } | { allow: false; status: 401 | 403 }>;

type TriggerBody = {
  executionId?: string;
  workflowId: string;
  params?: unknown;
  context?: Record<string, unknown>;
  idempotent?: unknown;
};

const admissionStatus: Record<ExecutionAdmissionErrorCode, number> = {
  EXECUTION_ID_CONFLICT: 409,
  EXECUTION_REQUEST_MISMATCH: 409,
  INVALID_EXECUTION_EVENT: 400,
  IDEMPOTENT_TRIGGER_REQUIRES_ID: 400,
};

/**
 * Admits an execution. The body is decoded with the durable value codec, so
 * plain JSON is accepted and the HTTP SDK's tagged values (such as `undefined`
 * and errors) keep their shape. A dispatcher delivers the execution later.
 *
 * Responses:
 * - `202` with `{ executionId, status: "accepted" }`.
 * - `200` with `status: "existing"` when an idempotent repeat matches.
 * - `409` when the ID is in use, or in idempotent mode is in use by a
 *   different request.
 * - `400` when the event cannot be stored without loss, or an idempotent
 *   trigger has no ID.
 */
export const createTriggerHandler =
  (params: {
    executions: ExecutionClient;
    logger: Logger;
    middleware?: MiddlewareFunction[];
  }): RouteHandler =>
  async (req) => {
    const { logger, executions, middleware = [] } = params;

    const decoding = durableValueCodec.decode(await req.text());
    if (decoding.status === "invalid" || !isTriggerBody(decoding.value)) {
      return errorResponse(
        400,
        "INVALID_EXECUTION_EVENT",
        decoding.status === "invalid"
          ? `Invalid trigger body: ${decoding.message}`
          : "Invalid trigger body: expected { workflowId, executionId?, params?, context?, idempotent? }"
      );
    }
    const body = decoding.value;

    const event: MiddlewareEvent = {
      executionId: body.executionId,
      workflowId: body.workflowId,
      params: body.params as MiddlewareEvent["params"],
      context: new FreezableMap<string, any>(Object.entries(body.context ?? {})),
    };

    try {
      return await executeMiddlewareChain(
        req,
        event,
        logger,
        middleware,
        async (req, event) => {
          const admission = await executions.admit({
            executionId: event.executionId,
            workflowId: event.workflowId,
            params: event.params,
            context: event.context,
            idempotent: body.idempotent as boolean | undefined,
          });
          if (admission.status !== "rejected") {
            return Response.json(
              { executionId: admission.executionId, status: admission.status },
              { status: admission.status === "accepted" ? 202 : 200 }
            );
          }
          return errorResponse(
            admissionStatus[admission.code],
            admission.code,
            admission.message
          );
        }
      );
    } catch (err) {
      return internalError(logger, err);
    }
  };

type ObservationParams = {
  executions: ExecutionClient;
  observer: ExecutionObserver;
  control: ExecutionControl;
  logger: Logger;
  observationMiddleware: ObservationMiddlewareFunction[];
  maxWait: number;
};

/**
 * Reads an observation body, runs observation middleware, and then calls
 * `handle`. `parse` returns the request to authorize, or an error response.
 * A body that is not a JSON object responds `400` with `invalidBodyCode`.
 */
function observationRoute<T>(
  params: ObservationParams,
  parse: (body: Record<string, unknown>) => { request: ObservationRequest; value: T } | Response,
  handle: (req: Request, value: T) => Promise<Response>,
  invalidBodyCode: ObservationErrorCode = "INVALID_OBSERVATION_REQUEST"
): RouteHandler {
  return async (req) => {
    const { logger } = params;
    try {
      const decoding = durableValueCodec.decode(await req.text());
      if (decoding.status === "invalid" || !isDecodedObject(decoding.value)) {
        return errorResponse(400, invalidBodyCode, "The body must be a JSON object");
      }
      const parsed = parse(decoding.value);
      if (parsed instanceof Response) return parsed;
      for (const middleware of params.observationMiddleware) {
        const decision = await middleware(req, parsed.request, logger);
        if (decision?.allow === true) continue;
        return decision?.status === 401
          ? errorResponse(401, "UNAUTHORIZED", "Authentication required")
          : errorResponse(403, "FORBIDDEN", "Request denied");
      }
      return await handle(req, parsed.value);
    } catch (err) {
      return internalError(logger, err, req.signal.aborted);
    }
  };
}

function parseExecutionRequest(
  params: ObservationParams,
  operation: "poll" | "wait" | "events",
  body: Record<string, unknown>
) {
  const { executionId } = body;
  if (typeof executionId !== "string" || executionId === "") {
    return invalidObservationRequest("executionId must be a non-empty string");
  }
  const request: ObservationRequest = {
    operation,
    executionId,
    load: () => params.executions.get(executionId),
  };
  return { request, value: executionId };
}

/** `{ executionId }`: `200` with an `ExecutionView`, or `404`. */
export const createPollHandler = (params: ObservationParams): RouteHandler =>
  observationRoute(
    params,
    (body) => parseExecutionRequest(params, "poll", body),
    async (_req, executionId) => {
      const view = await params.observer.poll(executionId);
      return view ? durableResponse(view, 200) : executionNotFound(executionId);
    }
  );

/**
 * `{ executionId, timeout? }`: `200` with a `WaitResult`, or `404`. The wait
 * is capped at `maxWait`, and a capped wait returns `timed-out`. The
 * request's abort signal ends the wait.
 */
export const createWaitHandler = (params: ObservationParams): RouteHandler =>
  observationRoute(
    params,
    (body) => {
      const { timeout } = body;
      if (timeout !== undefined && !isDuration(timeout)) {
        return invalidObservationRequest(
          "timeout must be a non-negative number of milliseconds"
        );
      }
      const parsed = parseExecutionRequest(params, "wait", body);
      if (parsed instanceof Response) return parsed;
      return {
        request: parsed.request,
        value: {
          executionId: parsed.value,
          timeout: Math.min(timeout ?? Infinity, params.maxWait),
        },
      };
    },
    async (req, { executionId, timeout }) => {
      const result = await params.observer.wait(executionId, {
        timeout,
        signal: req.signal,
      });
      return result ? durableResponse(result, 200) : executionNotFound(executionId);
    }
  );

/** `{ workflowId?, status?, cursor?, limit? }`: `200` with a page. */
export const createListHandler = (params: ObservationParams): RouteHandler =>
  observationRoute(
    params,
    (body) => {
      const request = body as ExecutionListRequest;
      try {
        const { workflowId, status } = prepareExecutionListRequest(request);
        return { request: { operation: "list", filter: { workflowId, status } }, value: request };
      } catch (err) {
        if (!(err instanceof ExecutionListRequestError)) throw err;
        return errorResponse(400, "INVALID_LIST_REQUEST", err.message);
      }
    },
    async (_req, request) => durableResponse(await params.observer.list(request), 200)
  );

/**
 * Deprecated: use `/wait`. `{ executionId }`: waits, without a timeout, until
 * the record is terminal. Responds `200` with the result, or `500` with the
 * recorded error, each encoded with the durable value codec, or `404` for an
 * unknown ID. The request's abort signal ends the wait.
 */
export const createEventsHandler = (params: ObservationParams): RouteHandler =>
  observationRoute(
    params,
    (body) => parseExecutionRequest(params, "events", body),
    async (req, executionId) => {
      const outcome = await params.observer.wait(executionId, { signal: req.signal });
      if (!outcome) return executionNotFound(executionId);
      if (outcome.status === "failed") {
        return durableJson(encodeThrownValue(outcome.error), 500);
      }
      if (outcome.status === "cancelled") {
        const error = new WorkflowCancelledError({
          reason: outcome.reason,
          source: outcome.source,
        });
        return durableJson(encodeThrownValue(error), 500);
      }
      if (outcome.status === "timed-out") {
        throw new Error(`Execution "${executionId}" is still ${outcome.execution.status}`);
      }
      const result = durableValueCodec.encode(outcome.result);
      if (result.status === "invalid") {
        return durableJson(encodeThrownValue(new Error(result.message)), 500);
      }
      return durableJson(result.canonical, 200);
    }
  );

/**
 * `{ executionId, reason?, force? }`. Responds `202` while the execution is
 * active, `200` with its terminal view, `404` for an unknown ID and `400` for
 * a malformed body.
 */
export const createCancelHandler = (params: ObservationParams): RouteHandler =>
  observationRoute(
    params,
    (body) => {
      const prepared = prepareExecutionCancel(
        body as { executionId: string; reason?: string; force?: boolean }
      );
      if (!prepared.ok) {
        return errorResponse(400, "INVALID_CANCEL_REQUEST", prepared.message);
      }
      const { executionId } = prepared;
      const request: ObservationRequest = {
        operation: "cancel",
        executionId,
        force: prepared.request.force,
        load: () => params.executions.get(executionId),
      };
      return { request, value: { executionId, options: prepared.request } };
    },
    async (_req, { executionId, options }) => {
      const result = await params.control.cancel(executionId, options);
      if (!result) return executionNotFound(executionId);
      return durableResponse(result, result.status === "requested" ? 202 : 200);
    },
    "INVALID_CANCEL_REQUEST"
  );

function errorResponse(
  status: number,
  code: ObservationErrorCode | ExecutionAdmissionErrorCode,
  message: string
) {
  return Response.json({ code, message }, { status });
}

function invalidObservationRequest(message: string) {
  return errorResponse(400, "INVALID_OBSERVATION_REQUEST", message);
}

function executionNotFound(executionId: string) {
  return errorResponse(404, "EXECUTION_NOT_FOUND", `Execution "${executionId}" does not exist`);
}

/**
 * Logs the error and responds with a fixed message, so the response does not
 * reveal middleware or database internals.
 */
function internalError(logger: Logger, err: unknown, aborted = false) {
  // A wait ended by the client's disconnect is not a server error, and no one
  // reads the response.
  if (!aborted) logger.error(err);
  return errorResponse(500, "INTERNAL_ERROR", "Internal error");
}

/** Encodes `value` with the durable value codec, so results and errors keep their shape. */
function durableResponse(value: unknown, status: number) {
  const encoding = durableValueCodec.encode(value);
  if (encoding.status === "invalid") {
    throw new Error(`Response cannot be encoded: ${encoding.message}`);
  }
  return durableJson(encoding.canonical, status);
}

function durableJson(canonical: string, status: number) {
  return new Response(canonical, {
    status,
    headers: { "content-type": "application/json" },
  });
}

function isDecodedObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof Error) &&
    !(value instanceof Date) &&
    !(value instanceof Map) &&
    !(value instanceof Set)
  );
}

function isTriggerBody(value: unknown): value is TriggerBody {
  if (!isDecodedObject(value) || typeof value.workflowId !== "string") {
    return false;
  }
  if (value.executionId !== undefined && typeof value.executionId !== "string") {
    return false;
  }
  return value.context === undefined || isDecodedObject(value.context);
}

export function createRoutes(params: {
  basePath?: `/${string}`;
  runtime: { executions: ExecutionClient };
  /**
   * Optional. When the dispatcher runs in this process, waits listen to its
   * `workflowEndEmitter` and respond as soon as an outcome is recorded.
   * Results do not depend on it.
   */
  invoker?: Pick<WorkflowInvoker, "workflowEndEmitter">;
  logger: Logger;
  /** Runs before admission on `/trigger`. It does not apply to other routes. */
  middleware?: MiddlewareFunction[];
  /**
   * Runs, in array order, before `/poll`, `/wait`, `/list`, `/events` and
   * `/cancel` read any record. The first denial ends the request.
   */
  observationMiddleware?: ObservationMiddlewareFunction[];
  /**
   * How often each open `/wait` and `/events` request reads the record, in
   * milliseconds. It must be greater than zero.
   * @default 25
   */
  pollInterval?: number;
  /**
   * The longest the server holds a `/wait` request open, in milliseconds. A
   * request that reaches it responds `timed-out`, and the HTTP SDK repeats
   * it. Keep it below the server's idle timeout (10 seconds by default in
   * Bun). It must be greater than zero.
   * @default 8000
   */
  maxWait?: number;
}): Routes {
  const { logger, runtime, invoker, middleware = [], basePath = "" } = params;
  // A deployment that already guards /trigger gains open observation routes on
  // upgrade, including /list, which returns every execution's outcome.
  if (middleware.length > 0 && params.observationMiddleware === undefined) {
    logger.warn(
      "createRoutes has trigger middleware but no observationMiddleware, so /poll, /wait, /list, /events and /cancel accept every request. Pass observationMiddleware, or [] to allow every request on purpose."
    );
  }
  const { executions } = runtime;
  const maxWait = params.maxWait ?? DEFAULT_MAX_WAIT;
  if (!isPositiveInterval(maxWait)) {
    throw new RangeError("maxWait must be a positive, finite number of milliseconds");
  }
  const observation: ObservationParams = {
    executions,
    observer: createExecutionObserver(executions, {
      notifier: invoker?.workflowEndEmitter,
      pollInterval: params.pollInterval,
    }),
    control: createExecutionControl(executions),
    logger,
    observationMiddleware: params.observationMiddleware ?? [],
    maxWait,
  };
  return {
    [`${basePath}/trigger`]: {
      POST: createTriggerHandler({ executions, logger, middleware }),
    },
    [`${basePath}/poll`]: { POST: createPollHandler(observation) },
    [`${basePath}/wait`]: { POST: createWaitHandler(observation) },
    [`${basePath}/list`]: { POST: createListHandler(observation) },
    [`${basePath}/events`]: { POST: createEventsHandler(observation) },
    [`${basePath}/cancel`]: { POST: createCancelHandler(observation) },
  };
}
