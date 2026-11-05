# HTTP Server

The HTTP server exposes a runtime's execution records through POST endpoints, so clients can admit executions and read them back over the network. Its handlers use standard `Request` and `Response` types. The example uses `Bun.serve`, but the routes object can plug into any Request-to-Response router.

## Server

```ts [server.ts]
import pino from "pino";
import { ExecutionDispatcher } from "@yieldstar/core";
import { createRoutes } from "@yieldstar/http-server";
import { createWorkflowInvoker } from "@yieldstar/worker-invoker";
import { runtime } from "./shared";

const logger = pino();
const workerPath = new URL("./worker.ts", import.meta.url).href;
const invoker = createWorkflowInvoker({ workerPath, logger });

const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger });
dispatcher.start();

Bun.serve({
  port: 8080,
  routes: {
    "/status": new Response("OK"),
    ...createRoutes({ runtime, invoker, logger }),
  },
});
```

`runtime` is the `SqliteRuntime` from the [quick start](./quickstart.md)'s `shared.ts`, and the worker is the same `worker.ts`. The routes only read and write execution records; the dispatcher is what delivers admitted executions to workers. `invoker` is optional. When the dispatcher runs in the same process, it lets `/wait` and `/events` respond as soon as the dispatcher records an outcome, instead of at their next read of the record. No response depends on it.

## Client

```ts
import { createHttpSdkFactory } from "yieldstar";
import type { Router } from "./shared";

const createSdk = createHttpSdkFactory<Router>();
const sdk = createSdk({ url: "http://localhost:8080" });

const exec = await sdk.trigger({
  workflowId: "my-workflow",
});
await exec.ack();
const result = await exec.waitForResult();
```

`waitForResult` posts to `/wait`. See [HTTP SDK](./sdk-http.md) for `poll`, `wait` and `list`.

## Routes

`createRoutes` registers five POST endpoints:

| Endpoint        | Body                                                           | Responses                                                                                                     |
| --------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `POST /trigger` | `{ workflowId, params?, context?, executionId?, idempotent? }` | `202 { executionId, status: "accepted" }` once admission commits; `200 { executionId, status: "existing" }` for an idempotent repeat; `409` `EXECUTION_ID_CONFLICT` or `EXECUTION_REQUEST_MISMATCH`; `400` `INVALID_EXECUTION_EVENT` or `IDEMPOTENT_TRIGGER_REQUIRES_ID` |
| `POST /poll`    | `{ executionId }`                                              | `200` with the execution's view; `404` `EXECUTION_NOT_FOUND`                                                  |
| `POST /wait`    | `{ executionId, timeout? }`                                    | `200` with a `WaitResult`; `404` `EXECUTION_NOT_FOUND`                                                        |
| `POST /list`    | `{ workflowId?, status?, cursor?, limit? }`                    | `200 { executions, cursor? }`                                                                                 |
| `POST /events`  | `{ executionId }`                                              | Deprecated. Blocks until the record is terminal. `200` with the encoded result, `500` with the encoded recorded error, `404` `EXECUTION_NOT_FOUND` |

The views, wait results and pages are the same as the [local SDK](./sdk-local.md#execution-views) returns. A failed execution is a `200` response from `/poll` and `/wait`, with the error in the view.

`/wait` holds the request open for at most 8 seconds by default, below Bun's default 10-second idle timeout; the `maxWait` option of `createRoutes` changes it. `maxWait` and `pollInterval` must be a finite number greater than zero, because the HTTP SDK repeats a capped request at once and a wait re-reads the record after each `pollInterval`; `createRoutes` throws a `RangeError` otherwise. When the execution is not terminal by then, or by `timeout` if that is shorter, it responds `200` with `{ status: "timed-out", execution }`. The [HTTP SDK](./sdk-http.md) repeats the request until the execution is terminal. `/events` has no cap; use `/wait` instead. When a client disconnects, its `/wait` or `/events` request stops reading the record.

Each open `/wait` or `/events` request reads the execution record every `pollInterval` milliseconds (25 by default). The requests do not share reads, so many concurrent waits multiply the load on the backend. Raise `pollInterval` or lower `maxWait` in `createRoutes` if many clients wait at once. Passing `invoker` lets a wait respond as soon as the dispatcher in the same process records an outcome, so a longer `pollInterval` costs less latency.

Every route reads its body with the durable value codec, and writes views, results and recorded errors with it. Plain JSON is a valid encoding, so any HTTP client can call the routes, while the HTTP SDK can also send `undefined` values, `Date`s, `Map`s, `Set`s, `bigint`s and errors, which the codec writes as objects tagged with a `$type` key. A plain object with its own `$type` key must be sent wrapped as `{ "$type": "object", "value": ... }`.

`/trigger` responds before any workflow code runs. When the body omits `executionId`, the runtime chooses one and the response carries it.

### Errors

Error responses carry `{ code, message }`. Besides the codes in the table above, the routes respond:

| Condition                                                    | Response                                |
| ------------------------------------------------------------ | --------------------------------------- |
| A `/poll`, `/wait`, `/list` or `/events` body that is not a JSON object, a missing or empty `executionId`, or a `timeout` that is negative or not a number | `400` `INVALID_OBSERVATION_REQUEST` |
| A `/list` `workflowId` that is not a string, a `limit` that is not a positive integer, an unknown `status`, or a malformed `cursor` | `400` `INVALID_LIST_REQUEST` |
| Observation middleware denies the request                    | `401` `UNAUTHORIZED` or `403` `FORBIDDEN` |
| Middleware throws, or the runtime fails                      | `500` `INTERNAL_ERROR`                  |

A `500` response always has the message `Internal error`. The server logs the thrown error with `logger.error` and does not send its message, so middleware and database details stay on the server.

A `/trigger` body that is not a valid trigger event, including an `idempotent` value that is not a boolean, responds `400` `INVALID_EXECUTION_EVENT`. A `/list` `limit` above 1000 is lowered to 1000.

An optional `basePath` prefixes every route:

```ts
createRoutes({ runtime, logger, basePath: "/api/workflows" });
// → POST /api/workflows/trigger
// → POST /api/workflows/poll
// → POST /api/workflows/wait
// → POST /api/workflows/list
// → POST /api/workflows/events
```

## Middleware

Trigger middleware functions run before the execution is admitted and can inspect the request, modify the event context, or short-circuit with an early response:

```ts
import { createMiddleware } from "@yieldstar/http-server";

const auth = createMiddleware(async (req, event, next, logger) => {
  const token = req.headers.get("Authorization");
  if (!token) return new Response("Unauthorized", { status: 401 });
  event.context.set("userId", parseToken(token).sub);
  return next();
});

createRoutes({ runtime, logger, middleware: [auth] });
```

Each middleware receives four arguments: `req` (the incoming `Request`), `event` (a `MiddlewareEvent` whose `context` map is mutable at this stage, and whose `executionId` is `undefined` when the client lets the runtime choose one), `next` (a function that forwards to the next middleware or the final handler), and `logger`.

Once every middleware has run, the context map is frozen and admitted with the event. Every delivery reads it back from the execution record as a read-only `Map` available through `event.context`, so context values must be durable values (see [Step values](./steps.md#step-values)).

Trigger middleware does not run for the observation routes.

## Observation middleware

`/poll`, `/wait`, `/list` and `/events` return results and errors, which are often derived from `params`, to anyone who can call them. Without observation middleware, any caller that can reach the server can read every execution. Configure observation middleware to decide who may read them:

```ts
import type { ObservationMiddlewareFunction } from "@yieldstar/http-server";

const tenantOnly: ObservationMiddlewareFunction = async (req, request, logger) => {
  const tenant = tenantFromToken(req.headers.get("Authorization"));
  if (!tenant) return { allow: false, status: 401 };
  // A list filter has no tenant, so this policy does not allow lists.
  if (request.operation === "list") return { allow: false, status: 403 };
  const record = await request.load();
  // An unknown ID is denied too, so a caller cannot probe for other tenants' IDs.
  if (record?.params?.tenant !== tenant) return { allow: false, status: 403 };
  return { allow: true };
};

createRoutes({ runtime, logger, observationMiddleware: [tenantOnly] });
```

Each function receives three arguments: `req` (the incoming `Request`), `request` (an `ObservationRequest` that describes what the caller asks to read) and `logger`. `ObservationRequest` is one of:

```ts
type ObservationRequest =
  | {
      operation: "poll" | "wait" | "events";
      executionId: string;
      load(): Promise<ExecutionRecord | undefined>;
    }
  | {
      operation: "list";
      filter: { workflowId?: string; status?: ExecutionStatus };
    };
```

Each function resolves with `{ allow: true }` or `{ allow: false, status: 401 | 403 }`.

- The functions run in array order, before the route reads any record. The first denial wins, and the later functions do not run.
- A denial responds `401` `UNAUTHORIZED` or `403` `FORBIDDEN`, never `404`. So a denied caller cannot tell whether an execution exists.
- `load` reads the full execution record, including `params` and `context`, for a policy that needs them. It resolves `undefined` for an unknown ID. A policy that denies only when the record exists reveals whether it exists. The example above denies an unknown ID as well, so it reveals nothing.
- A `list` request carries its filter and no `load`.
- A function that throws responds `500` `INTERNAL_ERROR`.
- Observation middleware does not run for `/trigger`.

`/trigger` also reveals something about existing executions. A caller that trigger middleware allows can send `idempotent: true` with another execution's ID and a guess at its `params`. The response is `200` `existing` when the guess matches and `409` `EXECUTION_REQUEST_MISMATCH` when it does not, so the caller can test guesses at that execution's `params`. Any trigger with a used ID also shows that the ID exists. Observation middleware cannot prevent this. Limit who may trigger with trigger middleware, or use execution IDs that other callers cannot guess.

This package does not choose an authorization policy.
