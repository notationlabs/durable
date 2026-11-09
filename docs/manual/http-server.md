# HTTP server

`@notation/durable/http` serves the SDK's operations as HTTP routes. Code that cannot reach the runtime directly, such as an app server on another host, uses the routes to start executions, read them and cancel them, through the [HTTP SDK](./sdk-http.md).

This page serves the routes to the web app of a reports product, which runs on another host. The app calls the routes on behalf of signed-in users, with each user's session token. A user can start an export, and read and cancel only their own:

```ts [src/durable/server.ts]
import pino from "pino";
import { createRoutes } from "@notation/durable/http";
import { dispatcher } from "./client";
import { ownExecutionsOnly, requireUser } from "./auth";
import { runtime } from "./runtime";

const logger = pino();

dispatcher.start();

Bun.serve({
  port: 8080,
  routes: createRoutes({
    basePath: "/durable",
    runtime,
    logger,
    middleware: [requireUser],
    observationMiddleware: [ownExecutionsOnly],
  }),
});
```

`runtime` and `dispatcher` are from the [Quick start](./quickstart.md). `requireUser` and `ownExecutionsOnly` are shown below, in [Trigger middleware](#trigger-middleware) and [Observation middleware](#observation-middleware).

## Routes

`createRoutes` returns an object that maps each path to a handler for `POST`. A handler takes a standard `Request` and returns a `Response`, so the object works with `Bun.serve` as above, and with any router that takes `Request` handlers.

| Route | Does | HTTP SDK method |
| --- | --- | --- |
| `POST /durable/trigger` | Records a new execution | `trigger` |
| `POST /durable/poll` | Returns an execution's view | `poll` |
| `POST /durable/wait` | Waits for an execution to end | `wait` |
| `POST /durable/list` | Returns a page of executions | `list` |
| `POST /durable/cancel` | Records a cancellation request | `cancel` |
| `POST /durable/events` | Deprecated. Use `/wait` | None |

Each route does what the [local SDK](./sdk-local.md) method of the same name does. Without `basePath`, the routes are `/trigger`, `/poll` and so on.

**The routes do not run workflow code.** They read and write the runtime's records. A dispatcher that uses the same runtime delivers the executions that `/trigger` records. The example starts it in the same process.

## Options

| Option | Default | Does |
| --- | --- | --- |
| `runtime` | Required | The runtime whose records the routes read and write |
| `logger` | Required | A pino logger |
| `basePath` | `""` | A prefix for every route, starting with `/` |
| `middleware` | `[]` | Functions that run before `/trigger` records an execution |
| `observationMiddleware` | `[]` | Functions that allow or deny the other routes |
| `maxWait` | `8000` | The longest, in milliseconds, that `/wait` holds a request open |
| `pollInterval` | `25` | Milliseconds between reads of the record while `/wait` waits |
| `invoker` | None | Lets `/wait` respond as soon as the dispatcher in this process records an outcome |

`createRoutes` throws a `RangeError` if `maxWait` or `pollInterval` is not a number greater than zero.

## Request and response bodies

Each route takes a JSON object with the fields of the SDK method's arguments:

| Route | Body |
| --- | --- |
| `/trigger` | `{ workflowId, params?, context?, executionId?, idempotent? }` |
| `/poll` | `{ executionId }` |
| `/wait` | `{ executionId, timeout? }` |
| `/list` | `{ workflowId?, status?, limit?, cursor? }` |
| `/cancel` | `{ executionId, reason?, force? }` |

A response body holds what the SDK method returns, such as `{ executionId, status }` from `/trigger` or a [view](./sdk-local.md#reading-an-execution) from `/poll`.

Bodies are JSON, written by Durable's codec for [durable values](./step-values.md). The codec writes values that JSON cannot hold, such as a `Date`, a `Map`, `undefined` or an error, as objects with a `$type` key.

So a failed execution's error reaches the HTTP SDK with its class, `code` and `cause`. Plain JSON is valid input, so any HTTP client can call the routes.

## Status codes

| Route | Success | Other responses |
| --- | --- | --- |
| `/trigger` | `202` for a new execution. `200` for an idempotent repeat that returns `"existing"` | `409` for `EXECUTION_ID_CONFLICT` and `EXECUTION_REQUEST_MISMATCH`. `400` for `INVALID_EXECUTION_EVENT` and `IDEMPOTENT_TRIGGER_REQUIRES_ID` |
| `/poll`, `/wait` | `200` | `404` `EXECUTION_NOT_FOUND` |
| `/list` | `200` | `400` `INVALID_LIST_REQUEST` |
| `/cancel` | `202` while the execution has not ended. `200` with the view once it has ended, including after a forced cancel | `404` `EXECUTION_NOT_FOUND`. `400` `INVALID_CANCEL_REQUEST` |

A failed or cancelled execution is a `200` from `/poll` and `/wait`. The view holds the error, or the cancellation's `reason`.

Error responses have the body `{ code, message }`. These codes can come from any route:

| Response | When |
| --- | --- |
| `400` `INVALID_OBSERVATION_REQUEST` | A `/poll`, `/wait`, `/list` or `/events` body is not a JSON object. Or a `/poll` or `/wait` body has no `executionId`, or has a `timeout` that is negative or not a number |
| `401` `UNAUTHORIZED`, `403` `FORBIDDEN` | Observation middleware denied the request |
| `500` `INTERNAL_ERROR` | Middleware threw, or the runtime failed |

A `500` response always has the message `Internal error`. The server logs the error with `logger.error`, and does not send it, so database and middleware details stay on the server.

## Long waits

`/wait` holds the request open until the execution ends, or until `maxWait` passes, whichever comes first. If `maxWait` passes first, it responds `200` with `{ status: "timed-out", execution }`. The HTTP SDK then sends the request again, so its `wait` works with timeouts longer than `maxWait`.

The default `maxWait` of 8 seconds is below Bun's default idle timeout of 10 seconds. Keep `maxWait` below the idle timeout of the server and of any proxy in front of it, or they close the connection before `/wait` responds.

Each open `/wait` request reads the record every `pollInterval` milliseconds. Open requests do not share reads, so many waiting clients add load to the database. If many clients wait at once, raise `pollInterval`, and pass `invoker` so that a wait still responds as soon as the dispatcher in this process records an outcome.

## Trigger middleware

Trigger middleware runs on `/trigger`, before the execution is recorded. `requireUser` rejects requests without a signed-in user, and records the user's ID in the execution's context:

```ts [src/durable/auth.ts]
import { createMiddleware } from "@notation/durable/http";
import { userFromRequest } from "../auth";

export const requireUser = createMiddleware(async (req, event, next) => {
  const userId = await userFromRequest(req);

  if (!userId) {
    return Response.json({ code: "UNAUTHORIZED", message: "Sign in first" }, { status: 401 });
  }

  event.context.clear();
  event.context.set("userId", userId);

  return next();
});
```

`userFromRequest` is the application's own code. It reads the session token from the `Authorization` header, and returns the user's ID or `undefined`.

A middleware function receives these arguments:

| Argument | Holds |
| --- | --- |
| `req` | The `Request` |
| `event` | The execution to record: `workflowId`, `params`, `executionId` and a `context` map that the middleware can change |
| `next` | A function that calls the next middleware, or records the execution after the last one. It returns the response |
| `logger` | The logger passed to `createRoutes` |

A middleware function that returns its own response, without calling `next`, stops the request, and nothing is recorded. A function that calls `next` can also change the response before it returns it, for example to add headers.

The workflow reads the context as `event.context`, so it can read the user's ID with `event.context.get("userId")`. Context values must be [durable values](./step-values.md).

**The context starts with the values that the client sent.** A client can send `context: { userId: "user-admin" }` in the body, and without middleware that changes it, the workflow receives that value. `requireUser` calls `event.context.clear()` first, so the workflow receives only the values that the middleware set.

## Observation middleware

**Without observation middleware, anyone who can reach the server can read and cancel every execution.** `/list` returns every execution, with its result or error. Observation middleware decides who can use `/poll`, `/wait`, `/list`, `/cancel` and `/events`.

`ownExecutionsOnly` allows a user to read and cancel only the executions they started. It compares the user with the `userId` that `requireUser` recorded:

```ts [src/durable/auth.ts]
import type { ObservationMiddlewareFunction } from "@notation/durable/http";

export const ownExecutionsOnly: ObservationMiddlewareFunction = async (req, request) => {
  const userId = await userFromRequest(req);

  if (!userId) {
    return { allow: false, status: 401 };
  }

  if (request.operation === "list") {
    return { allow: false, status: 403 };
  }

  const record = await request.load();

  if (record?.context.get("userId") !== userId) {
    return { allow: false, status: 403 };
  }

  return { allow: true };
};
```

A list filter does not name a user, so this policy denies `/list`.

The function receives the `Request`, a description of the request, and the logger. It returns `{ allow: true }`, or `{ allow: false, status }` with a status of `401` or `403`. The description depends on the route:

| `request.operation` | Other fields |
| --- | --- |
| `"poll"`, `"wait"`, `"events"` | `executionId`, and `load()` |
| `"cancel"` | `executionId`, `force`, and `load()` |
| `"list"` | `filter`, with `workflowId` and `status` |

`load()` reads the whole execution record, including `params` and `context`, or returns `undefined` if no execution has the ID.

Observation middleware follows these rules:

- The functions run in array order, before the route reads anything. The first denial ends the request.
- A denial responds `401` `UNAUTHORIZED` or `403` `FORBIDDEN`, never `404`.
- A function that throws responds `500` `INTERNAL_ERROR`.
- The same functions decide reads and cancels. A policy that allows reads but not cancels checks `request.operation`.

**A policy that denies only existing executions shows which IDs exist.** If `ownExecutionsOnly` allowed requests when `load()` returned `undefined`, a caller would get `404` for an unknown ID and `403` for another user's execution. `ownExecutionsOnly` denies both with `403`.

If `createRoutes` has trigger middleware but no `observationMiddleware`, it logs a warning. To allow every request on purpose, pass `observationMiddleware: []`.

## What `/trigger` shows

`/trigger` can show that an execution exists, and observation middleware does not run on it:

- A trigger with an ID that is in use responds `409`, so the caller learns that the ID exists.
- An idempotent trigger with another user's execution ID and a guess at its `params` responds `200` if the guess is right, and `409` if it is wrong.

To prevent this, limit who can trigger with trigger middleware, or let the server choose execution IDs, which are random UUIDs.

## Order of checks

The routes check the body before middleware runs. A request with an invalid body gets `400` even from a caller that middleware would reject with `401`. The `400` response describes only what was wrong with the body.

## `/events`

`/events` is deprecated. It waits without a time limit, and responds `200` with the workflow's return value, or `500` with the workflow's error.

**A `500` from `/events` does not always mean that the workflow failed.** For a failed or cancelled execution, the body is the recorded error, written by the codec. For a server error, the body is `{ code: "INTERNAL_ERROR", message: "Internal error" }`. Use `/wait` instead, which reports both outcomes as `200` views.
