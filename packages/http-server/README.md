# @yieldstar/http-server

## Features

- Route handlers for admitting executions and reading their outcomes
- Middleware for request/response/context processing

Handlers use the standard `Request` and `Response` APIs. The routes object plugs into `Bun.serve({ routes })` or any router that accepts `Request` to `Response` handlers.

## Installation

```sh
npm install @yieldstar/http-server
```

## Basic Usage

The routes only write and read execution records. An `ExecutionDispatcher` running against the same runtime delivers the executions they admit.

```ts
import { ExecutionDispatcher } from "@yieldstar/core";
import { createRoutes } from "@yieldstar/http-server";
import { pino } from "pino";

const logger = pino();

new ExecutionDispatcher(runtime, { invoker, logger }).start();

const server = Bun.serve({
  port: 3000,
  routes: createRoutes({
    runtime,
    invoker,
    logger,
    middleware: [middleware1, middleware2],
  }),
});

logger.info(`Server started on port ${server.url}`);
```

## Custom Routes

```ts
Bun.serve({
  port: 3000,
  routes: {
    "/status": new Response('OK'),
    // mount workflow routes on /workflow
    ...createRoutes({
      basePath: "/workflow",
      runtime,
      logger,
    }),
  },
});
```

## Middleware

Middleware can be passed to the server. Middleware enables:

- reading the HTTP request
- setting context (available to other middleware and the admitted workflow)
- returning early HTTP responses
- setting HTTP response headers

```ts
import { createRoutes, createMiddleware } from "@yieldstar/http-server";

// CORS middleware
const corsMiddleware = createMiddleware(async (req, event, next) => {
  // Continue to the next middleware or handler
  const response = await next();

  // Add CORS headers to the response
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, DELETE, OPTIONS"
  );
  response.headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization"
  );

  return response;
});

// Authentication middleware
const authMiddleware = createMiddleware(async (req, event, next) => {
  // Get auth token from request
  const cookies = parseCookies(req.headers.get("Cookie"));
  const authToken = cookies["X-Token"];

  if (!authToken) {
    return new Response("Unauthorized", { status: 401 });
  }

  // Store the auth token in the context
  event.context.set("authToken", authToken);

  // Continue to the next middleware or handler
  return next();
});

// Create server with middleware
const server = Bun.serve({
  port: 8080,
  routes: createRoutes({
    logger,
    runtime,
    invoker,
    middleware: [corsMiddleware, authMiddleware],
  }),
});
```

### Middleware lifecycle

```ts
import { createMiddleware } from "@yieldstar/http-server";

const myMiddleware = createMiddleware(async (req, event, next) => {
  // Read the HTTP request
  const token = req.headers.get("X-token");

  // Read context set by previous middlewares
  event.context.get("token");

  // Set context
  if (!token) {
    event.context.set("token", token);
  }

  // Advance the middleware chain until either:
  // A middleware function returns a response,
  // or, the final handler admits the execution
  const response = await next();

  // After next() is called, the context is frozen to prevent modifications
  // Any attempt to modify the context will throw an error:
  event.context.set("key", "value"); // Error: Cannot call set on a frozen map

  // The context is admitted with the execution, and every delivery reads it
  // back as a read-only map, so values must be durable values

  // The response can still be updated before it is sent to the user
  response.headers.set("X-Handled-By", "SuperAceDev");

  return response;
});
```

## API Reference

### `createRoutes(options)`

Creates the `POST /trigger`, `POST /poll`, `POST /wait`, `POST /list`, `POST /cancel` and deprecated `POST /events` route handlers.

Options:

- `runtime`: The runtime whose `executions` client the routes admit into and read from
- `invoker`: Optional. When the dispatcher runs in this process, lets `/wait` and `/events` respond as soon as it records an outcome
- `logger`: A Pino logger instance
- `middleware`: Optional array of middleware functions for `/trigger`
- `observationMiddleware`: Optional array of functions that allow or deny `/poll`, `/wait`, `/list`, `/events`, `/cancel` and `/clear-divergence` requests. Without it they accept every request, and `createRoutes` logs a warning if `middleware` is set
- `pollInterval`: Optional. How often each open `/wait` and `/events` request reads the record, in milliseconds. Defaults to 25. It must be a finite number greater than zero. Each open request reads on its own, so many concurrent waits multiply the reads of the backend
- `maxWait`: Optional. The longest the server holds a `/wait` request open, in milliseconds. Defaults to 8000. It must be a finite number greater than zero
- `basePath`: Optional path to mount the routes on

Responses:

- `POST /trigger`: `202 { executionId, status: "accepted" }` once admission commits, `200 { executionId, status: "existing" }` when an idempotent trigger matches an existing execution, `409` `EXECUTION_ID_CONFLICT` or `EXECUTION_REQUEST_MISMATCH` when the execution ID is in use, `400` `INVALID_EXECUTION_EVENT` when `params` or `context` are not durable values, `400` `IDEMPOTENT_TRIGGER_REQUIRES_ID` when an idempotent trigger has no ID
- `POST /poll`: `200` with the execution's view, `404` `EXECUTION_NOT_FOUND`
- `POST /wait`: `200` with the terminal view, or `{ status: "timed-out", execution }` after `timeout` or `maxWait`, whichever is shorter; `404` `EXECUTION_NOT_FOUND`
- `POST /list`: `200 { executions, cursor? }`, newest first by admission
- `POST /cancel`: records a cancellation request and does not wait for the workflow to stop; `202 { status: "requested", execution }` while the execution is active, `200` with the terminal view when it is terminal (including after `force: true`), `404` `EXECUTION_NOT_FOUND`, `400` `INVALID_CANCEL_REQUEST`
- `POST /events`: deprecated; blocks until the execution record is terminal; `200` with the result, `500` with the recorded error, both encoded with the durable value codec, `404` for an unknown execution ID

Error responses carry `{ code, message }`. A `500` `INTERNAL_ERROR` response has the fixed message `Internal error`; the server logs the thrown error instead of sending it.

### `createMiddleware(handler)`

Creates a middleware function.

Handler parameters:

- `req`: The request object
- `event`: The middleware event with a context property (Map). `event.executionId` is `undefined` when the client lets the runtime choose one
- `next`: Function to call the next middleware or handler
- `logger`: The logger passed to createRoutes

## License

MIT
