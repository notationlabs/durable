# HTTP Wire Protocol

Notation Durable serves its HTTP control plane through `createRoutes` in `packages/http-server/src/http-server.ts` and consumes it through `createHttpSdkFactory` in `packages/sdk/src/exports/sdk-http.ts`. Every endpoint accepts `POST` JSON bodies, frames values through `durableValueCodec`, and separates admission, authorization, and chunked long-polling.

## Route contract and codec framing

| Route | Behavior |
| :--- | :--- |
| `POST /trigger` | Admits an execution or matches an idempotent repeat |
| `POST /poll` | Returns the current `ExecutionView` or `404` |
| `POST /wait` | Long-polls up to `maxWait` and returns a `WaitResult` or `404` |
| `POST /list` | Returns a cursor-paginated `ExecutionViewPage` |
| `POST /cancel` | Records a cancellation request and returns `202`, `200`, or `404` |
| `POST /events` | Deprecated unbounded wait returning raw result or `500` error |

Every route accepts `createRoutes` with an optional path prefix and standard JSON bodies. `/events` stays only for legacy callers, returning raw encoded results or errors rather than structured wait envelopes.

Server handlers decode every request body and encode observation responses with Durable's value codec over JSON. Standard JSON inputs pass through unchanged, while tagged values preserve undefined values, bigints, dates, maps, sets, and registered error classes.

Before sending a request, client triggers encode workflow arguments locally and throw `ExecutionAdmissionError` if encoding fails. Observation helpers serialize plain request payloads, decode success responses through the value codec, and parse structured JSON error bodies (see [Value Codec & Errors](./value-codec.md)).

## Context immutability

After validating the trigger payload, the server populates a `FreezableMap` with request context entries on `MiddlewareEvent`. The middleware chain runs sequentially and freezes context right before admission, rejecting later mutations:

```ts [packages/http-server/src/middleware.ts]
const next = async (): Promise<Response> => {
  if (index >= middlewares.length) {
    event.context.freeze();
    return await handler(req, event);
  }
  const middleware = middlewares[index++];
  return middleware(req, event, next, logger);
};
```

## Middleware and error status mapping

`MiddlewareFunction` runs only on trigger requests and may mutate request context before freeze or return an early response. `ObservationMiddlewareFunction` runs before observation or cancel endpoints read any execution row, exposing a lazy loader so authorization avoids leaking ID existence.

| Status | Meaning |
| :--- | :--- |
| `200` | Observation success, idempotent `"existing"` trigger, or terminal `/cancel` |
| `202` | Trigger `"accepted"` or active `/cancel` `"requested"` |
| `400` | Malformed body, codec failure, `INVALID_EXECUTION_EVENT`, `IDEMPOTENT_TRIGGER_REQUIRES_ID`, `INVALID_OBSERVATION_REQUEST`, `INVALID_LIST_REQUEST`, or `INVALID_CANCEL_REQUEST` |
| `401` | Observation middleware denial with `UNAUTHORIZED` |
| `403` | Observation middleware denial with `FORBIDDEN` |
| `404` | Unknown execution ID with `EXECUTION_NOT_FOUND` |
| `409` | Admission conflict with `EXECUTION_ID_CONFLICT` or `EXECUTION_REQUEST_MISMATCH` |
| `500` | Unhandled server error with `INTERNAL_ERROR`, or `/events` terminal failure |

`createHttpSdkFactory` throws `HttpRouteError` for non-admission HTTP failures, except missing execution `404` responses on poll, wait, and cancel requests, which resolve to `undefined`.

## Chunked `/wait` long-polling and disconnects

The wait handler bounds each HTTP hold time at `maxWait` (defaulting to 8,000 ms to stay under Bun's 10-second idle timeout). While open, `ExecutionObserver` polls every `pollInterval` (25 ms by default) or wakes immediately when `invoker.workflowEndEmitter` notifies completion:

```ts [packages/sdk/src/exports/sdk-http.ts]
const deadline = Date.now() + timeout;
while (true) {
  signal?.throwIfAborted();
  const remaining = Math.max(0, deadline - Date.now());
  const result = await waitRequest(
    executionId,
    remaining === Infinity ? undefined : remaining,
    signal
  );
  if (result?.status !== "timed-out" || Date.now() >= deadline) {
    return result;
  }
}
```

When a server wait chunk times out before termination, `/wait` returns a timed-out envelope and `sdk.wait` issues follow-up requests with the remaining timeout budget. Wait endpoints forward `Request.signal` to cancel polling timers cleanly on client disconnect without logging aborted disconnects as errors.
