# HTTP SDK

`createHttpSdkFactory` produces a factory function that creates typed SDK clients bound to a remote server's base URL. Like the local SDK, it takes your `Router` type as a generic parameter so TypeScript validates every `workflowId` and its corresponding `params` at compile time. The server must expose the routes from [`createRoutes`](./http-server.md).

## Setup

```ts
import { createHttpSdkFactory } from "yieldstar";
import type { Router } from "./shared";

const createSdk = createHttpSdkFactory<Router>();
const sdk = createSdk({ url: "http://localhost:8080" });
```

Pass `fetchOptions` to attach custom headers or other `RequestInit` properties to every outgoing request. The SDK sends them to every route, so the same header can satisfy both the server's trigger middleware and its [observation middleware](./http-server.md#observation-middleware):

```ts
const sdk = createSdk({
  url: "http://localhost:8080",
  fetchOptions: {
    headers: { Authorization: "Bearer token" },
  },
});
```

## `sdk.trigger`

Posts to `/trigger` and returns a handle containing `executionId`, `status`, `ack()`, and `waitForResult()`. The server admits the execution and responds with the execution ID it accepted; the SDK reads `executionId` from that response rather than generating one itself. `status` is `"accepted"`, or `"existing"` for an idempotent repeat that matched an execution already admitted with the same request (see [Idempotent trigger](./sdk-local.md#idempotent-trigger)).

```ts
const exec = await sdk.trigger({
  workflowId: "process-order",
  executionId: "order-abc123",
  idempotent: true,
  params: { orderId: "abc123" },
});
```

When the server rejects the admission, `trigger` throws an `ExecutionAdmissionError` with the server's `code`: `EXECUTION_ID_CONFLICT` or `EXECUTION_REQUEST_MISMATCH` for a `409`, and `INVALID_EXECUTION_EVENT` or `IDEMPOTENT_TRIGGER_REQUIRES_ID` for a `400` (see [Local SDK](./sdk-local.md#sdktrigger)). `trigger` throws `INVALID_EXECUTION_EVENT` without sending a request when its own params or context are not durable. Any other error response, such as a middleware denial, throws an `HttpRouteError` with the response's `status`, and its `code` when the body has one.

## `exec.ack()`

Returns the parsed trigger response, `{ executionId, status }`.

```ts
const { executionId } = await exec.ack();
```

## `exec.waitForResult()`

Calls [`sdk.wait`](#sdkpoll-sdkwait-and-sdklist) without a timeout. It returns the result, or throws the recorded error.

```ts
const result = await exec.waitForResult();
```

The server writes the view with the durable value codec, and the SDK reads it with the same codec, so `undefined` properties keep their keys and errors keep their shape across HTTP. If the record is `failed`, `waitForResult` throws the recorded error as its registered class, with its original `stack`, `code` and `cause`. For an unknown ID it throws an `HttpRouteError` with code `EXECUTION_NOT_FOUND`.

`sdk.waitForResult({ executionId })` does the same for an execution triggered elsewhere.

## `sdk.poll`, `sdk.wait` and `sdk.list`

These behave as they do in the [local SDK](./sdk-local.md#sdkpoll), and return the same [views](./sdk-local.md#execution-views). They post to `/poll`, `/wait` and `/list`.

```ts
const execution = await sdk.wait("order-abc123", { timeout: 30_000 });
```

The server holds each `/wait` request open for at most 8 seconds, and then responds `timed-out`. `sdk.wait` repeats the request until the execution is terminal or your own `timeout` passes, so a `timeout` longer than 8 seconds, or no `timeout`, works as it does locally.

Errors map to the same results as in the local SDK where they can:

| Server response                    | SDK result                                     |
| ---------------------------------- | ---------------------------------------------- |
| `404` `EXECUTION_NOT_FOUND` from `/poll` or `/wait` | `poll` and `wait` resolve with `undefined` |
| `400` `INVALID_LIST_REQUEST`       | `list` throws `ExecutionListRequestError`      |
| Any other error response           | Throws an `HttpRouteError`                     |

A `404` without the code `EXECUTION_NOT_FOUND`, for example from a wrong `url` or a server without the observation routes, throws an `HttpRouteError`, so `waitForResult` does not report an existing execution as unknown.

A negative `timeout` throws a `RangeError` before any request.

## `HttpRouteError`

`HttpRouteError` is the error for a response that has no more specific error, such as an observation middleware denial. It carries the response's `status`, and its `code` when the body has one.

```ts
import { HttpRouteError } from "yieldstar";

try {
  await sdk.poll("order-abc123");
} catch (err) {
  if (err instanceof HttpRouteError && err.status === 403) {
    // observation middleware denied the request
  }
}
```

## Trigger event shape

| Field         | Type                  | Required                 | Description                                                  |
| ------------- | --------------------- | ------------------------ | ------------------------------------------------------------ |
| `workflowId`  | `string`              | Yes                      | ID matching a key in the router                              |
| `params`      | `Record<string, any>` | Depends on workflow type | Input data accessible via `event.params` inside the workflow |
| `executionId` | `string`              | No                       | Custom execution ID. The runtime chooses one if omitted      |
| `context`     | `Record<string, any>` | No                       | Additional context passed through middleware                 |
| `idempotent`  | `boolean`             | No                       | Makes a repeat of the same request return `"existing"`. Requires `executionId` |
