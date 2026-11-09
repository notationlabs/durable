# HTTP SDK

The HTTP SDK starts, reads and cancels executions through the routes of an [HTTP server](./http-server.md). Code that cannot reach the runtime directly, such as an app server on another host, uses it in place of the [local SDK](./sdk-local.md).

This page uses the web app of a reports product. The app server runs on a different host from Durable. A signed-in user starts an export of a month of their account's data, waits for it, and can cancel it. The app calls Durable with the user's session token, so the Durable server knows which user made each request:

```ts [src/durable-client.ts]
import { createHttpSdkFactory } from "@notation/durable/client";
import type { Router } from "./workflows/router";

const createSdk = createHttpSdkFactory<Router>();

export function sdkForUser(sessionToken: string) {
  return createSdk({
    url: "https://durable.internal.example.com/durable",
    fetchOptions: {
      headers: { Authorization: `Bearer ${sessionToken}` },
    },
  });
}
```

The app's export functions use it:

```ts [src/exports.ts]
import { sdkForUser } from "./durable-client";

export async function startExport(sessionToken: string, accountId: string, month: string) {
  const sdk = sdkForUser(sessionToken);

  const { executionId } = await sdk.trigger({
    workflowId: "export-report",
    executionId: `export:${accountId}:${month}`,
    idempotent: true,
    params: { accountId, month },
  });

  return executionId;
}

export async function waitForExport(sessionToken: string, executionId: string) {
  const sdk = sdkForUser(sessionToken);
  const { url } = await sdk.waitForResult({ executionId });

  return url;
}

export async function cancelExport(sessionToken: string, executionId: string) {
  const sdk = sdkForUser(sessionToken);
  await sdk.cancel(executionId, { reason: "cancelled by user" });
}
```

## Creating the SDK

`createHttpSdkFactory` takes the `Router` type as a type argument, and returns a function that creates an SDK for one server. TypeScript uses `Router` to check each workflow ID and its params, as it does for the local SDK. Creating an SDK sends no request, so the app creates one for each user.

**Import the router with `import type`.** The SDK needs only the router's type, so the app server does not load the workflows or the code they import.

`@notation/durable/client` exports the HTTP SDK, its errors and the view types. Unlike `@notation/durable`, it imports no Node modules, so the same code also works in a browser.

| Option | Holds |
| --- | --- |
| `url` | The address of the routes, including the server's `basePath`. The SDK adds `/trigger`, `/poll` and so on |
| `fetchOptions` | Options that the SDK passes to `fetch` for every request, such as `headers`. `method` and `body` are not allowed |

The SDK sends the same `fetchOptions` to every route. In the example, the `Authorization` header reaches both the server's [trigger middleware](./http-server.md#trigger-middleware) and its [observation middleware](./http-server.md#observation-middleware), so both can identify the user.

## Differences from the local SDK

`poll`, `wait`, `list` and `cancel` take the same arguments and return the same values as in the local SDK. `trigger` is different, and there is no `triggerAndWait`:

| Local SDK | HTTP SDK |
| --- | --- |
| `trigger` returns `{ executionId, status }` | `trigger` returns `{ executionId, status }` with two methods, `ack` and `waitForResult` |
| `triggerAndWait(event)` | `(await trigger(event)).waitForResult()` |
| None | `waitForResult({ executionId })` |

## Starting an execution

`trigger` sends the event to `/trigger`, and returns when the server has recorded the execution:

```ts
const exec = await sdk.trigger({
  workflowId: "export-report",
  params: { accountId: "acct-1", month: "2026-10" },
});

exec.executionId;          // the ID the server recorded
exec.status;               // "accepted" or "existing"
await exec.waitForResult(); // the workflow's return value
```

Without an `executionId` in the event, the server chooses the ID, and the response carries it. `exec.ack()` returns `{ executionId, status }`.

The event's fields and the [idempotent trigger](./sdk-local.md#repeated-triggers) work as in the local SDK. In the example, a user who clicks "Export" twice gets `"existing"` for the second click.

Server middleware can add to the event's context, or replace it. See [Trigger middleware](./http-server.md#trigger-middleware).

## Waiting for a result

`waitForResult` waits until the execution ends, and returns the workflow's return value with the workflow's result type. `waitForExport` uses it with an ID that `startExport` returned in an earlier request:

```ts
const { url } = await sdk.waitForResult({ executionId });
```

| The execution | `waitForResult` |
| --- | --- |
| Completes | Returns the workflow's return value |
| Fails | Throws the workflow's error, as recorded. See [What the record keeps](./errors.md#what-the-record-keeps) |
| Is cancelled | Throws `WorkflowCancelledError`, with the cancellation's `reason` and `source` |
| Does not exist | Throws `HttpRouteError` with the code `EXECUTION_NOT_FOUND` |

The server sends the error with Durable's codec, so the error keeps its class, `code` and `cause` across HTTP. A class registered with [`registerErrorClass`](./errors.md#error-classes) in the app server is read back as that class.

## Waits longer than the server allows

The server holds a `/wait` request open for at most its `maxWait`, 8 seconds by default, and then responds `timed-out`. `wait` and `waitForResult` send the request again until the execution ends, or until their own `timeout` passes. So a wait with no timeout, or a timeout longer than `maxWait`, works as it does with the local SDK.

When a `signal` passed to `wait` aborts, `wait` aborts the request in progress, sends no more, and rejects with the signal's reason.

## Errors

The SDK turns error responses into the same results that the local SDK returns where it can:

| Server response | SDK result |
| --- | --- |
| `404` `EXECUTION_NOT_FOUND` from `/poll`, `/wait` or `/cancel` | `poll`, `wait` and `cancel` return `undefined` |
| `409` or `400` from `/trigger`, with one of the [admission error](./sdk-local.md#admission-errors) codes | `trigger` throws `ExecutionAdmissionError` with the server's code |
| `400` `INVALID_LIST_REQUEST` | `list` throws `ExecutionListRequestError` |
| Any other error response | Throws `HttpRouteError` |

The SDK checks some arguments before it sends a request. `trigger` throws `ExecutionAdmissionError` with the code `INVALID_EXECUTION_EVENT` for params or context that are not durable values. `cancel` throws `ExecutionCancelRequestError` for an empty execution ID, a `reason` that is not a string or a `force` that is not a boolean.

**A `404` without the code `EXECUTION_NOT_FOUND` throws.** A wrong `url` also responds `404`, so the SDK does not take every `404` to mean that the execution does not exist.

`HttpRouteError` has the response's `status`, and its `code` when the body has one. The example can tell that the server denied a request because the user's session expired:

```ts
import { HttpRouteError } from "@notation/durable/client";

try {
  await sdk.poll(executionId);
} catch (err) {
  if (err instanceof HttpRouteError && err.status === 401) {
    // The user's session has expired.
  }
}
```
