# HTTP SDK

The HTTP SDK calls a Notation Durable [HTTP server](./http-server.md) over `fetch` to start, inspect, list, wait on, and cancel executions from processes or browsers that cannot access the runtime directly.

## Browser-safe client bundle

Import `createHttpSdkFactory`, error classes, and view types from `@notation/durable/client`. That subpath omits Node built-ins, the logger, and the dispatcher, so it bundles cleanly for browsers and edge runtimes.

Always import your workflow router type with `import type` so TypeScript validates workflow IDs, parameters, and return types without pulling workflow implementations into the client bundle.

## Creating a client

Call `createHttpSdkFactory<Router>()` once to create a factory, then instantiate an SDK per server URL or per authenticated user session:

::code-group

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

```ts [src/api/exports.ts]
import { sdkForUser } from "../durable-client";

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
  return sdk.cancel(executionId, { reason: "cancelled by user" });
}
```

::

| Option | Purpose |
| :--- | :--- |
| `url` | Base URL of the Durable routes including any server `basePath` |
| `fetchOptions` | `RequestInit` options forwarded to every `fetch` request |

`fetchOptions` accepts any `RequestInit` property except method and body, and applies to every request the SDK sends. Calling the SDK factory function performs no network requests.

## Triggering and waiting for results

The observation and cancellation methods share the same arguments and return values as in the [local SDK](./local-sdk.md). Only `trigger` differs, and `waitForResult` replaces `triggerAndWait`:

| Method | Behavior |
| :--- | :--- |
| `trigger` | Admits an execution and returns `TriggerAck` with `ack` and `waitForResult` helpers |
| `waitForResult` | Waits without a timeout for an execution and returns its typed result |

Call `waitForResult()` on the handle returned by `trigger`, or pass a stored execution ID directly to the SDK:

```ts [src/api/trigger-and-wait.ts]
import { sdkForUser } from "../durable-client";

export async function runExport(sessionToken: string, accountId: string, month: string) {
  const sdk = sdkForUser(sessionToken);

  const execution = await sdk.trigger({
    workflowId: "export-report",
    params: { accountId, month },
  });

  const ack = await execution.ack();
  const result = await execution.waitForResult();

  return { ack, result };
}
```

`trigger` encodes workflow parameters and context locally before sending, throwing `ExecutionAdmissionError` if any value is non-durable. `waitForResult` throws the workflow's error on failure, `WorkflowCancelledError` on cancellation, or `HttpRouteError` if the execution ID does not exist.

### Automatic loop across server maxWait

The HTTP server caps each wait request at its configured `maxWait` duration, returning a timed-out status when the cap expires. Both `wait` and `waitForResult` loop with follow-up requests until the execution finishes or your client-side timeout elapses.

When a caller's `AbortSignal` aborts, `wait` cancels the active fetch request, stops looping, and rejects with the signal's reason.

## Error handling and class revival

### Reviving custom error classes

The HTTP server preserves standard error properties and durable fields across the wire. Register custom error classes with `registerErrorClass` in the client process so observation methods revive instances of your class rather than plain `Error` objects:

```ts [src/errors.ts]
import { registerErrorClass } from "@notation/durable/client";

export class ExportQuotaError extends Error {
  readonly code = "EXPORT_QUOTA_EXCEEDED";

  constructor(message: string) {
    super(message);
    this.name = "ExportQuotaError";
  }
}

registerErrorClass("ExportQuotaError", ExportQuotaError);
```

Once registered, errors thrown by `waitForResult` match `instanceof ExportQuotaError` in client code.

### HTTP route errors

| Response | SDK behavior |
| :--- | :--- |
| Missing execution | Resolves `undefined` from `poll`, `wait`, and `cancel` |
| Admission error | Throws `ExecutionAdmissionError` with the server's `code` |
| List validation error | Throws `ExecutionListRequestError` |
| Any other error | Throws `HttpRouteError` with `status` and optional `code` |

Upstream or misconfigured proxy 404 errors still throw `HttpRouteError`; only a server missing-execution response resolves to `undefined` on observation and cancel methods.

Client-side validation also rejects bad arguments before any request is sent, throwing `ExecutionCancelRequestError` for invalid cancel options and `RangeError` for negative wait timeouts. See [`@notation/durable/client`](../reference/client.md) for the full error mapping.

```ts [src/api/check-export.ts]
import { HttpRouteError } from "@notation/durable/client";
import { sdkForUser } from "../durable-client";

export async function checkExport(sessionToken: string, executionId: string) {
  const sdk = sdkForUser(sessionToken);

  try {
    return await sdk.poll(executionId);
  } catch (err) {
    if (err instanceof HttpRouteError && err.status === 401) {
      throw new Error("Session expired");
    }
    throw err;
  }
}
```
