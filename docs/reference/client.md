# @notation/durable/client

`@notation/durable/client` exports the HTTP SDK, its errors and the types of the views that it returns. It imports no Node modules, so code that uses it also runs in a browser.

```ts
import { createHttpSdkFactory } from "@notation/durable/client";
```

See [HTTP SDK](../manual/sdk-http.md) for how to use it.

## Functions

| Export | Does |
| --- | --- |
| `createHttpSdkFactory<Router>()` | Returns a function that takes `{ url, fetchOptions? }` and creates an SDK for that server |
| `registerErrorClass(name, errorClass)` | Registers a class to rebuild errors of that name from the server's responses. See [Error classes](../manual/errors.md#error-classes) |

## Errors

| Export | Thrown by |
| --- | --- |
| `HttpRouteError` | Any method, for an error response with no more specific error |
| `ExecutionAdmissionError` | `trigger` |
| `ExecutionListRequestError` | `list` |
| `ExecutionCancelRequestError` | `cancel`, before it sends a request |
| `WorkflowCancelledError` | `triggerAndWait` and `waitForResult`, for a cancelled execution |

See [Errors](../manual/sdk-http.md#errors) on the HTTP SDK page.

## Types

| Type | Is |
| --- | --- |
| `ExecutionObserver` | The methods that read executions: `poll`, `wait` and `list` |
| `ExecutionControl` | The `cancel` method |
| `ExecutionView`, `TerminalExecutionView` | What `poll` returns |
| `ExecutionViewPage` | What `list` returns |
| `WaitResult` | What `wait` returns |
| `TriggerAck` | What `trigger` returns |
| `ExecutionListRequest`, `ExecutionWaitOptions` | The arguments of `list` and `wait` |
| `CancelOptions`, `CancelResult` | The argument and result of `cancel` |
| `ExecutionStatus`, `ExecutionCancellation` | Fields of a view |
| `ExecutionAdmissionErrorCode` | The codes of `ExecutionAdmissionError` |

The local SDK and the HTTP SDK both implement `ExecutionObserver` and `ExecutionControl`. See [Code for either SDK](../manual/sdk-local.md#code-for-either-sdk).
