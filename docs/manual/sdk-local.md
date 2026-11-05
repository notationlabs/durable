# Local SDK

`createLocalSdk` builds a typed client that admits executions directly into a runtime in the same process, and reads them back from their execution records. It takes your `Router` type as a generic parameter, so TypeScript checks every `workflowId` and its corresponding `params` at compile time.

## Setup

```ts
import { ExecutionDispatcher } from "@yieldstar/core";
import { createLocalSdk } from "yieldstar";
import type { Router } from "./shared";

const sdk = createLocalSdk<Router>(runtime, { invoker });

// The SDK never runs workflow code. A dispatcher must be running somewhere
// against the same runtime to deliver what the SDK admits.
new ExecutionDispatcher(runtime, { invoker, logger }).start();
```

| Option         | Type              | Default | Description                                                                         |
| -------------- | ----------------- | ------- | ----------------------------------------------------------------------------------- |
| `invoker`      | `WorkflowInvoker` | –       | Optional. When the dispatcher runs in this process, lets `wait` and `triggerAndWait` read the record as soon as the dispatcher records an outcome, instead of at their next read. No result depends on it |
| `pollInterval` | `number`          | `25`    | Milliseconds between reads of the execution record in `wait` and `triggerAndWait`. It must be greater than zero; any other value throws a `RangeError` |

## `sdk.trigger`

Admits an execution and resolves with `{ executionId, status }` once admission commits. `status` is `"accepted"`, or `"existing"` for an [idempotent](#idempotent-trigger) repeat. Admission writes the [execution record](./workflows.md#execution-records) and makes it due immediately; `trigger` does not wait for a worker to start.

```ts
const { executionId } = await sdk.trigger({
  workflowId: "process-order",
  params: { orderId: "abc123" },
});
```

`trigger` throws an `ExecutionAdmissionError` when admission is rejected:

| `code`                           | Cause                                                                     |
| -------------------------------- | ------------------------------------------------------------------------- |
| `EXECUTION_ID_CONFLICT`          | The supplied `executionId` is already in use. IDs are never reused, even after the execution fails |
| `EXECUTION_REQUEST_MISMATCH`     | An idempotent trigger's `executionId` is in use by a different request    |
| `IDEMPOTENT_TRIGGER_REQUIRES_ID` | An idempotent trigger has no `executionId`                                |
| `INVALID_EXECUTION_EVENT`        | `params` or `context` are not durable values (see below), or `idempotent` is not a boolean |

```ts
import { ExecutionAdmissionError } from "yieldstar";

try {
  await sdk.trigger({ workflowId: "process-order", executionId: "order-abc123", params });
} catch (err) {
  if (err instanceof ExecutionAdmissionError && err.code === "EXECUTION_ID_CONFLICT") {
    // order-abc123 was already admitted
  }
}
```

## Idempotent trigger

Suppose a web server triggers `process-order` with the ID `order-42`, and the response is lost. The server retries with the same ID. Without idempotent mode, the retry throws `EXECUTION_ID_CONFLICT`. That error does not tell the server whether its first trigger was admitted, or whether a different request already owns `order-42`.

Pass `idempotent: true` with your own `executionId` to make the retry safe:

```ts
const { executionId, status } = await sdk.trigger({
  executionId: `order-${orderId}`,
  idempotent: true,
  workflowId: "process-order",
  params: { orderId },
});
```

When the ID is not in use, the trigger is admitted as usual and returns `"accepted"`. When the ID is in use, the runtime compares the request with the record:

- If `workflowId` and `params` match, the trigger admits nothing and returns `"existing"` with the same `executionId`. This is true whatever the execution's status, including `completed` and `failed`, and also when the first trigger was not idempotent.
- If either differs, the trigger throws `EXECUTION_REQUEST_MISMATCH`, and the record is unchanged.

`params` match when their canonical durable value encodings are equal. The encoding sorts the keys of plain objects, so their key order does not matter. Missing `params` and `{}` do not match. `context` is not compared, because it carries per-request data such as an authentication token. Of two idempotent triggers with the same request that run at the same time, one returns `"accepted"` and the other `"existing"`.

So the retry for `order-42` returns `"existing"`, and the server can call [`wait`](#sdkwait) on `order-42`. A trigger that reuses `order-42` with a different amount throws `EXECUTION_REQUEST_MISMATCH`, and the caller knows to choose a new ID.

Without `idempotent`, a used ID still throws `EXECUTION_ID_CONFLICT`.

`TriggerEvent` requires `executionId` when `idempotent` is `true`, so leaving it out is a type error. Admission still checks it, so an untyped caller, such as an HTTP client, gets `IDEMPOTENT_TRIGGER_REQUIRES_ID`.

## `sdk.triggerAndWait`

`trigger` followed by `wait` without a timeout. It returns the workflow's return value.

```ts
const result = await sdk.triggerAndWait({
  workflowId: "process-order",
  params: { orderId: "abc123" },
});
```

The promise settles from the execution record, not from an in-memory event, so it settles even if another process ran the execution or the worker restarted. It does not need an `invoker`. If the record is `failed`, the promise rejects with the recorded error. The error keeps its `name`, `message`, `stack`, a string `code`, its `cause` and its durable fields, and it is revived as its class when that class is registered with `registerErrorClass` (see [Errors in step records](./steps.md#errors-in-step-records)).

## `sdk.poll`

Returns the current [view](#execution-views) of an execution, or `undefined` if no record has that ID.

```ts
const execution = await sdk.poll("order-abc123");
if (execution?.status === "running") {
  // a worker holds a lease on it
}
```

## `sdk.wait`

Reads the execution record, and then waits for it to reach `completed` or `failed`. It resolves with the terminal view.

```ts
const execution = await sdk.wait("order-abc123", { timeout: 30_000 });

if (!execution) {
  // no record has this ID
} else if (execution.status === "timed-out") {
  // execution.execution is the current view, for example "suspended"
} else if (execution.status === "failed") {
  throw execution.error;
} else {
  return execution.result;
}
```

- `wait` reads the record before it waits, so an outcome recorded before the call is returned at once.
- It returns an outcome recorded by any process that shares the runtime's backend, and across a restart.
- When `timeout` (in milliseconds) passes first, it resolves with `{ status: "timed-out", execution }`, where `execution` is the current view. It does not throw.
- Without a `timeout`, it waits until the execution is terminal.
- For an unknown ID it resolves with `undefined` at once. It does not wait for a later admission.
- When `signal` (an `AbortSignal`) aborts, it stops reading the record and rejects with the signal's reason.
- A negative `timeout` throws a `RangeError`.

A failed execution returns the same error, with the same `name` and `message`, every time it is read.

## `sdk.list`

Pages through executions, newest first.

```ts
let page = await sdk.list({ workflowId: "process-order", status: "failed", limit: 100 });
while (true) {
  for (const execution of page.executions) console.log(execution.executionId);
  if (!page.cursor) break;
  page = await sdk.list({ workflowId: "process-order", status: "failed", cursor: page.cursor });
}
```

| Field        | Type              | Description                                                                   |
| ------------ | ----------------- | ----------------------------------------------------------------------------- |
| `workflowId` | `string`          | Optional. Only executions of this workflow                                    |
| `status`     | `ExecutionStatus` | Optional. Only executions with this status                                   |
| `cursor`     | `string`          | Optional. The `cursor` of the previous page                                   |
| `limit`      | `number`          | Optional. Executions per page. Defaults to `50`; values above `1000` are lowered to `1000` |

The result is `{ executions, cursor? }`. `cursor` is present when more executions may follow. It is opaque: pass it back unchanged with the same filters.

The order is the order in which the backend admitted the executions, not `admittedAt`. The backend gives each execution an admission sequence number when it admits it, so the order holds across processes, whatever their clocks. The list includes executions started by [`step.start`](./steps.md#stepstart).

- Paging never returns an execution twice.
- Every matching execution admitted before the first page appears once. Executions admitted after the first page do not appear on later pages.
- `status` is compared with the status at the runtime clock's current time, so `running` matches executions with a live lease. An execution whose status changes while you page through a `status` filter may appear or disappear on later pages.

`list` throws `ExecutionListRequestError` (`code: "INVALID_LIST_REQUEST"`) for a `workflowId` that is not a string, a `limit` that is not a positive integer, an unknown `status`, or a malformed `cursor`. A field set to `null` is treated as absent.

## Execution views

`poll`, `wait` and `list` return views of the execution record:

```ts
// Not exported: the fields every view has.
type ExecutionViewBase = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
};

type ActiveExecutionView = ExecutionViewBase & {
  status: "admitted" | "running" | "suspended";
};

type TerminalExecutionView =
  | (ExecutionViewBase & { status: "completed"; result: unknown })
  | (ExecutionViewBase & { status: "failed"; error: Error });

type ExecutionView = ActiveExecutionView | TerminalExecutionView;

type WaitResult =
  | TerminalExecutionView
  | { status: "timed-out"; execution: ActiveExecutionView };

type ExecutionViewPage = { executions: ExecutionView[]; cursor?: string };
```

A view leaves out `params`, `context`, lease and wake state and `unacknowledgedDeliveryCount`. Operators can read those with `runtime.executions.get`. `admittedAt` and `updatedAt` come from the clock of the process that wrote them, so use them for display only.

Both the local SDK and the [HTTP SDK](./sdk-http.md) implement the `ExecutionObserver` interface (`poll`, `wait` and `list`), which `yieldstar` exports with these types. Code that only reads executions can take an `ExecutionObserver` and work with either SDK.

## Trigger event shape

| Field         | Type                  | Required                 | Description                                                  |
| ------------- | --------------------- | ------------------------ | ------------------------------------------------------------ |
| `workflowId`  | `string`              | Yes                      | ID matching a key in the router                              |
| `params`      | `Record<string, any>` | Depends on workflow type | Input data accessible via `event.params` inside the workflow |
| `executionId` | `string`              | No                       | Custom execution ID. The runtime chooses one if omitted      |
| `context`     | `Record<string, any>` | No                       | Becomes the read-only `event.context` map                    |
| `idempotent`  | `boolean`             | No                       | Makes a repeat of the same request return `"existing"`. Requires `executionId` |

Every delivery reads `params` and `context` back from the record exactly as they were admitted, so both must be durable values (see [Step values](./steps.md#step-values)). Admission rejects functions, class instances, `NaN` and `Infinity`, invalid `Date`s, sparse arrays and circular references with `INVALID_EXECUTION_EVENT`. `undefined` properties, errors, `Date`s, `Map`s, `Set`s and `bigint`s are accepted and read back as the same type. `workflow` rejects params types that are not durable at compile time, so `trigger` only accepts durable params for workflows that declare them; params typed `any`, the default for a workflow that does not, are checked only at admission.
