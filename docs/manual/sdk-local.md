# Local SDK

The local SDK starts executions, reads their status and results, and cancels them. It reads and writes the runtime's records directly, so it works in any process that has access to the runtime, such as a web server that shares the SQLite file with the dispatcher.

This page uses an API that exports a month of an account's data as a CSV file. The `export-report` workflow builds the file and returns its URL. The API starts an export, reports its progress, and lets the user cancel it:

```ts [src/api/exports.ts]
import { sdk } from "../durable/client";

export async function startExport(accountId: string, month: string) {
  const { executionId } = await sdk.trigger({
    workflowId: "export-report",
    executionId: `export:${accountId}:${month}`,
    idempotent: true,
    params: { accountId, month },
  });

  return executionId;
}

export async function getExport(executionId: string) {
  const execution = await sdk.poll(executionId);

  if (!execution) {
    return { status: "not-found" };
  }

  if (execution.status === "completed") {
    const { url } = execution.result as { url: string };
    return { status: "ready", url };
  }

  return { status: execution.status };
}

export async function cancelExport(executionId: string) {
  await sdk.cancel(executionId, { reason: "cancelled by user" });
}
```

`sdk` is the SDK from the [Quick start](./quickstart.md), in `src/durable/client.ts`.

## Creating the SDK

`createLocalSdk` takes the runtime, and the `Router` type as a type argument:

```ts
const sdk = createLocalSdk<Router>(runtime, { invoker });
```

TypeScript uses `Router` to check each workflow ID, and the params that go with it.

| Option | Default | Does |
| --- | --- | --- |
| `invoker` | None | Lets `wait` see an outcome as soon as the dispatcher in this process records it, instead of at its next read. Results are the same without it |
| `pollInterval` | `25` | Milliseconds between reads of the record while `wait` waits. Must be greater than zero |

**The SDK does not run workflow code.** It writes a new execution to the runtime and returns. A dispatcher that uses the same runtime, in this process or another one, delivers the execution to a worker. Without a running dispatcher, executions stay `admitted`.

## Starting an execution

`trigger` records a new execution and makes it due at once. It returns when the record is written, and does not wait for the workflow to start.

| Field | Required | Holds |
| --- | --- | --- |
| `workflowId` | Yes | The workflow's ID in the router |
| `params` | If the workflow declares params | The workflow's `event.params` |
| `context` | No | Extra values, read in the workflow as the `event.context` map. See [Event](./workflows.md#event) |
| `executionId` | No | The execution's ID. Without one, Durable creates a random UUID |
| `idempotent` | No | Makes a repeat of the same trigger safe. See [Repeated triggers](#repeated-triggers) |

`trigger` returns `{ executionId, status }`, where `status` is `"accepted"` for a new execution.

Each delivery reads `params` and `context` from the record, so both must be [durable values](./step-values.md). A function or a class instance in either one makes `trigger` throw.

## Execution IDs

An execution ID can be chosen by the caller. The example uses `export:acct-1:2026-10`, so the same account and month always map to the same execution.

**An execution ID is never used twice.** A second trigger with an ID that is in use throws `ExecutionAdmissionError` with the code `EXECUTION_ID_CONFLICT`, even if the first execution has ended.

## Repeated triggers

A user clicks "Export" twice, or the browser repeats a request after a dropped connection. The second call to `startExport` uses the same ID, so without `idempotent` it throws `EXECUTION_ID_CONFLICT`. That error does not say whether the first call started the export, or whether a different request owns the ID.

With `idempotent: true`, `trigger` compares the new request with the execution that has the ID:

| The execution with the ID | `trigger` |
| --- | --- |
| Does not exist | Records a new execution and returns `"accepted"` |
| Has the same `workflowId` and `params` | Records nothing and returns `"existing"`, with the same ID |
| Has a different `workflowId` or `params` | Throws `EXECUTION_REQUEST_MISMATCH`, and records nothing |

So the second click returns `"existing"`, and `getExport` reports the same export.

The comparison follows these rules:

- The key order of objects in `params` does not matter.
- Missing `params` and `{}` are different.
- `context` is not compared, because it holds per-request data, such as the ID of the user who made the request.
- The execution's status does not matter. A repeat after the export has completed also returns `"existing"`.

`idempotent: true` requires an `executionId`. TypeScript reports a missing ID, and `trigger` throws `IDEMPOTENT_TRIGGER_REQUIRES_ID` for callers without the types.

## Admission errors

`trigger` throws `ExecutionAdmissionError` when it records nothing:

| `code` | Cause |
| --- | --- |
| `EXECUTION_ID_CONFLICT` | The ID is in use, and the trigger is not idempotent |
| `EXECUTION_REQUEST_MISMATCH` | The ID is in use by a different request, and the trigger is idempotent |
| `IDEMPOTENT_TRIGGER_REQUIRES_ID` | The trigger is idempotent and has no `executionId` |
| `INVALID_EXECUTION_EVENT` | `params` or `context` are not durable values, or `idempotent` is not a boolean |

## Reading an execution

`poll` reads an execution's record once, and returns a view of it, or `undefined` if no execution has the ID. `getExport` uses it to report progress.

Every view has these fields:

| Field | Holds |
| --- | --- |
| `executionId` | The execution's ID |
| `workflowId` | The workflow's ID |
| `status` | The [execution status](./steps.md#execution-status) |
| `admittedAt`, `updatedAt` | When the execution was recorded and last changed, from the clock of the process that wrote them |
| `parent` | For a child workflow, the parent's execution ID and step key, and whether the child is detached |
| `cancellation` | The [cancellation request](./cancellation.md#requesting-cancellation), if there is one |

A view of an execution that has ended also has its outcome:

| `status` | Extra fields |
| --- | --- |
| `completed` | `result`, the workflow's return value |
| `failed` | `error`, the error the workflow threw |
| `cancelled` | `reason` and `source`, from the cancellation |

**`result` has the type `unknown`.** A view is not tied to one workflow, so TypeScript does not know the result's type. `getExport` asserts the type of the result. A view leaves out `params` and `context`.

## Waiting for an outcome

`wait` waits until the execution has ended, and returns its view:

```ts
const execution = await sdk.wait(executionId, { timeout: 30_000 });
```

| The execution | `wait` returns |
| --- | --- |
| Ends before the timeout | Its view, with `status` `completed`, `failed` or `cancelled` |
| Has not ended when the timeout passes | `{ status: "timed-out", execution }`, where `execution` is the current view |
| Does not exist | `undefined`, at once |

`wait` reads the record first, so an execution that has already ended returns at once. Without a `timeout`, `wait` waits until the execution ends. A `signal` option takes an `AbortSignal`, and `wait` rejects with the signal's reason when it aborts.

`wait` reads the record, not a message from the worker, so it sees an outcome that another process recorded, and it works across a restart.

## Starting and waiting in one call

`triggerAndWait` calls `trigger`, waits without a timeout, and returns the workflow's return value:

```ts
const { url } = await sdk.triggerAndWait({
  workflowId: "export-report",
  params: { accountId: "acct-1", month: "2026-10" },
});
```

Unlike a view's `result`, the return value has the workflow's result type.

| The execution | `triggerAndWait` |
| --- | --- |
| Completes | Returns the workflow's return value |
| Fails | Throws the workflow's error, as recorded. See [What the record keeps](./errors.md#what-the-record-keeps) |
| Is cancelled | Throws `WorkflowCancelledError`, with the cancellation's `reason` and `source` |

## Listing executions

`list` returns executions one page at a time, most recently recorded first. An admin page that shows every export uses it like this:

```ts
let page = await sdk.list({ workflowId: "export-report", limit: 100 });

while (true) {
  for (const execution of page.executions) {
    console.log(execution.executionId, execution.status);
  }

  if (!page.cursor) break;

  page = await sdk.list({ workflowId: "export-report", limit: 100, cursor: page.cursor });
}
```

| Field | Does |
| --- | --- |
| `workflowId` | Returns only executions of this workflow |
| `status` | Returns only executions with this status |
| `limit` | Sets the number of executions per page. The default is `50`, and the most is `1000` |
| `cursor` | Continues from the page that returned it. Pass it with the same filters |

Each page has a `cursor` if more executions may follow. The list includes child workflows.

The list is ordered by when the runtime recorded each execution, not by `admittedAt`, so the order holds across processes whose clocks differ. While a caller pages through the list, these rules hold:

- No execution appears twice.
- Every execution that matched when the first page was read appears once.
- Executions recorded after the first page was read do not appear.
- An execution whose status changes can appear or disappear on later pages, if the list filters by `status`.

`list` throws `ExecutionListRequestError` for a `limit` that is not a positive integer, an unknown `status`, or a cursor that `list` did not return.

## Cancelling

`cancel` records a cancellation request, and returns without waiting for the workflow to stop. `cancelExport` uses it with a reason:

```ts
await sdk.cancel(executionId, { reason: "cancelled by user" });
```

It returns `undefined` if no execution has the ID. See [Cancellation](./cancellation.md) for what the workflow does with the request, the `force` option and the other return values.

## Code for either SDK

The [HTTP SDK](./sdk-http.md) has the same `poll`, `wait`, `list` and `cancel` methods. `@notation/durable` exports their types as `ExecutionObserver`, for `poll`, `wait` and `list`, and `ExecutionControl`, for `cancel`. A function that takes an `ExecutionObserver` works with either SDK:

```ts
import type { ExecutionObserver } from "@notation/durable";

export async function recentFailedExports(observer: ExecutionObserver) {
  const page = await observer.list({ workflowId: "export-report", status: "failed", limit: 20 });
  return page.executions;
}
```
