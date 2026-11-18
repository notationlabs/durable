# Executions

An execution is one admitted run of a workflow. The [local SDK](./local-sdk.md) and the [HTTP SDK](./http-sdk.md) expose the same lifecycle operations – starting executions, inspecting status, waiting for outcomes, paging through history, and requesting cancellation.

Neither SDK runs workflow code itself. A trigger writes an execution record and returns, while an `ExecutionDispatcher` sharing the same runtime claims due executions and delivers them to workers.

These helpers manage a monthly CSV export workflow (`export-report`) using the local SDK:

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
  const view = await sdk.poll(executionId);
  if (!view) return { status: "not-found" as const };
  if (view.status === "completed") {
    const { url } = view.result as { url: string };
    return { status: "ready" as const, url };
  }
  return { status: view.status };
}

export async function cancelExport(executionId: string) {
  return sdk.cancel(executionId, { reason: "cancelled by user" });
}
```

## Starting executions

`trigger` admits an execution, makes it due immediately, and resolves with a `TriggerAck` (`{ executionId, status }`) once the write commits. It does not wait for a worker to claim or start the execution.

| Field | Purpose |
| :--- | :--- |
| `workflowId` | Workflow key registered in the router |
| `params` | Input payload read in the workflow as `event.params` |
| `context` | Per-request metadata read in the workflow as `event.context` |
| `executionId` | Caller-chosen unique identifier for the execution |
| `idempotent` | Enables safe retries when paired with `executionId` |

`params` is required unless the workflow accepts void parameters, `context` defaults to an empty map, and omitting `executionId` lets the runtime generate a random UUID. Both parameters and context entries must contain only [durable values](./step-values.md) with string context keys, or `trigger` throws `ExecutionAdmissionError`.

### Chosen IDs and idempotent triggers

Execution IDs are unique and never reused, even after an execution completes, fails, or is cancelled. Calling `trigger` with an existing ID when `idempotent` is omitted or false throws `ExecutionAdmissionError` (`EXECUTION_ID_CONFLICT`).

With `idempotent: true` and an explicit `executionId`, `trigger` checks whether the ID already exists and compares the incoming request against the recorded execution:

| Record state | Outcome |
| :--- | :--- |
| ID does not exist | Admits a new execution and returns `status` `"accepted"` |
| Matching workflow ID and params | Admits nothing and returns `status` `"existing"` |
| Different workflow ID or params | Throws `ExecutionAdmissionError` with `EXECUTION_REQUEST_MISMATCH` |

Canonical comparison follows four rules:

- **Key order ignored** – Object keys in `params` are sorted canonically before comparison.
- **Shape preserved** – Omitted `params` differs from `{}`, and `{ a: undefined }` differs from `{}`.
- **Context excluded** – `context` is not compared because it holds per-request metadata such as caller identity or request traces.
- **Status ignored** – A matching repeat returns `status: "existing"` whether the execution is still active or already terminal.

Setting `idempotent: true` without `executionId` fails typechecking and throws `ExecutionAdmissionError` with code `IDEMPOTENT_TRIGGER_REQUIRES_ID` at runtime.

| Error code | Cause |
| :--- | :--- |
| `EXECUTION_ID_CONFLICT` | The `executionId` is in use and `idempotent` is not `true` |
| `EXECUTION_REQUEST_MISMATCH` | The `executionId` exists with a different `workflowId` or `params` |
| `IDEMPOTENT_TRIGGER_REQUIRES_ID` | `idempotent` is `true` without an `executionId` |
| `INVALID_EXECUTION_EVENT` | The event fails validation or contains non-durable values |

## Inspecting status with poll

`poll(executionId)` reads the execution record once and returns an `ExecutionView`, or `undefined` when no execution has that ID.

| Field | Meaning |
| :--- | :--- |
| `executionId` | Unique identifier of the execution |
| `workflowId` | Router key of the workflow |
| `status` | Current execution status |
| `admittedAt` | Timestamp when admission committed |
| `updatedAt` | Timestamp of the most recent record update |
| `parent` | Caller `executionId`, `stepKey`, and `detached` flag |
| `cancellation` | Recorded cancellation request metadata |

The `parent` field appears only on child executions started by `step.start` or `step.call`, and `cancellation` appears once a cancellation request is recorded. `ExecutionView` omits parameters, request context, and internal delivery metadata.

Active executions have `status` `"admitted"`, `"running"`, or `"suspended"` and contain only those base fields. Terminal executions add outcome fields:

| Terminal status | Additional fields |
| :--- | :--- |
| `"completed"` | `result` holding the workflow return value |
| `"failed"` | `error` holding the recorded `Error` |
| `"cancelled"` | Optional `reason` and `source` execution ID |

`result` on an `ExecutionView` is typed as `unknown` because `poll` is not tied to a single workflow signature.

## Waiting for outcomes

`wait(executionId, options?)` reads the execution record and waits until the execution reaches a terminal status, an optional `timeout` in milliseconds elapses, or an `AbortSignal` aborts:

```ts [src/api/wait-export.ts]
import { sdk } from "../durable/client";

export async function waitForExport(executionId: string, signal?: AbortSignal) {
  const outcome = await sdk.wait(executionId, { timeout: 30_000, signal });

  if (!outcome) return { state: "missing" as const };
  if (outcome.status === "timed-out") {
    return { state: "pending" as const, status: outcome.execution.status };
  }
  if (outcome.status === "completed") {
    return { state: "ready" as const, result: outcome.result };
  }
  if (outcome.status === "failed") throw outcome.error;
  return { state: "cancelled" as const, reason: outcome.reason };
}
```

| Condition | Result |
| :--- | :--- |
| Execution is already or becomes terminal | Resolves with `TerminalExecutionView` |
| Timeout elapses before termination | Resolves with `status` `"timed-out"` and `execution` view |
| Execution ID does not exist | Resolves immediately with `undefined` |
| Signal aborts before completion | Rejects with the signal's `reason` |

Omitting `timeout` waits indefinitely until the execution terminates. Passing a negative or `NaN` `timeout` throws `RangeError`. Because `wait` reads the durable execution record rather than relying on in-process worker callbacks, it observes outcomes recorded by any process sharing the backend and survives restarts.

### Starting and waiting in one call

In the local SDK, `triggerAndWait(event)` admits an execution and waits without a timeout, returning the workflow's return value with its inferred TypeScript type:

```ts [src/api/run-export.ts]
import { sdk } from "../durable/client";

export async function runExport(accountId: string, month: string) {
  const { url } = await sdk.triggerAndWait({
    workflowId: "export-report",
    params: { accountId, month },
  });

  return url;
}
```

If the execution fails, `triggerAndWait` throws the recorded error; if cancelled, it throws `WorkflowCancelledError` carrying the cancellation reason and source. When using the HTTP SDK, call `waitForResult` on the execution handle or client instead (see [HTTP SDK](./http-sdk.md)).

## Listing execution history

`list(request?)` returns an `ExecutionViewPage` (`{ executions, cursor? }`) ordered newest first by admission sequence, including child executions.

```ts [src/api/export-history.ts]
import { sdk } from "../durable/client";

export async function listExports(accountId?: string) {
  const results = [];
  let page = await sdk.list({ workflowId: "export-report", limit: 100 });

  while (true) {
    results.push(...page.executions);
    if (!page.cursor) break;
    page = await sdk.list({
      workflowId: "export-report",
      limit: 100,
      cursor: page.cursor,
    });
  }

  return results;
}
```

| Option | Behavior |
| :--- | :--- |
| `workflowId` | Filters to executions of one workflow |
| `status` | Filters to executions matching that status |
| `limit` | Sets maximum views returned per page |
| `cursor` | Resumes after the last execution of the previous page |

`limit` defaults to `50` and is clamped to at most `1000`. Ordering follows the backend's monotonic admission sequence rather than wall-clock `admittedAt` timestamps, so paging remains stable across processes with skewed clocks: no execution is returned twice, and executions admitted after the first page are excluded from later pages.

Invalid filters, non-positive limits, or malformed cursors throw `ExecutionListRequestError`.

## Cancelling executions

`cancel(executionId, options?)` records a cancellation request on an execution and its non-detached descendants, then returns immediately without waiting for workflow code to stop:

```ts [src/api/cancel-export.ts]
import { sdk } from "../durable/client";

export async function abortExport(executionId: string, force = false) {
  return sdk.cancel(executionId, {
    reason: "cancelled by user",
    force,
  });
}
```

| Target state | Return value |
| :--- | :--- |
| Active execution with graceful cancel | `{ status: "requested", execution }` with active view |
| Active execution with forced cancel | Terminal `"cancelled"` view recorded immediately |
| Already terminal execution | Existing `TerminalExecutionView` unchanged |
| Unknown execution ID | `undefined` |

Invalid cancellation arguments fail locally with `ExecutionCancelRequestError` before contacting the backend. See [Cancellation](./cancellation.md) for how workflows catch `WorkflowCancelledError`, run cleanup steps, and propagate cancellation to child workflows.

## Writing SDK-agnostic helpers

Both the local SDK and the HTTP SDK satisfy `ExecutionObserver & ExecutionControl`. Type shared application helpers against `ExecutionObserver`, `ExecutionControl`, or both so the same functions run in-process or over HTTP:

```ts [src/api/export-queries.ts]
import type { ExecutionControl, ExecutionObserver } from "@notation/durable";

export async function recentFailedExports(observer: ExecutionObserver) {
  const page = await observer.list({
    workflowId: "export-report",
    status: "failed",
    limit: 20,
  });

  return page.executions;
}

export async function cancelIfRunning(
  sdk: ExecutionObserver & ExecutionControl,
  executionId: string
) {
  const view = await sdk.poll(executionId);
  if (!view || view.status === "completed" || view.status === "failed" || view.status === "cancelled") {
    return view;
  }
  return sdk.cancel(executionId, { reason: "aborted by operator" });
}
```
