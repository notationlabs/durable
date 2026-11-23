# Execution Client

`ExecutionClient` stores execution records, assigns admission order, manages leases and wake timestamps, starts child workflows idempotently, and propagates cancellations. Every SDK trigger, HTTP route, and `ExecutionDispatcher` loop operates through this client.

## `ExecutionState` and derived fields

`@notation/durable/runtime` defines `ExecutionState` as the storage-neutral shape of one execution record, with `params`, `context`, and `outcome` stored as pre-encoded strings.

| Field | Stored value |
| :--- | :--- |
| `executionId` | Unique execution ID |
| `workflowId` | Registered workflow ID |
| `params` | Canonical encoded event parameters |
| `context` | Canonical encoded context entries |
| `status` | Resting status excluding `"running"` |
| `outcome` | Encoded terminal outcome |
| `admittedAt` | Clock timestamp for admission |
| `updatedAt` | Clock timestamp for non-lease state changes |
| `dueAt` | Earliest pending wake timestamp |
| `claimedDueAt` | Wake timestamp covered by the current lease |
| `leaseToken` | Highest fencing token issued for this execution |
| `leaseExpiresAt` | Current lease expiry timestamp |
| `unacknowledgedDeliveryCount` | Consecutive unacknowledged deliveries |
| `expiredDeliveryCount` | Deliveries ended by lease expiry since the last progress, compared with the delivery limit |
| `redeliverAt` | Earliest retry timestamp after a release |
| `parent` | Immutable caller `executionId`, `stepKey`, and `detached` flag |
| `cancellation` | First recorded cancellation request, including `forced` and `deliveredAt` |

`ExecutionState` never stores `"running"` as its persisted status. `executionStatusAt` computes `"running"` dynamically while a lease is held and unexpired, falling back to `state.status` otherwise. `toExecutionRecord` decodes parameters, context, and outcome strings alongside live status and retry count.

Persist `nextClaimAt(state)` alongside the row on every insert and update so `claimDue` can query due work through an index. It returns `null` for terminal or unscheduled executions, or the earliest timestamp at which `isExecutionClaimable` turns true.

Claim, renew, and release transitions leave `updatedAt` untouched.

## Helper mapping

Implement each `ExecutionClient` method by validating the input, running a read-transition-write cycle inside one atomic transaction, and returning the helper's result:

| Method | Helpers |
| :--- | :--- |
| `admit` | `prepareExecutionAdmission`, `createExecutionState`, `matchExecutionAdmission` |
| `start` | `prepareExecutionStart`, `matchStartRequest`, `createStartedExecution` |
| `get` | `toExecutionRecord` |
| `list` | `prepareExecutionListRequest`, `matchesExecutionListRequest`, `toExecutionListPage` |
| `requestWake` | `requestExecutionWake`, `executionNotFound` |
| `claimDue` | `nextClaimAt`, `isExecutionClaimable`, `claimExecution` |
| `renew` | `renewExecutionLease` |
| `release` | `releaseExecutionLease` |
| `acknowledge` | `acknowledgeExecution` |
| `cancel` | `prepareExecutionCancel`, `requestExecutionCancellation`, `propagatedCancellation`, `executionNotFound` |

## Admitting executions

`admit` validates the request with `prepareExecutionAdmission`, inserts a new record due immediately with `createExecutionState`, and resolves duplicate `executionId` races with `matchExecutionAdmission`:

```ts [src/postgres-execution-admit.ts]
import {
  createExecutionState,
  matchExecutionAdmission,
  prepareExecutionAdmission,
  type ExecutionAdmissionRequest,
  type ExecutionAdmissionResult,
} from "@notation/durable/runtime";
import type { Pool } from "pg";
import { withTransaction } from "./postgres-tx";

export async function admitExecution(
  pool: Pool,
  clock: () => number,
  request: ExecutionAdmissionRequest
): Promise<ExecutionAdmissionResult> {
  const prepared = prepareExecutionAdmission(request);
  if (!prepared.ok) {
    return { status: "rejected", code: prepared.code, message: prepared.message };
  }

  return withTransaction(pool, async (tx) => {
    const state = createExecutionState({
      executionId: prepared.executionId,
      workflowId: prepared.workflowId,
      encoded: prepared.encoded,
      now: clock(),
    });
    const inserted = await tx.insertExecutionIfAbsent(state);
    if (inserted) {
      return { status: "accepted", executionId: prepared.executionId };
    }
    const existing = await tx.getExecutionState(prepared.executionId);
    return matchExecutionAdmission(existing!, prepared);
  });
}
```

The conflict check and insert must be atomic so two concurrent requests with the same `executionId` never both succeed.

When `request.idempotent` is true:

- `executionId` is required; omitting it rejects with `IDEMPOTENT_TRIGGER_REQUIRES_ID`.
- If the execution ID already exists, `matchExecutionAdmission` compares `workflowId` and canonical parameters (ignoring context), returning `{ status: "existing", executionId }` on match or `EXECUTION_REQUEST_MISMATCH` otherwise, even after termination.
- Without idempotency enabled, any colliding execution ID rejects with `EXECUTION_ID_CONFLICT`.

Assign a strictly increasing `admissionSequence` – such as Postgres `BIGSERIAL` or SQLite `rowid` – in the same atomic insert so `list` ordering stays immune to clock skew.

## Starting child workflows

`start` admits a child execution keyed idempotently by caller execution ID and step key. Run `prepareExecutionStart` first and return its result directly on an `INVALID_START_PARAMETERS` validation failure.

Inside one atomic transaction:

1. Look up an existing `StartRequestState` for the caller execution and step key. If found, `matchStartRequest` returns `"accepted"` with the stored child ID when target workflow, canonical parameters, and detached mode match, or `START_REQUEST_MISMATCH` otherwise.
2. Load the caller state (throwing if absent) and construct the child execution and start record with `createStartedExecution`, regenerating the child ID if it collides.
3. Insert both records together.

`createStartedExecution` copies the parent's encoded context, attaches the parent reference, and propagates any undelivered parent cancellation request to non-detached children immediately. Child starts are unfenced.

## Cursor pagination

`list` pages executions newest-first by `admissionSequence`. Call `prepareExecutionListRequest` to validate filters and cursor; it throws `ExecutionListRequestError` on invalid input, defaults limit to 50 (`DEFAULT_EXECUTION_LIST_LIMIT`), and clamps at 1,000 (`MAX_EXECUTION_LIST_LIMIT`).

Query up to `limit + 1` matching rows ordered newest first where:

- `admissionSequence` precedes `prepared.beforeSequence` when a cursor is set.
- `workflowId` matches the filter when set.
- Computed status at query time matches the status filter when set, treating unexpired leases as running and excluding live leases from other statuses.

Pass the fetched `SequencedExecutionState[]` slice to `toExecutionListPage`. Fetching one extra row lets the helper omit the cursor on the final page and emit the last returned sequence string otherwise.

## Wakes, claims, and leases

`requestWake` loads the row inside a transaction, returning `executionNotFound` when missing, or applies `requestExecutionWake` to persist the earliest pending wake timestamp and return `"ignored"` for terminal executions.

`claimDue` returns an empty array immediately when `limit <= 0` or `filter.executionIds` is empty; otherwise it claims eligible rows atomically:

```ts [src/postgres-execution-claim.ts]
import {
  claimExecution,
  expiredDeliveryCountAt,
  isExecutionClaimable,
  type ExecutionClaimFilter,
  type ExecutionLease,
} from "@notation/durable/runtime";
import type { Pool } from "pg";
import { withTransaction } from "./postgres-tx";

export async function claimDueExecutions(
  pool: Pool,
  clock: () => number,
  request: {
    limit: number;
    duration: number;
    filter?: ExecutionClaimFilter;
    maxExpiredDeliveries?: number;
    onDeliveryLimit?: (event: { executionId: string; deliveries: number }) => void;
  }
): Promise<ExecutionLease[]> {
  const ids = request.filter?.executionIds;
  if (request.limit <= 0 || ids?.length === 0) return [];
  const failed: { executionId: string; deliveries: number }[] = [];

  const leases = await withTransaction(pool, async (tx) => {
    const now = clock();
    const candidates = await tx.selectClaimableForUpdateSkipLocked(now, request.limit, ids);
    const leases: ExecutionLease[] = [];

    for (const state of candidates) {
      if (!isExecutionClaimable(state, now)) continue;
      const claimed = claimExecution(state, {
        now,
        duration: request.duration,
        maxExpiredDeliveries: request.maxExpiredDeliveries,
      });
      await tx.updateExecutionState(claimed.state);
      if (claimed.lease === null) {
        // The claim reached the delivery limit and recorded the execution failed.
        await tx.deleteWaitersHeldBy(state.executionId);
        await tx.wakeJoinWaitersOf(state.executionId);
        failed.push({
          executionId: state.executionId,
          deliveries: expiredDeliveryCountAt(state, now),
        });
        continue;
      }
      leases.push(claimed.lease);
    }

    return leases;
  });
  for (const event of failed) request.onDeliveryLimit?.(event);
  return leases;
}
```

When `claimExecution` returns `lease: null`, the claim reached `maxExpiredDeliveries` and the state is `failed` with a `DeliveryLimitError`. Delete the waiters the execution holds and wake its join waiters in the same transaction, as for a forced cancellation. After the transaction commits, call `onDeliveryLimit` once for each execution the claim failed. `expiredDeliveryCountAt(state, now)` on the state before the claim gives `deliveries`.

A heap step write that inserts a new done record must apply `recordExecutionProgress` to the execution's state in the same transaction, and write the state back when the helper returns a different object. A write that inserts a record that is not done, or finds the record already stored, must not.

Sort claimable rows by `nextClaimAt ASC, executionId ASC` and lock selected rows, for example with `SELECT ... FOR UPDATE SKIP LOCKED` in Postgres, so concurrent dispatchers never lease the same execution.

`renew`, `release`, and `acknowledge` load the execution under lock, return `{ status: "lost" }` if absent, apply the matching transition helper, and write back only when state changed.

When `acknowledge` transitions an execution into a terminal status, wake every waiting caller registered on that target in the same transaction. Leave join waiters in place so each joiner's finished step cleans up its own row.

## Cancelling executions

`cancel` validates inputs with `prepareExecutionCancel` and throws `TypeError` on invalid input. Inside one atomic transaction:

1. Load the target execution, returning `executionNotFound` when absent.
2. Walk the target and descendants breadth-first through `requestExecutionCancellation`, persisting every changed state.
3. Whenever `applied.propagate` is true, compute the child request with `propagatedCancellation` and enqueue all non-detached, non-terminal children.
4. Whenever a forced cancel terminates an active execution (`applied.terminal`), delete all store, effect, and join waiters belonging to that execution and wake its waiting joiners together.

Force-cancelling an active execution increments its fencing token and clears the lease expiration, immediately invalidating any in-flight worker writes.
