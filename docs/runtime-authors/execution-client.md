# Execution client

The execution client keeps one record for each execution. The record holds the execution's event, its status and outcome, its lease, the time it is next due, and any cancellation request. The SDK, the HTTP routes and the dispatcher all work through this client.

## Execution state

`@notation/durable/runtime` exports `ExecutionState`, the fields that a runtime stores for one execution. The event and outcome are already encoded as strings. The runtime stores the fields in any form, such as one row per execution:

| Field | Holds |
| --- | --- |
| `executionId`, `workflowId`, `params`, `context` | The event |
| `status`, `outcome` | The status without a lease, and the encoded outcome once the execution has ended |
| `admittedAt`, `updatedAt` | Times from the runtime's clock |
| `dueAt`, `claimedDueAt` | When the execution is next due, and the due time that the current lease covers |
| `leaseToken`, `leaseExpiresAt` | The highest token issued, and when the current lease expires. `leaseExpiresAt` is `null` without a lease |
| `unacknowledgedDeliveryCount`, `redeliverAt` | Deliveries in a row without an acknowledgement, and when a released execution may be claimed again |
| `parent` | The execution and step that started this one, if any |
| `cancellation` | The cancellation request, if any |

The state has no `running` status. A record is `running` while its lease has not expired, so the status changes when time passes without a write. `toExecutionRecord(state, now)` builds the record that `get` and `list` return, with the status at `now`.

## Methods

Each method reads the state, calls a helper from `@notation/durable/runtime`, and writes the result, in one transaction:

| Method | Called by | Helpers |
| --- | --- | --- |
| `admit` | `trigger` | `prepareExecutionAdmission`, `matchExecutionAdmission`, `createExecutionState` |
| `start` | `step.start`, `step.call` | `prepareExecutionStart`, `matchStartRequest`, `createStartedExecution` |
| `get` | The SDK, the dispatcher | `toExecutionRecord` |
| `list` | The SDK | `prepareExecutionListRequest`, `matchesExecutionListRequest`, `toExecutionListPage` |
| `requestWake` | The runtime's other clients | `requestExecutionWake` |
| `claimDue` | The dispatcher | `isExecutionClaimable`, `nextClaimAt`, `claimExecution` |
| `renew` | The dispatcher | `renewExecutionLease` |
| `release` | The dispatcher | `releaseExecutionLease` |
| `acknowledge` | The dispatcher | `acknowledgeExecution` |
| `cancel` | The SDK | `prepareExecutionCancel`, `requestExecutionCancellation`, `propagatedCancellation` |

`renew`, `release` and `acknowledge` are fenced. Their helpers return `{ status: "lost" }` and the state unchanged when the lease is not current. See [Delivery guarantees](./delivery-guarantees.md).

## Admitting executions

`admit` records a new execution as `admitted` and due now. `prepareExecutionAdmission` validates the request, encodes the event, and chooses an ID if the request has none. If the ID is in use, `matchExecutionAdmission` answers from the existing state. Otherwise `createExecutionState` builds the new state.

**The check for an existing ID and the insert must be one atomic step.** Two triggers with the same ID can arrive at once. One must be `accepted`, and the other must receive the answer for an ID in use.

Each execution also needs an admission sequence: a number greater than that of every execution admitted before it, from any process. `list` returns executions newest first by this number, not by `admittedAt`, because the clocks of two processes can disagree.

## Starting children

`start` admits a child execution on behalf of a step, and records the request under the calling execution's ID and the step key. `prepareExecutionStart` validates the request.

| The step's request | `start` |
| --- | --- |
| Is new | Calls `createStartedExecution`, and inserts the child and the request record in one write |
| Is recorded | Answers with `matchStartRequest`: the same child's ID, or `START_REQUEST_MISMATCH` |

`createStartedExecution` chooses the child's ID. If the ID is in use, call it again for another. It also gives the child the caller's context, its `parent`, and a cancellation request when the caller's request has not yet been delivered.

## Listing

`list` returns a page of executions, newest first. `prepareExecutionListRequest` validates the request and decodes its cursor into `beforeSequence`. It throws `ExecutionListRequestError` for an invalid request.

The runtime reads up to `limit + 1` executions with a sequence below `beforeSequence` that `matchesExecutionListRequest` accepts. `toExecutionListPage` builds the page from them, each with its admission sequence. It uses the extra execution to decide whether the page has a cursor.

## Wakes

An execution is due when its `dueAt` has passed. A wake from a store update, an effect settlement or a join sets `dueAt` with `requestExecutionWake`. When several wakes are pending, `dueAt` holds the earliest. A wake for an execution that has ended is ignored.

## Claiming

`claimDue` leases up to `limit` executions whose next claim time has passed, earliest first. `nextClaimAt(state)` computes that time from the due time and the redelivery delay. It returns `null` for an execution that is not due, or has ended.

`SqliteRuntime` stores `nextClaimAt(state)` in an indexed column each time it writes a state, and finds claimable executions with a query on that column. In a transaction, it checks each one again with `isExecutionClaimable`, and claims it with `claimExecution`:

```ts
return runTransaction(this.db, () => {
  const now = this.clock();
  const leases: ExecutionLease[] = [];

  for (const executionId of this.executionsDao.getClaimableIds(now, request.limit, executionIds)) {
    const state = this.executionsDao.getState(executionId);
    if (!state || !isExecutionClaimable(state, now)) continue;

    const claimed = claimExecution(state, { now, duration: request.duration });
    this.executionsDao.updateState(claimed.state);
    leases.push(claimed.lease);
  }

  return leases;
});
```

**Two claims that run at once must not both lease one execution.** SQLite's transaction takes the write lock, so the second claim sees the first claim's lease. A runtime on another database needs the same effect, for example with `SELECT ... FOR UPDATE SKIP LOCKED` in Postgres.

With `filter.executionIds`, `claimDue` claims only executions with those IDs. An empty list claims nothing.

## Acknowledging

`acknowledge` records a delivery's outcome and ends the lease. A `suspended` outcome makes the execution due again at its `wakeAt`, or at a wake that arrived during the delivery, whichever is earlier.

When the outcome ends the execution, the same write must make every execution that joins it due. `acknowledgeExecution` returns the same state object when the lease was lost or the execution had already ended. So a new state with a final status means that this acknowledgement ended the execution. See [Join client](./join-client.md).

## Cancelling

`cancel` records a cancellation request. `prepareExecutionCancel` validates it. For an invalid request, `cancel` throws a `TypeError` and changes nothing.

`requestExecutionCancellation(state, request, now)` applies the request to one execution, and returns the new state, the result, and two flags:

| Flag | When it is `true`, the same write also |
| --- | --- |
| `propagate` | Applies `propagatedCancellation(state)` to each child that has not ended and is not detached, and in turn to their children |
| `terminal` | Deletes the store, effect and join waiters that the execution holds, and makes every execution that joins it due |

`terminal` is `true` for a forced cancel, which ends the execution at once.

**The request and every propagated request are one write.** A crash must not leave a parent cancelled and some of its children running.
