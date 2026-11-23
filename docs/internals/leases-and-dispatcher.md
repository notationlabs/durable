# Leases & Dispatcher

`ExecutionDispatcher` in Notation Durable polls for due executions, acquires time-bounded leases, renews active leases on a timer, aborts or kills deliveries on lease loss, and applies exponential backoff when deliveries fail before settlement.

## Poll loop & claim priority

The dispatcher bounds concurrent work by reserving free slots from `concurrency`, active runs, and in-flight claims. Each tick claims due work up to the free slot count with `leaseDuration` and any active ID filter.

`claimDue` selects non-terminal executions whose `nextClaimAt` timestamp is at or before the runtime clock, ordered by earliest `nextClaimAt` and then `executionId`. When at least one execution is claimed and slots remain, or when an in-flight delivery finishes, the dispatcher schedules the next claim immediately; idle polls wait `pollInterval`.

| Execution state | Claim eligibility |
| :--- | :--- |
| Terminal | Never claimed again |
| No held lease | Latest of `dueAt` or `redeliverAt` |
| Held lease | Latest of pending wakes or `leaseExpiresAt` plus backoff delay |

| Option | Default |
| :--- | :--- |
| `concurrency` | `10` active deliveries |
| `pollInterval` | `10` ms when idle |
| `leaseDuration` | `30_000` ms |
| `renewInterval` | `leaseDuration / 3` |
| `abortGracePeriod` | `1_000` ms |
| `maxExpiredDeliveries` | No limit |

`renewInterval` must be positive and shorter than `leaseDuration`, `filter.executionIds` cannot be empty, and `maxExpiredDeliveries` must be a positive integer.

## Fencing tokens (`dueAt` vs `claimedDueAt`)

Each claim atomically issues an `ExecutionLease` carrying a monotonically increasing `leaseToken` and expiration timestamp. To isolate concurrent wakes, `ExecutionState` separates `claimedDueAt` for the active lease from `dueAt` for wakes arriving mid-delivery.

```mermaid [lease fencing across overlapping claims]
sequenceDiagram
  participant W1 as Worker 1 (token 1)
  participant DB as Execution Store
  participant W2 as Worker 2 (token 2)
  W1->>DB: claimDue() -> lease token 1, expiresAt T1
  Note over W1,DB: W1 stalls past T1; dueAt stays isolated
  W2->>DB: claimDue() -> lease token 2, claimedDueAt merged
  W1->>DB: writeStep(lease token 1)
  DB-->>W1: { status: "lost" }
  W2->>DB: acknowledge(lease token 2, completed)
  DB-->>W2: { status: "completed" }
```

Every fenced operation – step writes, cancellation delivery, renewal, release, and acknowledgement – checks inside one atomic transaction that `leaseExpiresAt` is non-null and `leaseToken` equals `lease.token`. Clock expiry makes an execution claimable again, but an expired lease remains current until a successor claim or forced cancellation increments `leaseToken`.

When a delivery suspends, `claimedDueAt` clears and `dueAt` merges any next wake timestamp so mid-delivery wakes are preserved. Releasing a lease or claiming over an expired one keeps whichever wake timestamp is earlier.

## Lease renewal & hard termination

While a delivery runs, the dispatcher renews its lease every `renewInterval`, retrying transient renewal errors because the lease remains valid until superseded. Losing the lease stops renewal and aborts the delivery's `AbortController` with `LeaseLostError`.

| Mechanism | Behavior |
| :--- | :--- |
| SDK signal check | Throws `InfrastructureError` before advancing steps or fenced writes |
| Grace period abandonment | Abandons deliveries stuck past `abortGracePeriod` and frees the slot |
| Child worker abort | Kills the forked worker process immediately with `SIGKILL` |
| Worker IPC disconnect | Exits child process with code `75` if the parent runtime dies |

An abandoned delivery's late resolution or rejection is logged and ignored without calling `release` or `acknowledge`.

## Outcome classification & retry backoff

Normal workflow outcomes settle through `acknowledge`, resetting retry counts and clearing backoff timers. Infrastructure failures – such as worker crashes, lost leases, step conflicts, and aborted delivery signals – reject instead without acknowledging workflow progress.

| Rejection | Dispatcher action |
| :--- | :--- |
| Worker exited before reply | Leaves lease to expire so dying worker writes settle before reclaim |
| Lease lost on step write | Calls `release`, which returns `lost` without modifying state |
| Step conflict or runtime error | Calls `release` to schedule redelivery after backoff |
| Aborted delivery signal | Skips `release` and `acknowledge` after renewal loss or grace expiry |

If loading state, releasing, or acknowledging fails unexpectedly, the dispatcher lets the lease expire safely. Each unacknowledged delivery increments the failure count and schedules retry via capped exponential backoff (`1_000 * 2 ** (n - 1)` ms, up to 300,000 ms). A newly recorded step resets the count, as an acknowledgement does.

## Delivery limit

A delivery whose lease expires has ended without the dispatcher seeing it end. The usual causes are a worker that exits, a process that is reset, and a step that runs past its lease without the renewal reaching the runtime. If the cause is in the workflow, for example a step that always crashes its worker, the execution is delivered again after each backoff for as long as the runtime runs.

`maxExpiredDeliveries` sets how many consecutive deliveries of one execution may end with an expired lease. The dispatcher passes it to `claimDue`. The default is no limit.

`ExecutionState.expiredDeliveryCount` holds the count. The runtime changes it in these writes:

| Write | Effect on `expiredDeliveryCount` |
| :--- | :--- |
| Claim over an expired lease | Adds 1 |
| Release | No change |
| New step record, done or not | Resets to 0 |
| Step write that finds the record already written | No change |
| Cancellation recorded as delivered | Resets to 0 |
| Acknowledgement or forced cancellation | Resets to 0 |

A step write and an acknowledgement also reset `unacknowledgedDeliveryCount`, so the backoff starts again at 1,000 ms after the execution has made progress. A step write changes the `executions` row only when one of the counts is not 0.

A release does not count, because the dispatcher released the lease after a runtime client rejected: the dispatcher was running and the cause is outside the workflow. A replayed step writes nothing, so an execution whose delivery dies at the same step each time keeps counting.

`claimExecution` checks the limit in the same atomic write as the claim. When the claim over an expired lease would take the count to the limit, it records the execution `failed` with a `DeliveryLimitError` and takes no lease. The error has `code: "DELIVERY_LIMIT_REACHED"` and `deliveries`, the number of deliveries that ended with an expired lease. The adapter then treats the execution like a force-cancelled one: it deletes the store, effect and join waiters the execution holds, and wakes its join waiters, in the same write. `claimDue` returns no lease for the execution.

A claim that fails an execution still counts against the claim's `limit`, so the dispatcher starts fewer deliveries than it has free slots until its next claim. The dispatcher does not log the failure.

If a graceful cancellation is pending and not yet delivered when the limit is reached, the execution ends `failed` with the `DeliveryLimitError`, not `cancelled`. Its cancellation request stays on the record with `deliveredAt` null.

No workflow code runs for this failure, so a workflow cannot catch it. `workflowEndEmitter` does not emit it, because no delivery ends; `wait`, `poll`, `/wait`, `/events` and joins read it from the record like any other failure.
