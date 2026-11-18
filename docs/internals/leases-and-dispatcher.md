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

`renewInterval` must be positive and shorter than `leaseDuration`, and `filter.executionIds` cannot be empty.

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

If loading state, releasing, or acknowledging fails unexpectedly, the dispatcher lets the lease expire safely. Each unacknowledged delivery increments the failure count and schedules retry via capped exponential backoff (`1_000 * 2 ** (n - 1)` ms, up to 300,000 ms).
