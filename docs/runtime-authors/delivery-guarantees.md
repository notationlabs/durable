# Delivery guarantees

A server can stop in the middle of a delivery: its process crashes, or its host loses the network. Another server must then deliver the execution, or it never finishes. But the first server may only be paused, and it may resume and write step records after the second server has taken over.

Durable uses leases to decide when another server may take over, and tokens to decide whose writes count. A runtime keeps both in its [execution client](./execution-client.md).

## Leases

A dispatcher claims an execution with `executions.claimDue`, which returns a lease:

```ts
type ExecutionLease = {
  executionId: string;
  token: number;
  expiresAt: number;
};
```

While the delivery runs, the dispatcher renews the lease with `executions.renew`. By default a lease lasts 30 seconds, and the dispatcher renews it every 10 seconds. A delivery ends when the dispatcher acknowledges its outcome with `executions.acknowledge`, or releases the lease with `executions.release` after an error.

**An execution has at most one live lease at a time**, across every runtime object that uses the same database. `claimDue` claims only executions without one.

If the dispatcher stops renewing, because its process died, the lease expires. Another dispatcher can then claim the execution. An expired lease stays current until another claim replaces it, so a dispatcher that is only slow keeps its lease if nobody else claims it.

## Tokens

Each claim of an execution issues a token that is higher than every earlier token for it. Every write made under a lease carries the lease, and the runtime checks its token in the same atomic step as the write:

| Fenced write | Client |
| --- | --- |
| `renew`, `release`, `acknowledge` | Execution client |
| `writeStep`, `recordCancellation` | Heap client |

If a higher token has been issued, the write changes nothing and returns `{ status: "lost" }`. **The delivery that receives `lost` stops at once, without acknowledging.** So once a second claim has taken over, nothing the first delivery writes is recorded.

Clocks decide only when a lease expires, and so when another claim may happen. Tokens decide whose writes count. Two servers whose clocks disagree can take over from each other too early, but their writes still cannot both count.

## Example: a paused server

Two servers share one database. Server A claims an execution, and then pauses for 40 seconds, for example while its virtual machine is moved to another host:

| Time | Server A | Server B |
| --- | --- | --- |
| 0 s | Claims the execution with token 1, and starts step `charge-card` | |
| 1 s | Pauses before it records `charge-card` | |
| 31 s | | The lease expired at 30 s, and the 1-second [redelivery delay](#deliveries-without-an-outcome) has passed. Claims the execution with token 2, runs `charge-card` and records it |
| 41 s | Resumes. Its write of `charge-card` returns `lost`, and it stops | |

The record of `charge-card` is server B's. But the card was charged twice, once by each server.

## What leases do not guarantee

**Leases and tokens make sure that one delivery's records count. They do not stop a step function from running twice.** A step function that runs before its delivery loses the lease has already acted, as in the example. So Durable delivers each step at least once. [Effects](../manual/effects.md) make an external call safe to repeat.

When a renewal returns `lost`, the dispatcher aborts the delivery's `signal`. The worker invoker then kills the worker process, so even a step function that never returns stops at once.

An invoker that runs workflows in the dispatcher's process cannot stop a step function. The dispatcher waits `abortGracePeriod`, 1 second by default, and then frees the delivery's slot. The step function may keep running, but every write it makes returns `lost`.

## Writes without a lease

Some writes during a delivery are not fenced. Each is safe to repeat, so a paused delivery that makes one late does no harm:

| Write | Why a late one is safe |
| --- | --- |
| `executions.start` | It is keyed by the calling execution and step. A repeat returns the child that the first call started |
| Store updates from a workflow | The store records each step's update. A repeat returns the recorded result without applying the update again |
| `effects.begin`, `settle` and `watch` | They are keyed by effect ID. A settlement that disagrees with the first is kept as a conflict report, and the first outcome stays |
| `joins.join` | A late registration causes at most one extra delivery of the joining execution |

## Deliveries without an outcome

A delivery that ends without an acknowledgement counts as unacknowledged. This happens when the dispatcher releases the lease after an error, or when the lease expires and another claim replaces it. The execution record counts these in `unacknowledgedDeliveryCount`, and an acknowledgement resets the count to zero.

After an unacknowledged delivery, the execution waits before it can be claimed again. The delay is 1 second after the first, and doubles after each one in a row, up to 5 minutes. `redeliveryDelay(count)` computes it. The [error reference](../manual/error-reference.md#failed-deliveries) lists what the dispatcher logs in each case.
