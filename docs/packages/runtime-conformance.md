# @yieldstar/runtime-conformance

Vitest qualification suites for Yieldstar runtimes. `registerStoreClientConformance` verifies a `StoreClient`'s store lifecycle, concurrency, replay idempotency, waiter wake-up, wake collapsing, and crash-recovery behavior. `registerExecutionClientConformance` verifies a runtime's `ExecutionClient` and fenced heap writes against RFC 001, RFC 002 and RFC 003. `registerEffectClientConformance` verifies its `EffectClient` against RFC 005. `registerJoinClientConformance` verifies its `JoinClient` and the join wakes of a terminal acknowledgement against RFC 009. `registerCancellationClientConformance` verifies `ExecutionClient.cancel`, its propagation to descendants and `HeapClient.recordCancellation` against RFC 010. `registerReplayStabilityClientConformance` verifies step kinds, `HeapClient.listSteps`, the recorded workflow version, divergence and `ExecutionClient.clearDivergence` against RFC 012. `registerRetentionClientConformance` verifies pruning, tombstones and the cleanup of a pruned execution's data against RFC 013.

## Install

```sh
pnpm add -D @yieldstar/core @yieldstar/runtime-conformance vitest
```

## Register the suite

Create a Vitest test file and provide a factory that creates a clean backend and returns a `Runtime` built on it. The suite observes store wakes through the runtime's `ExecutionClient`: it admits the executions that wait on stores, and a commit that satisfies a waiter must make that execution due, so `runtime.executions.claimDue()` returns it.

```ts
import { registerStoreClientConformance } from "@yieldstar/runtime-conformance";
import { MyRuntime } from "./my-runtime";

registerStoreClientConformance({
  name: "my-store",
  create() {
    const backend = createCleanBackend();
    return {
      runtime: new MyRuntime(backend),
      dispose: () => backend.close(),
    };
  },
});
```

Run it with either supported test host:

```sh
vitest run runtime-conformance.test.ts
```

## Optional capabilities

Return `createPeer` when the backend supports multiple runtimes sharing the same data. This enables the cross-client race suites. The execution client suite uses the same capability for its lease races (see below).

Return `restart` when the backend is durable. This enables the suites that check commits, wakes and waiters survive a restart.

```ts
create() {
  const backend = createCleanBackend();
  const runtime = new MyRuntime(backend);

  return {
    runtime,
    createPeer: () => new MyRuntime(backend),
    restart: async () => {
      await runtime.close();
      return new MyRuntime(backend.reopen());
    },
    dispose: () => backend.close(),
  };
}
```

## Execution client suite

`registerExecutionClientConformance` checks a runtime's `ExecutionClient` and fenced heap writes against RFC 001, RFC 002, RFC 003, RFC 007 and RFC 008: admission, wake merging, claims, acknowledgement and terminal outcomes; a waiter combined with a suspended `wakeAt`, as a timed `when` leaves them; leases, renewal, release and the `running` status; the redelivery delay and `unacknowledgedDeliveryCount`; fencing of heap writes, renewals, releases and acknowledgements against a stale holder; `start`, its idempotency by request ID, parameter validation, mismatches and the target's parent link; idempotent admission, its matching on `workflowId` and canonical `params` but not `context`, mismatches, terminal and non-idempotent originals, and concurrent repeats; and `list`, its order by admission rather than `admittedAt`, page sizes and cursors, filters, invalid requests, and paging while new executions are admitted. The suite passes a `clock` to `create`; build every runtime on it so the suite controls time, including lease expiry.

The harness has three optional capabilities:

- `createPeer` returns another runtime over the same backend, as a second runtime process would. The suite uses it to race two clients for one execution, to reclaim an expired lease from a different client than the one that holds it, and to check that `list` orders executions that two clients admit by admission when their clocks disagree. Return it whenever several processes can share the backend; without it those cases run both sides on `runtime`, which proves less.
- `restart` reopens the same durable backend in a new runtime. It enables the restart cases; without it they are skipped.
- `failStartCommits` makes every new `start` on `runtime` fail after it begins writing but before it commits, and returns a function that stops the failures. It enables the case that a failed start leaves no trace; without it that case is skipped. The SQLite harness installs a temporary trigger that aborts the start-request insert.

```ts
import { registerExecutionClientConformance } from "@yieldstar/runtime-conformance";
import { MyRuntime } from "./my-runtime";

registerExecutionClientConformance({
  name: "my-runtime",
  create({ clock }) {
    const backend = createCleanBackend();
    return {
      runtime: new MyRuntime(backend, { clock }),
      // A separate connection, as another process would open.
      createPeer: () => new MyRuntime(backend.connect(), { clock }),
      restart: () => new MyRuntime(backend.reopen(), { clock }),
      dispose: () => backend.close(),
    };
  },
});
```

## Effect client suite

`registerEffectClientConformance` checks a runtime's `EffectClient` against RFC 005: `begin` creating or reading one effect, mismatched operations and inputs, concurrent equal and different settlements and their conflict reports, failed effects and conflicts between a result and a failure, `watch` registration and immediate wakes, settlement racing `watch`, waiter deletion with the step's result and its rejection for a stale holder, value encoding, and durability across a restart. Its harness takes the same `clock`, `createPeer` and `restart` as the execution client suite, plus `failEffectWatches`, which makes every `watch` on `runtime` fail before it commits and returns a function that stops the failures. Without it the case for a failed watch is skipped; the SQLite harness installs a temporary trigger that aborts the waiter insert.

```ts
import { registerEffectClientConformance } from "@yieldstar/runtime-conformance";

registerEffectClientConformance({
  name: "my-runtime",
  create({ clock }) {
    const backend = createCleanBackend();
    return {
      runtime: new MyRuntime(backend, { clock }),
      createPeer: () => new MyRuntime(backend.connect(), { clock }),
      restart: () => new MyRuntime(backend.reopen(), { clock }),
      dispose: () => backend.close(),
    };
  },
});
```

## Join client suite

`registerJoinClientConformance` checks a runtime's `JoinClient` against RFC 009: `join` returning a terminal target's outcome or `not-found` and registering nothing; `join` registering one waiter on an admitted, running or suspended target, without a wake of its own; a terminal acknowledgement waking every join waiter on the target and keeping the waiters registered; no wake for a suspended acknowledgement or a terminal joiner; a joiner that registers during its own delivery and is woken after it suspends; `join` racing the terminal acknowledgement; deletion of a waiter with its step's result, and its survival after an unfinished write or a stale holder's rejected write; a stale holder's late registration causing at most one extra delivery; the parent link on a started child's record and view; and durability of waiters and wakes across a restart. Its harness takes the same `clock`, `createPeer` and `restart` as the execution client suite, plus `failExecutionWrites(executionId)`, which makes every write to that execution's record on `runtime` fail before it commits and returns a function that stops the failures. It enables the case that a terminal acknowledgement records its outcome and every joiner's wake together or not at all; without it that case is skipped. The SQLite harness installs a temporary trigger that aborts updates to the execution's row.

## Cancellation client suite

`registerCancellationClientConformance` checks a runtime's `ExecutionClient.cancel` and `HeapClient.recordCancellation` against RFC 010: a graceful request recorded and the execution made due, including an admitted execution that never ran and one with a live lease; a terminal execution left unchanged with its outcome returned; the first `reason` and `requestedAt` kept on a repeat; `EXECUTION_NOT_FOUND` for an unknown ID; a `TypeError` for an invalid request, with nothing changed; a forced cancel recording `cancelled` at once, clearing the pending wake and making the lease holder's next fenced write return `lost`; a forced cancel after a graceful one keeping the first reason and setting `forced`; a forced cancel waking its join waiters, a join on it returning the cancelled outcome, and the deletion of the store, effect and join waiters it holds; propagation to every non-terminal, non-detached child and grandchild with the root as `source`, and of `force`; a detached child's parent link and its exclusion from propagation; a descendant with an earlier request keeping its `reason` and `source`; terminal descendants left unchanged and not propagated through; a child admitted after the parent's request but before delivery receiving a request, and one started after `recordCancellation` receiving none; `detached` compared on a repeated start; `recordCancellation` returning `lost` for a stale or force-ended lease, writing each step's done record, keeping an identical existing record at the same attempt, writing nothing when a step already has a different record, deleting those steps' waiters so later commits, settlements and joins do not wake the execution, and setting `deliveredAt`; a cleanup step's waiter registered after delivery waking the execution; and durability of requests, delivery, detached parent links and start requests across a restart. Its harness takes the same `clock`, `createPeer`, `restart` and `failExecutionWrites(executionId)` as the join client suite. `failExecutionWrites` enables the cases that a forced cancel records its outcome and its join wakes together, that a request reaches every descendant or none, and that `recordCancellation` writes its records, waiter deletions and `deliveredAt` together or not at all; without it those cases are skipped.

## Replay stability client suite

`registerReplayStabilityClientConformance` checks a runtime's adapter against RFC 012: `HeapClient.listSteps` listing every step key with a record, with the kind of its latest attempt, and only the execution's own steps; the kind that `writeStep` and `recordCancellation` take as `stepKind` stored as `meta.kind`, and absent when none was given; no listing or version for a write a stale lease made; the workflow version recorded by the first fenced write that gives one, whether a step write, `recordCancellation` or an acknowledgement, and never overwritten; a delivery released before any fenced write leaving the next delivery to record its own version; a `diverged` acknowledgement recording `divergence` with its code, step key, the recorded version (else the outcome's, else 1) and the detection time, ending the lease, keeping the `admitted` or `suspended` status, not counting as an unacknowledged delivery, and returning `lost` for a stale lease; a diverged execution not delivered by pending or new wakes, by a graceful cancel request, or after any delay; a forced cancel ending a diverged execution and clearing `divergence`; `clearDivergence` from any runtime removing `divergence` so the pending wakes deliver the execution, the next delivery diverging again or completing, `not-diverged` with nothing changed for a healthy, terminal or already cleared execution, and `EXECUTION_NOT_FOUND` for an unknown ID; `list({ diverged })` with `true` and `false`, combined with `status` and a cursor, a null value treated as absent, and `ExecutionListRequestError` for a non-boolean; and durability of step kinds, the version and the divergence across a restart. Its harness takes the same `clock`, `createPeer` and `restart` as the execution client suite.

## Retention client suite

`registerRetentionClientConformance` checks a runtime's adapter against RFC 013: `ExecutionClient.prune`, `listPendingPrunes` and `finishPrune`, the cleanup methods `HeapClient.deleteSteps`, `StoreClient.deleteExecutionData`, `EffectClient.deleteWaitersForExecution` and `JoinClient.deleteWaitersForExecution`, and `Runtime.prune` over them. The cases are: a completed, failed or cancelled execution pruned once its policy's period has passed since its outcome was recorded, and its tombstone kept for good; the deletion of every step record, including earlier attempts, `step.all` and `step.race` decision records and patch markers, of its store receipts, so a repeated update applies again, and of the start requests it owns as the caller; the tombstone returned by `get`, `list({ status: "pruned" })` and `cancel`, excluded from the default `list`, `EXECUTION_ID_CONFLICT` for a new admission and `EXECUTION_PRUNED` for an idempotent one, `not-diverged` from `clearDivergence`, `ignored` from `requestWake`, and no claim; requests that leave a terminal execution unchanged not restarting the retention clock; no pruning of an admitted, running, suspended, cancel-requested or diverged execution of any age, of a runtime without a policy, or of an outcome without a period; a workflow's policy replacing the default as a whole; a shortened policy applying to executions that are already terminal; `limit` and the oldest-outcome-first order; a pruned parent's children, their parent link and their propagated cancellation left unchanged; shared stores and effect records kept; the execution's own stores kept without `pruneExecutionStores` and deleted under every definition name with it; a pass that stopped after the tombstone, or part way through the cleanup, finished by the next pass, including after a restart; every cleanup step safe to repeat, and `finishPrune` changing nothing for an execution that is not pruned; readers seeing the terminal record or the tombstone during a prune, never a partial record; and two runtimes pruning at once tombstoning each execution once and reaching the same end state.

For joins, it checks that a target whose joiner was woken but not delivered, or whose joiner is parked by a divergence, is not pruned until the joiner records its join result; that once the result is recorded the waiter is gone and the target is pruned when its retention expires; that `join` on a pruned target returns `pruned` and registers nothing; that a `join` racing the pruner returns the outcome or `pruned` and never registers a waiter on a pruned target; that waiters of terminal joiners do not keep a target, and the tombstone deletes them; and that pruning a joiner deletes the waiters it left on a live target.

The suite passes `clock` and each case's `retain` options to `create`; build `runtime`, and every peer and restarted runtime, with both. Its harness takes the same `createPeer` and `restart` as the execution client suite. Without `restart`, the restart case is skipped. Cases that change a policy call `ExecutionClient.prune` with the new options rather than reopening the runtime.

RFC 013's dispatcher, SDK and route cases, such as periodic pruning, `JOIN_TARGET_PRUNED` thrown by `step.join`, `ExecutionPrunedError` and the HTTP status codes, are tested in the repository (`test/retention.test.ts` and the other `test/retention-*.test.ts` files).

RFC 012's workflow-driver cases, such as divergence detection, `step.patched`, `requireStepKeys` and the version exposed as `event.workflowVersion`, are tested in the repository (`test/replay-stability.test.ts`), and the `clear-divergence` route in `test/clear-divergence.test.ts`.

RFC 010's workflow-driver cases, such as the step at which `WorkflowCancelledError` is thrown, replay of a delivered cancellation, and the outcome of a workflow that catches it, are tested in the repository (`test/cancellation.test.ts`).

The suite covers the runtime adapter. RFC 003's workflow-driver and delivery cases for `step.start`, RFC 005's for `step.effect`, RFC 007's for a timed `when`, and RFC 009's for `step.join` and `step.call`, are tested in the repository rather than here (`test/start.test.ts`, `test/effect.test.ts`, `test/effect-delivery.test.ts`, `test/timed-when.test.ts` and `test/join.test.ts`). RFC 002's dispatcher and worker cases, such as concurrency, releasing on a retry reply, a worker that exits without replying, and a worker whose runtime process dies, are properties of `ExecutionDispatcher` and the invoker rather than of an adapter; the repository tests them in `test/dispatcher.test.ts` and `test/worker-invoker.test.ts`.

The normative contract is in the repository's [store connector conformance specification](https://github.com/notationlabs/yieldstar/blob/main/docs/store-connector-spec.md).
