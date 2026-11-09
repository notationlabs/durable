# `@notation/durable-conformance`

This package contains the runnable qualification suites for Durable runtimes: one for store connectors, one for the execution client, one for the effect client, one for the join client and one for cancellation. The normative protocol is documented in the [store connector conformance specification](https://github.com/notationlabs/durable/blob/main/docs/store-connector-spec.md).

Install it with the test runner and `@notation/durable`:

```sh
pnpm add -D @notation/durable @notation/durable-conformance vitest
```

Register the suite from a Vitest test file. The only required integration point is a factory that creates a clean backend and returns a `Runtime` built on it. Store wakes are observed through the runtime's `ExecutionClient`: the suite admits the executions that wait on stores, and a commit that satisfies a waiter must make that execution due, so `runtime.executions.claimDue()` returns it.

```ts
import { registerStoreClientConformance } from "@notation/durable-conformance";
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

The base factory qualifies a process-local runtime. A backend that supports multiple clients should also return `createPeer`, a runtime over the same backend as another process would open; a durable backend should additionally return `restart`. These capabilities activate the shared-client race and crash-recovery suites:

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

Run the test with either supported runtime:

```sh
vitest run runtime-conformance.test.ts
```

## Execution client suite

`registerExecutionClientConformance` checks a runtime's `ExecutionClient` and fenced heap writes: admission, wake merging, claims, acknowledgement and terminal outcomes; a store waiter combined with a suspended `wakeAt`, as a timed `when` leaves them; leases, renewal, release and the `running` status; the redelivery delay and `unacknowledgedDeliveryCount`; fencing of heap writes, renewals, releases and acknowledgements against a stale holder; and `start`, its idempotency by request ID, parameter validation, mismatches and the target's parent link. The suite passes a `clock` to `create`; build every runtime on it so the suite controls time, including lease expiry.

The harness has three optional capabilities:

- `createPeer` returns another runtime over the same backend, as a second runtime process would. The suite uses it to race two clients for one execution and to reclaim an expired lease from a different client than the one that holds it. Return it whenever several processes can share the backend; without it those cases run both sides on `runtime`, which proves less.
- `restart` reopens the same durable backend in a new runtime. It enables the restart cases; without it they are skipped.
- `failStartCommits` makes every new `start` on `runtime` fail after it begins writing but before it commits, and returns a function that stops the failures. It enables the case that a failed start leaves no trace; without it that case is skipped. The SQLite harness installs a temporary trigger that aborts the start-request insert.

```ts
import { registerExecutionClientConformance } from "@notation/durable-conformance";
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

`registerEffectClientConformance` checks a runtime's `EffectClient`: `begin` creating or reading one effect, mismatched operations and inputs, concurrent equal and different settlements and their conflict reports, `watch` registration and immediate wakes, settlement racing `watch`, waiter deletion with the step's result and its rejection for a stale holder, value encoding, and durability across a restart. Its harness takes the same `clock`, `createPeer` and `restart` as the execution client suite, plus `failEffectWatches`, which makes every `watch` on `runtime` fail before it commits and returns a function that stops the failures. Without it the case for a failed watch is skipped; the SQLite harness installs a temporary trigger that aborts the waiter insert.

```ts
import { registerEffectClientConformance } from "@notation/durable-conformance";

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

`registerJoinClientConformance` checks a runtime's `JoinClient`: `join` returning a terminal target's outcome or `not-found` and registering nothing, registering one waiter on a target that is not terminal, a terminal acknowledgement waking every join waiter and keeping them registered, `join` racing the acknowledgement, waiter deletion with the step's result, a stale holder's late registration, the parent link on a started child, and durability across a restart. Its harness takes the same `clock`, `createPeer` and `restart` as the execution client suite, plus `failExecutionWrites(executionId)`, which makes every write to that execution's record fail before it commits and returns a function that stops the failures. Without it the case for a crash during a terminal acknowledgement is skipped.

The suite covers the runtime adapter. The workflow-driver and delivery cases for `step.start`, `step.effect`, `step.join` and `step.call` are tested in the repository rather than here (`test/start.test.ts`, `test/effect.test.ts`, `test/effect-delivery.test.ts` and `test/join.test.ts`). The dispatcher and worker cases, such as concurrency, releasing on a retry reply, a worker that exits without replying, and a worker whose runtime process dies, are properties of `ExecutionDispatcher` and the invoker rather than of an adapter; the repository tests them in `test/dispatcher.test.ts` and `test/worker-invoker.test.ts`.

## Cancellation client suite

`registerCancellationClientConformance` checks a runtime's `ExecutionClient.cancel` and `HeapClient.recordCancellation`: a graceful request recorded and the execution made due, including during a live lease; a terminal execution left unchanged with its outcome returned; the first reason kept on a repeat; `EXECUTION_NOT_FOUND` for an unknown ID; a `TypeError` for an invalid request; a forced cancel recording `cancelled` at once and fencing out the lease holder, accepted after a graceful request, waking its join waiters and deleting the waiters it holds; propagation to children and grandchildren with the root as `source`, forced propagation, detached children, descendants that already have a request, terminal descendants, and children started before and after delivery; `detached` compared on a repeated start; `recordCancellation` fenced by the lease, writing the step records, deleting those steps' waiters and setting `deliveredAt`; a cleanup step's waiter waking the execution after delivery; and durability across a restart. Its harness takes the same `clock`, `createPeer`, `restart` and `failExecutionWrites(executionId)` as the join client suite. Without `failExecutionWrites` the cases that a forced cancel and a propagated request are all-or-nothing are skipped.

The workflow-driver cases, such as where `WorkflowCancelledError` is thrown and which outcome a workflow ends with, are tested in the repository (`test/cancellation.test.ts`).

