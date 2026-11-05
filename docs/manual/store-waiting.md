# Waiting on State

Two primitives suspend a workflow until a store reaches some condition: `when` observes, `take` consumes. Neither polls. The delivery ends as `suspended`, and a relevant write makes the execution due again.

## `store.when`

`when` waits once for a condition and returns the selected value. It does not mutate the store.

```ts
const message = yield* store.when("next-message", (s) =>
  s.messages.find((m) => !m.processed)
);
```

If the selector already returns a truthy value, the workflow continues immediately. Otherwise the execution suspends. On a missing store, `when` first creates it from the definition's `initial`, and the selector runs against that state. When a later write touches something the selector read, the workflow replays and the selector re-evaluates.

Inside a loop, put something that identifies the iteration in the key, because each key must be unique within the execution. The selector-only form, `store.when(selector)`, takes its key from its call site and is deprecated (see [Step keys](./replay-stability.md#step-keys)):

```ts
const message = yield* store.when(`next-message:${turn}`, (s) =>
  s.messages.find((m) => !m.processed)
);
```

## Waiting with a deadline

Pass `{ deadline }` to wait for the condition or for a time, whichever comes first. The deadline is an absolute timestamp in milliseconds. The step compares it with the runtime clock, which is `Date.now()` by default. Workflow code cannot read the runtime clock, so a runtime created with its own `clock` must keep that clock in step with `Date.now()`, as tests do when they fake both. Otherwise the comparison mixes two clocks. The timed form always takes an explicit key:

```ts
const approval = step.store(ApprovalStore, { id: orderId });

const result = yield* approval.when(
  `decision:${orderId}`,
  (state) => state.decision,
  { deadline: Date.now() + 24 * 60 * 60 * 1000 }
);

if (result.status === "timed-out") {
  yield* step.run(`cancel:${orderId}`, () => cancelOrder(orderId));
} else {
  // result.value is the decision
}
```

The timed form returns a `TimedWhenResult<R>`:

```ts
type TimedWhenResult<R> =
  | { status: "matched"; value: NonNullable<R> }
  | { status: "timed-out" };
```

The form without a deadline does not change, and returns the selected value. In the timed form, `deadline` is required and must be a finite number. A workflow whose deadline is optional therefore needs two call sites, one for each form, and must handle both return types:

```ts
const decision = deadline === undefined
  ? yield* approval.when(`decision:${orderId}`, (state) => state.decision)
  : yield* approval.when(`decision:${orderId}`, (state) => state.decision, { deadline });
```

The step records the first result it finds: `matched` if the selector matches, otherwise `timed-out` once the deadline has passed. The rules are:

- **The first evaluation records the deadline.** The step is recorded unfinished with the deadline as its wake time, as `step.delay` records its wake time. Later evaluations use the recorded deadline and ignore the argument. So a deadline computed from `Date.now()` does not move when the workflow replays. If the unfinished record has no deadline, because an earlier version of the workflow used the same key without one, the step uses the argument and records it.
- **The selector runs first.** Each evaluation runs the selector before it compares the deadline with the clock. A match returns `matched`, even when the deadline has already passed. Without a match, a deadline that has passed returns `timed-out`.
- **Only the first recorded result counts.** Once the step records `timed-out`, a later matching update cannot replace it, and replay returns `timed-out`. The same holds for `matched`.
- **A commit that does not match changes nothing.** The commit makes the execution due, the selector runs again, and the step suspends again with the recorded deadline as `wakeAt`.
- **The deadline never ends the wait early.** The step does not time out before the deadline. The runtime makes the execution due at the deadline, but it does not promise to deliver it at that exact moment.
- **Recording the result removes both wakes.** The write that records the result deletes the waiter. The deadline wake needs no removal: once the step has a result, the delivery no longer reports the deadline as its wake time, and acknowledging that delivery drops the old wake.

An unresolved evaluation takes these steps in order, as a `when` or `take` without a deadline does:

1. It records the unfinished step with the deadline.
2. It registers the waiter.
3. It ends the delivery as `suspended` with `wakeAt` set to the deadline.

If the worker crashes before the delivery is acknowledged, the execution is delivered again. The redelivery reads the recorded deadline if step 1 completed, runs the selector again, and registers the waiter again. A worker whose lease has been taken over cannot record a result, so it cannot delete the waiter of the worker that holds the lease now.

A timed `when` covers one store and one deadline. It cannot wait on several stores.

## How wake-ups work

The selector runs against a tracking proxy – a wrapper around the state that records which paths the selector read. If the selector does not match, the runtime stores those paths in a _waiter_: the store name and ID, the store version the selector saw, the read paths, and the waiting `executionId` and step key. The waiter carries no copy of the workflow event.

When an update later writes to an overlapping path, the runtime requests a wake for the waiter's execution in the same transaction as the write. So once the write is durable, the wake is durable too, and a crash cannot commit one without the other. Appending to `messages` wakes a selector that read `s.messages`; replacing `profile` wakes a selector that read `s.profile.name`. A write to `status` does not wake a selector that only read `messages`.

Why does this matter? Because the selector itself is never serialised. The recorded paths are only a wake-up filter – on wake, the workflow replays and the selector re-runs against the real committed state. If a wake turns out to be spurious, the selector re-evaluates to falsy and the execution suspends again.

Two more rules follow from waking the execution rather than queueing a copy of its event:

- **Waiters persist until their step finishes.** The runtime deletes a waiter in the same write that records its step's result, not when the wake fires. If the delivery that a wake starts crashes before the step finishes, the waiter is still registered, so a later matching write still wakes the execution.
- **Many wakes collapse into one delivery.** Wakes are state on the execution record, so ten writes that satisfy the same waiter before the dispatcher claims it produce one delivery, not ten. A store wake and a pending `step.delay` for the same execution also merge, and the execution is delivered at the earlier time.

Selectors must be synchronous and pure: no network, no timers, no mutation (the proxy throws if you try). The selected value is recorded as the step result, so it must be a durable value (see [Step values](./steps.md#step-values)); otherwise the step throws `WorkflowValueError` (`INVALID_STEP_VALUE`).

## `store.take`

`when` observes; it does not claim. If two executions both `when` on the same unprocessed message, both receive it. For mailboxes and queues where exactly one consumer should win each item, use `take`:

```ts
const msg = yield* store.take(
  `next:${turn}`,
  (s) => s.messages.find((m) => !m.processed),
  (draft, msg) => {
    draft.messages.find((m) => m.id === msg.id)!.processed = true;
  }
);
```

The selector and the claim run inside the _same_ store transaction, committing as one version bump. When two workers race, one commits its claim first. The other's selector then re-evaluates against the claimed state, finds the message already processed, and moves on – so both workers cannot receive the same message.

`take` requires an explicit key, and the claim mutation follows the same rules as an updater.

## Example: an agent loop

```ts
export const agent = workflow(async function* (step, event) {
  const store = step.store(AgentStore, { id: event.params.agentId });

  let turn = 0;

  while (true) {
    const msg = yield* store.take(
      `next-message:${turn++}`,
      (s) => s.messages.find((m) => !m.processed),
      (draft, m) => {
        draft.messages.find((x) => x.id === m.id)!.processed = true;
      }
    );

    const response = yield* step.run(`llm:${msg.id}`, () =>
      callModel(msg.content)
    );

    yield* store.update(`reply:${msg.id}`, (draft) => {
      draft.messages.push(response);
    });
  }
});
```

Between turns the workflow costs nothing – no process, no timer, no poll. A new message (from another workflow, or [written externally](./store-external.md)) wakes it.
