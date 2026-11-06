# Replay Stability

Each delivery runs the workflow from the top, and each step looks up its record by its step key. A step with a record returns the recorded value instead of running again. Replay is therefore correct only while the code requests the same step keys that the execution's history recorded.

A deploy can break this for executions that are in flight. This page describes how Yieldstar detects that the code no longer matches the history, what happens to such an execution, and the two ways to change a workflow safely while executions are in flight: workflow versions and patches.

The examples on this page use one workflow, `place-order`. It reserves stock, charges the card, and then waits up to two days for a courier:

```ts
export const placeOrder = workflow(async function* (step, event) {
  const { orderId } = event.params;
  yield* step.run("reserve-stock", () => warehouse.reserve(orderId));
  yield* step.run("charge", () => payments.charge(orderId));
  yield* step.delay("courier", 2 * DAY);
  return "shipped";
});
```

Order 42 is waiting on the courier. Its history holds three records: `reserve-stock`, `charge` and `courier`.

## Step keys

Most steps take their key as the first argument. Five forms can also be called without a key:

| Keyless form              | Keyed form                       |
| ------------------------- | -------------------------------- |
| `step.run(fn)`            | `step.run("key", fn)`            |
| `step.delay(ms)`          | `step.delay("key", ms)`          |
| `step.poll(opts, fn)`     | `step.poll("key", opts, fn)`     |
| `store.get()`             | `store.get("key")`               |
| `store.when(selector)`    | `store.when("key", selector)`    |

A keyless step takes its key from a SHA-1 hash of its call site: the file path, line and column. Adding a line above the step, reformatting the file, changing the bundler or moving the project directory all change that key. Suppose `charge` had no key, and a developer added a logging step above it while order 42 waits:

```ts
yield* step.run("reserve-stock", () => warehouse.reserve(orderId));
yield* step.run("log-order", () => log(orderId)); // new line
yield* step.run(() => payments.charge(orderId)); // same code, new call-site key
```

The next delivery of order 42 requests the charge under a key that has no record. Before this release, replay ran the step again and charged the card a second time. Now the delivery diverges instead (see [Divergence](#divergence)), and the card is not charged.

**Keyless steps are deprecated.** Give every step an explicit key, and keep the key stable across deploys.

### The deprecation warning

When a keyless step has no record and is about to run, the driver logs a warning through the delivery's logger:

```
Deprecated: a step without a key takes its key from its call site, so editing the workflow file can make replay run it again. Pass an explicit key.
```

The step then runs as before. The warning carries the `executionId` and the call-site `stepKey`.

### `requireStepKeys`

The SQLite and memory runtimes take a `requireStepKeys` option:

```ts
const runtime = new SqliteRuntime(db, { requireStepKeys: true });
```

With `requireStepKeys: true`, a keyless step that has no record does not run. The delivery diverges with code `STEP_KEY_REQUIRED`, and the execution is parked as for any [divergence](#what-a-divergence-does). Nothing is recorded for the step, and workflow code does not see an error.

To fix it, give the step an explicit key, deploy, and call `clearDivergence`. The next delivery runs the step under its new key.

A keyless step that already has a record under its call-site key replays as usual. This is important because it means that turning the option on does not break executions that are in flight: their keyless steps that have already run keep their records.

`WorkflowRunner` reads the option from the runtime it was constructed with. Under the [worker invoker](../packages/worker-invoker.md), that is the runtime constructed in the worker, so set the option there. Setting it only on the dispatcher's runtime has no effect.

The option is off by default. It will default to on in the next major version.

## Divergence

A delivery diverges when the workflow code no longer matches the execution's history. The driver detects it with one rule: **no step does new work while a recorded step has not been requested.**

### Consumed steps

At the start of each delivery, the driver reads the list of step keys the execution has records for. During the delivery, a recorded step counts as consumed once workflow code requests its key.

For order 42 on the original code, the delivery after the courier delay requests `reserve-stock`, `charge` and `courier` in turn. All three records are consumed, and the workflow returns.

### When the driver checks

The driver checks that every recorded step is consumed at these points, outside any [`step.all` or `step.race`](./concurrent-steps.md) branch:

- before a step runs that has no record;
- before a step continues that has an unfinished record, such as a store, join or effect wait, a retry backoff, or a `step.delay` whose time has not passed; and
- before the delivery records `completed`, `failed` or `cancelled`, or ends `suspended`.

Two more cases diverge at any point:

- Workflow code requests a recorded key as a different kind of step, such as `step.delay("charge", ms)` for a key recorded by `step.run`.
- A `step.run` schema rejects, on replay, a value it accepted on the first run (see [Schema changes](#schema-changes)).

### Example: a renamed step

The shop starts charging tax, and a developer replaces `charge` with `charge-v3`:

```ts
yield* step.run("reserve-stock", () => warehouse.reserve(orderId));
yield* step.run("charge-v3", () => payments.chargeWithTax(orderId));
yield* step.delay("courier", 2 * DAY);
```

The next delivery of order 42 goes as follows:

1. `reserve-stock` replays from its record, and is consumed.
2. `charge-v3` has no record, so the driver checks the history before running it.
3. The record for `charge` has not been consumed, so the delivery diverges. `charge-v3` does not run.

Code that is edited to return or throw before a recorded step diverges in the same way. The check before `completed` or `failed` finds the unconsumed record, so the edit cannot end the execution. When the new code throws, for example a `TypeError` from a bug, the divergence's `message` names that error, and the runner's warning carries it as the divergence error's `cause`.

A new step inserted before a recorded step also diverges. If the developer adds `step.run("send-receipt", ...)` between `charge` and `courier`, order 42 reaches `send-receipt` with `courier` unconsumed. A new step after the last recorded step is safe, because by then every record has been consumed. [Patches](#patches) make the first kind of edit safe.

### What a divergence does

Divergence is not a workflow error. The driver ends the delivery itself, and does not throw into workflow code:

- **Workflow code cannot catch it.** A `try` around the diverging step does not reach its `catch` or `finally` block.
- **No new work starts.** The step that would have run does not run, and no other branch starts a step. A step already in flight in another `step.all` or `step.race` branch is still recorded, so it does not run again after the divergence is cleared.
- **No outcome is recorded.** The runner logs a warning with the divergence error as `err`, and resolves with `{ status: "diverged", code, stepKey }`. The acknowledgement records a `divergence` on the execution record instead of an outcome, and the status stays as it was, such as `suspended`.
- **The execution is parked.** While `divergence` is set, no wake delivers the execution. Wakes from delays, store commits and joins stay pending, so the execution does not run again in a loop.
- **It is not a failed delivery.** The acknowledgement does not add to `unacknowledgedDeliveryCount`.

For order 42, the record now holds:

```ts
{
  status: "suspended",
  workflowVersion: 1,
  divergence: {
    code: "NONDETERMINISTIC_REPLAY",
    stepKey: "charge-v3", // the step that would have run, when there was one
    message: 'Step "charge-v3" would run, but replay did not reach recorded step "charge"',
    detectedAt: 1793750400000,
  },
}
```

| `divergence.code`         | Cause                                                              |
| ------------------------- | ------------------------------------------------------------------ |
| `NONDETERMINISTIC_REPLAY` | A recorded step was not requested, a recorded key was requested as another kind of step, or a settled `step.all` or `step.race` no longer reaches its recorded decision |
| `STEP_SCHEMA_CHANGED`     | A `step.run` schema rejects on replay a value it accepted on the first run |
| `STEP_KEY_REQUIRED`       | A keyless step has no record, and the runtime was created with [`requireStepKeys`](#requirestepkeys) |

`message` says what replay found, such as the recorded step it did not reach, or the error workflow code threw before reaching it. It is stored on the record, so an operator can read it from `list` without the delivery's logs.

The step kind strings, such as `run` and `store.when`, are stored with each record. A timed and an untimed `when` share the kind `store.when`, so adding a deadline to a `when` that is still waiting replays without divergence. Switching a `when` that has finished between the two forms is not detected: replay returns the recorded value in the shape of the form that recorded it.

A [graceful cancellation](./cancellation.md) of a diverged execution is recorded but not delivered, because no delivery runs while the divergence is set.

### Finding diverged executions

`list` takes a `diverged` filter, and every view carries `divergence` while it is set:

```ts
const page = await sdk.list({ workflowId: "place-order", diverged: true });
for (const execution of page.executions) {
  console.log(execution.executionId, execution.divergence);
}
```

`diverged: false` returns only executions without a divergence.

### Clearing a divergence

An operator has two ways to resolve a diverged execution:

1. Deploy code that matches the history, and call `clearDivergence`. Usually the fix is to keep the old step for executions that recorded it, behind a [version gate](#workflow-versions) or a [patch](#patches).
2. Force-cancel the execution with `sdk.cancel(executionId, { force: true })` (see [Forced cancellation](./cancellation.md#forced-cancellation)).

`clearDivergence` removes the divergence and makes the execution due now:

```ts
const result = await sdk.clearDivergence("order-42");
// { status: "cleared" }, { status: "not-diverged" }, or undefined for an unknown ID
```

The next delivery replays the execution on the code deployed by then. If that code still diverges, the delivery sets `divergence` again with a new `detectedAt`. An execution without a divergence is not changed, and the result is `not-diverged`. The HTTP SDK posts to `/clear-divergence`, and observation middleware authorizes it with `operation: "clear-divergence"` (see [Observation middleware](./http-server.md#observation-middleware)).

A terminal outcome clears `divergence`, so a forced cancel leaves a `cancelled` execution with no divergence.

### Inside `step.all` and `step.race`

Branches of a combinator interleave differently on each delivery, so the check before each step does not apply inside a branch. Every branch replays through its recorded steps on every delivery, settled or not, so each recorded branch step is consumed by the step that recorded it. A losing race branch that recorded steps therefore does not diverge.

Two checks still cover combinators:

- A settled combinator whose branches no longer reach its recorded decision diverges. For example, a `step.all` recorded `fulfilled`, and a branch now throws.
- Before the delivery ends, every recorded step must be consumed. An unsettled combinator whose branch step was renamed diverges at that point.

**Detection inside an unsettled combinator happens late.** The check runs only at the end of the delivery, so a new branch step may already have run and been recorded. If `place-order` renames a step inside a `step.all` that has not settled, the renamed step runs before the delivery diverges. Gate changes to branches on a [workflow version](#workflow-versions).

`step.patched` cannot be called inside a branch (see [Patches inside combinators](#patches-inside-combinators)).

## Workflow versions

A workflow can declare a version. The first step write of each execution records it, and every later delivery exposes the recorded version as `event.workflowVersion`, whatever version the current code declares:

```ts
export const placeOrder = workflow({ version: 3 }, async function* (step, event) {
  const { orderId } = event.params;
  yield* step.run("reserve-stock", () => warehouse.reserve(orderId));
  if (event.workflowVersion < 3) {
    yield* step.run("charge", () => payments.charge(orderId));
  } else {
    yield* step.run("charge-v3", () => payments.chargeWithTax(orderId));
  }
  yield* step.delay("courier", 2 * DAY);
  return "shipped";
});
```

Order 42 started under version 2, so it keeps the `charge` step and replays without divergence. A new order records version 3 and charges tax.

- A version is a non-negative integer. `workflow` throws a `TypeError` for any other value.
- A workflow that declares no version is version 1.
- The version is recorded by the execution's first step record or cancellation record. The driver reads it back with the step keys at the start of each delivery. A workflow that fails before any step records no version.
- If a delivery ends before any step record, for example because the worker crashed, no history exists yet. The next delivery is again a first delivery and records the version deployed by then. A version deployed between admission and the first delivery is therefore the one the execution records.
- Execution views carry `workflowVersion` once it is recorded.

### Executions from before this release

An execution that was in flight before the upgrade has no recorded version. If it already has step records, its next delivery exposes version 1, whatever the code declares, and records it. Code from before this release declared no version, so version 1 is the version it ran under. An execution with no step records is treated as a first delivery and records the declared version.

## Patches

`step.patched(id)` makes a smaller change than a version. It answers one question: did this execution reach this point before or after the patch was deployed?

The shop wants to email a receipt after the charge. The new step goes between `charge` and `courier`, which would make order 42 diverge (see [Example: a renamed step](#example-a-renamed-step)). With a patch, it does not:

```ts
yield* step.run("charge", () => payments.charge(orderId));
if (yield* step.patched("send-receipt")) {
  yield* step.run("receipt", () => mail.receipt(orderId));
}
yield* step.delay("courier", 2 * DAY);
```

`step.patched` decides in three steps:

1. If a marker for `send-receipt` is recorded, it returns `true`.
2. Otherwise, if any recorded step has not been consumed, the original run passed this point without the patch. It returns `false` and records nothing.
3. Otherwise replay is at the end of the history. It records a marker under the step key `$$patch$$:send-receipt` and returns `true`.

| Execution                                        | `step.patched("send-receipt")` | Why |
| ------------------------------------------------ | ------------------------------ | --- |
| Order 42, waiting on `courier`                   | `false`                        | `courier` is recorded and not yet consumed |
| Order 43, admitted after the deploy              | `true`, and records a marker   | Nothing after this point is recorded |
| Order 43, on a later delivery                    | `true`                         | The marker is recorded |
| An order suspended on `charge`'s retry backoff at the deploy | `true`, and records a marker | Every record is consumed when it reaches the patch |

A second `step.patched` call with the same ID in the same delivery returns the same answer as the first.

A marker counts as a recorded step. Keep the `step.patched` call in the code while any execution that recorded the marker can still be delivered: removing it leaves the marker unconsumed, and the execution diverges.

### Patches inside combinators

Inside a `step.all` or `step.race` branch, whether the execution passed a point depends on how the branches interleave. A `step.patched` call inside any branch therefore throws `WorkflowReplayError` with code `PATCH_IN_COMBINATOR` into the branch. Nothing is recorded: replay reaches the same call inside a branch and throws the same error again. Two branches that call `patched` with the same ID both throw `PATCH_IN_COMBINATOR`, not a duplicate-key error.

Call `step.patched` before the combinator and pass the result in, or gate the change on `event.workflowVersion`:

```ts
const sendReceipt = yield* step.patched("send-receipt");
yield* step.all("notify", [
  step.run("notify-warehouse", () => warehouse.notify(orderId)),
  ...(sendReceipt ? [step.run("receipt", () => mail.receipt(orderId))] : []),
]);
```

## Schema changes

A `step.run` [schema](./steps.md#schemas) validates the recorded value on every replay, and the step record notes whether the schema accepted the value on the first run. If a newer schema rejects a value that the first run accepted, the history says the value was valid and the code disagrees. The delivery diverges with code `STEP_SCHEMA_CHANGED`, and the step does not throw.

A value that the schema rejected on the first run keeps throwing `WorkflowValueError` with code `STEP_SCHEMA_INVALID` on replay, as recorded.

Gate a schema change on `event.workflowVersion` or `step.patched`, so executions that recorded values under the old schema keep validating them with it.

## Errors

| Error or code | Reaches workflow code | Cause |
| --- | --- | --- |
| `WorkflowReplayError` `PATCH_IN_COMBINATOR` | Yes, not recorded | `step.patched` was called inside a `step.all` or `step.race` branch |
| Divergence `NONDETERMINISTIC_REPLAY` | No | The code no longer matches the history |
| Divergence `STEP_SCHEMA_CHANGED` | No | A schema rejects on replay a value it accepted on the first run |
| Divergence `STEP_KEY_REQUIRED` | No | A keyless step has no record under `requireStepKeys` |

`WorkflowReplayError` has `code` and `stepKey`, and is registered by default, so replay revives it as the same class.

## Upgrading

Code edits that used to re-run steps silently now park executions. After you deploy this release, list executions with `diverged: true` to find any that a deploy has parked.

- **Records without a kind.** Step records written before this release have no step kind, so the driver does not compare their kind. Code that requests such a record's key as another kind of step, such as `step.delay("charge", ms)` for an old `step.run("charge", ...)` record, does not diverge.
- **Delays recorded by an earlier release.** An earlier release recorded a delay once, when it started, and read the clock again on every replay. This release reads the clock for such a delay too. If a host's clock reads earlier than the delay's end, and the execution has records after the delay, the delivery diverges. Call `clearDivergence` once the clocks agree.
- **Executions without a version.** See [Executions from before this release](#executions-from-before-this-release).
- **Shared SQLite files.** Every process that shares a SQLite file must run this release before any execution is parked. A process on an earlier version does not read the divergence, and delivers a parked execution again. See [`@yieldstar/sqlite-runtime`](../packages/sqlite-runtime.md).
