# Effect client

The effect client keeps the records of [`step.effect`](../manual/effects.md). An effect record says whether an external operation, such as a card charge, has a known outcome. The step reads it before it calls the external system, so that it does not repeat an operation that already happened.

Effect records are keyed by effect ID, across every execution. They are never deleted.

## Effect state

`@notation/durable/runtime` exports `EffectState`, the fields that a runtime stores for one effect:

| Field | Holds |
| --- | --- |
| `effectId` | The effect's ID |
| `operation`, `input` | What the effect does. `input` is encoded |
| `status` | `pending`, `settled` or `failed` |
| `result` | The encoded result, or the encoded error when `status` is `failed` |
| `conflicts` | Reports of settlements that disagreed with the stored outcome |

The client also keeps effect waiters. A waiter names an effect, and the execution and step that wait for its outcome.

## Methods

Each method runs in one transaction, and uses a helper from `@notation/durable/runtime`:

| Method | Does | Helpers |
| --- | --- | --- |
| `begin` | Creates the effect as `pending`, or answers from the existing effect | `prepareEffectBegin`, `createEffectState`, `matchEffectBegin` |
| `settle` | Stores the outcome, and makes every waiting execution due | `prepareEffectSettle`, `settleEffect` |
| `watch` | Registers a waiter | `prepareEffectWatch` |
| `getEffect` | Returns the record, for operators | `toEffectRecord` |

## Example: a charge that times out

A workflow charges a card with the effect ID `acme:charge:order-42`. The first call to the payment provider times out. The records change like this:

| Event | Effect | Waiters |
| --- | --- | --- |
| The step calls `begin`, which returns `new` | `pending` | |
| `perform` times out. The step calls `watch`, and suspends for `retryIn` | `pending` | `acme:charge:order-42` ← the execution, step `charge` |
| The next delivery calls `begin`, which returns `pending`. The step calls `lookup`, which finds the charge. The step calls `settle` | `settled`, with the charge | Unchanged |
| The step records its result, `done` | Unchanged | The waiter is deleted in the same write |

## Beginning

`begin` reads the effect and, if there is none, inserts a `pending` one. **The read and the insert must be one atomic step**, so that two steps that begin one effect at once both see one record.

For an existing effect, `matchEffectBegin` compares the request's `operation` and encoded `input` with the stored ones. If they differ, it returns `EFFECT_REQUEST_MISMATCH`, and the effect does not change. Otherwise it returns `pending`, or the stored outcome.

## Settling

`settleEffect(state, settle, now)` returns the new state, the result, and a `wake` flag. `wake` is `true` only when this settlement ended a `pending` effect. **The settlement and the wakes of every waiter must be one write**, so that a crash cannot store the outcome and lose a wake. `SqliteRuntime` does both in one transaction:

```ts
return runTransaction(this.db, () => {
  const state = this.effectsDao.getState(settle.effectId);
  if (!state) throw effectNotFound(settle.effectId);
  const settled = settleEffect(state, settle, this.clock());
  if (settled.state !== state) this.effectsDao.updateState(state, settled.state);
  if (settled.wake) {
    for (const executionId of this.effectWaitersDao.getWaitingExecutionIds(settle.effectId)) {
      this.executions.wakeInTransaction(executionId);
    }
  }
  return settled.result;
});
```

The first outcome always stays. A later settlement with an equal outcome, or a failure after a failure, returns the stored outcome. Any other settlement returns `conflict` with both outcomes, and adds a report to `conflicts`, once for each conflicting value.

## Watching

`watch` registers the step as a waiter on the effect. A repeat keeps one registration. If the effect already has an outcome, `watch` also makes the execution due at once, because no settlement is left to wake it.

A waiter stays until the heap writes the step's `done` record, which deletes it in the same write. A forced cancel of the execution deletes it too.

## Errors

`begin`, `settle` and `watch` are not fenced. Each is keyed by effect ID, so a paused delivery that calls one late does no harm.

The client throws `EffectClientInvariantError` for a request that the worker should never make, such as one for an effect that does not exist, and for stored data that it cannot read. The `prepare` helpers and `effectNotFound` throw it for you. The worker ends the delivery with `EFFECT_RUNTIME_INVARIANT`.

Any other error from the client means that it may be unavailable. The worker ends the delivery with `EFFECT_CLIENT_UNAVAILABLE`. See [Failed deliveries](../manual/error-reference.md#failed-deliveries).
