# Effect Client

`EffectClient` stores global effect records and effect waiters for [`step.effect`](../core/external-effects.md). Each record tracks whether an external operation has started, settled with a result, or failed definitely, preventing duplicate side effects across retries, crashes, and executions.

## Effect records and eternal persistence

Effect records are keyed globally by `effectId` across every execution and runtime, and they are never deleted – even after the execution that began an effect completes, is force-cancelled, or is purged.

`@notation/durable/runtime` exports `EffectState` as the canonical storage representation:

| Field | Contents |
| :--- | :--- |
| `effectId` | Global identifier of the external operation |
| `operation` | Operation name versioning `input`, `lookup`, and `perform` |
| `input` | Canonical durable-value encoding of the request input object |
| `status` | One of `"pending"`, `"settled"`, or `"failed"` |
| `result` | Canonical encoding of the settled result or failed `Error` when not pending |
| `conflicts` | List of `EffectConflictState` entries recorded when a later settlement disagreed |

Storing canonical encodings from `durableValueCodec` ensures equal objects with different key insertion orders compare as equal strings without decoding inside transactions.

## EffectClient methods and state helpers

Every method on `EffectClient` maps to pure validation and state-transition helpers exported from `@notation/durable/runtime`:

| Method | Helpers |
| :--- | :--- |
| `begin` | `prepareEffectBegin`, `createEffectState`, `matchEffectBegin` |
| `settle` | `prepareEffectSettle`, `settleEffect`, `effectNotFound` |
| `watch` | `prepareEffectWatch`, `effectNotFound` |
| `getEffect` | `toEffectRecord` |

The adapter runs validation via the `prepare*` helpers, then performs the storage read and conditional write inside one atomic transaction:

```ts [src/effect-client.ts]
import {
  createEffectState,
  effectNotFound,
  matchEffectBegin,
  prepareEffectBegin,
  prepareEffectSettle,
  prepareEffectWatch,
  settleEffect,
  toEffectRecord,
  type EffectBeginRequest,
  type EffectBeginResult,
  type EffectClient,
  type EffectRecord,
  type EffectSettleRequest,
  type EffectSettleResult,
  type EffectWatchRequest,
} from "@notation/durable/runtime";

export class CustomEffectClient implements EffectClient {
  async begin(request: EffectBeginRequest): Promise<EffectBeginResult> {
    const begin = prepareEffectBegin(request);
    return this.db.transaction(() => {
      const state = this.effects.getState(begin.effectId);
      if (state) return matchEffectBegin(state, begin);
      this.effects.insertState(createEffectState(begin));
      return { status: "new" as const };
    });
  }

  async settle(request: EffectSettleRequest): Promise<EffectSettleResult> {
    const settle = prepareEffectSettle(request);
    return this.db.transaction(() => {
      const state = this.effects.getState(settle.effectId);
      if (!state) throw effectNotFound(settle.effectId);
      const settled = settleEffect(state, settle, this.clock());
      if (settled.state !== state) this.effects.updateState(state, settled.state);
      if (settled.wake) {
        for (const executionId of this.effectWaiters.getWaitingExecutionIds(settle.effectId)) {
          this.executions.wakeInTransaction(executionId);
        }
      }
      return settled.result;
    });
  }

  async watch(request: EffectWatchRequest): Promise<void> {
    const watch = prepareEffectWatch(request);
    this.db.transaction(() => {
      const state = this.effects.getState(watch.effectId);
      if (!state) throw effectNotFound(watch.effectId);
      this.effectWaiters.upsertWaiter(watch);
      if (state.status !== "pending") {
        this.executions.wakeInTransaction(watch.executionId);
      }
    });
  }

  async getEffect(effectId: string): Promise<EffectRecord | undefined> {
    const state = this.effects.getState(effectId);
    return state ? toEffectRecord(state) : undefined;
  }
}
```

## Beginning and request matching

`begin` runs `prepareEffectBegin` to verify non-empty identity strings and durable plain-object inputs before entering the transaction, canonicalizing the encoded payload up front.

Inside one atomic transaction, look up the effect by ID:

- **Missing record** – Insert `createEffectState(begin)` in pending status and return `"new"`. Atomic read-or-insert guarantees concurrent `begin` calls produce `"new"` for at most one caller.
- **Matching record** – `matchEffectBegin` verifies identical operation and canonical input (ignoring key order while preserving explicit undefined keys), returning `"pending"` or the decoded `EffectOutcome`.
- **Mismatched record** – If the operation or canonical input changed, return `{ status: "rejected", code: "EFFECT_REQUEST_MISMATCH" }` and leave the record untouched.

## Settling, outcomes, and conflicts

An `EffectOutcome` represents a terminal external outcome in one of two variants:

| Variant | Representation |
| :--- | :--- |
| `settled` | `{ status: "settled", result }` carrying any durable value including `undefined` |
| `failed` | `{ status: "failed", error }` carrying an `Error` instance for a definite failure |

`settle` validates inputs with `prepareEffectSettle` and loads the effect inside an atomic transaction, throwing `effectNotFound` if missing. `settleEffect` then drives the transition:

- **First settlement** – Transitions pending records to settled or failed, stores canonical results, and signals `wake: true`. Wake every waiter for that effect in the same transaction so crashes never persist outcomes without wakes.
- **Idempotent repeat** – Repeating an identical settlement result or failing an already-failed effect again returns the recorded `EffectOutcome` with `wake: false` and creates no conflict.
- **Conflicting settlement** – Keeps the first recorded outcome, returns `{ status: "conflict", outcome, conflictingOutcome }`, and appends an `EffectConflictReport` deduplicated by conflicting result.

## Watching and waiter cleanup

`watch` validates inputs with `prepareEffectWatch` and throws `effectNotFound` if the effect has never begun. Inside the same transaction:

1. Upsert the waiter keyed by effect ID, execution ID, and step key so duplicate watches keep one row.
2. When the effect is already settled or failed, call `wakeInTransaction` immediately on every `watch` call because no future settlement will wake it.

Waiters stay alive across wakes and unfinished step attempts. They are removed atomically by finished step writes (or `deleteWaitersOf`), recorded cancellations, and forced execution cancels without touching the underlying effect record.

## Unfenced writes and error handling

`begin`, `settle`, and `watch` are intentionally unfenced. Every operation is keyed by effect ID, so a late call from a paused lease holder is idempotent and cannot corrupt a recorded outcome.

Errors rejected by `EffectClient` divide into invariant violations and availability failures:

| Rejection kind | Delivery behavior |
| :--- | :--- |
| Invariant error | Worker ends the delivery with `EFFECT_RUNTIME_INVARIANT` without recording a step |
| Other rejection | Worker ends the delivery with `EFFECT_CLIENT_UNAVAILABLE` and redelivers later |

Validation, state transition, and lookup helpers throw `EffectClientInvariantError` with `EFFECT_RUNTIME_INVARIANT` for malformed requests, unknown effects, mismatched settle operations, or corrupt stored data. Neither error category reaches workflow code (see [Runtime Reference](../reference/runtime.md)).
