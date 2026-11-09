# External effects

`step.effect` calls an external system, such as a payment provider, so that the operation happens once, even if a delivery stops partway through. This page uses a workflow that charges a customer for an order.

## Problem: a step can run twice

A step that charges a card with `step.run` can charge it twice:

```ts
yield* step.run("charge", async () => {
  return await payments.charge({ orderId, amount: 4999 });
});
```

The worker can crash after the provider takes the payment but before Durable records the step. The step then has no record, so the next delivery calls `payments.charge` again.

`step.effect` closes this gap. Before it charges again, it asks the provider whether the first charge happened.

## `step.effect`

```ts
const charge = yield* step.effect("charge", {
  effectId: `acme:charge:order-${orderId}`,
  operation: "charge-v1",
  input: { amount: 4999 },
  retryIn: 60_000,

  lookup: async (effectId) => {
    const found = await payments.findByIdempotencyKey(effectId);
    return found ? { status: "settled", result: found } : { status: "not-found" };
  },

  perform: async (effectId) => {
    return await payments.charge({ orderId, amount: 4999, idempotencyKey: effectId });
  },
});
```

| Field | Meaning |
| --- | --- |
| `effectId` | The name of the operation in the external system. A non-empty string |
| `operation` | The name of this version of `input`, `lookup` and `perform`. A non-empty string |
| `input` | A plain object of durable values that describes the request |
| `retryIn` | Milliseconds to wait before checking an uncertain outcome again |
| `lookup` | Asks the external system what it knows about `effectId` |
| `perform` | Carries out the operation under `effectId`, and returns its result |
| `failsOn` | Optional. Says which errors from `perform` are [definite failures](#definite-failures) |
| `maxAttempts` | Optional. How many attempts may end uncertain before the step [gives up](#giving-up) |

## Effect IDs

The step key names the step in this execution. The effect ID names the operation in the external system.

Every caller that means one operation must use the same effect ID, and no caller may use it for a different operation. A support tool that checks the same charge has no `charge` step, but it uses the same effect ID. Build the ID from what identifies the operation: here, the provider account, the kind of operation and the order.

## How the step decides what to call

Durable keeps an effect record for each effect ID. Before the step calls `perform`, Durable writes the record as pending. A later attempt that finds the record still pending cannot tell whether the earlier `perform` happened. It therefore calls `lookup` first.

`lookup` returns one of four answers:

```ts
type EffectLookup<T> =
  | { status: "settled"; result: T }
  | { status: "failed"; error: Error }
  | { status: "not-found" }
  | { status: "unknown" };
```

| Situation | The step |
| --- | --- |
| The step has a record | Returns or throws the recorded outcome, and calls nothing |
| The effect has no record | Calls `perform` |
| The effect is pending | Calls `lookup` |
| `lookup` returns `not-found` | Calls `perform` |
| `perform` returns, or `lookup` returns `settled` | Records the result and returns it |
| `lookup` returns `failed` | Records the error and throws it |
| `lookup` returns `unknown`, or `lookup` or `perform` throws | Waits `retryIn` milliseconds, then starts again |

An error thrown by `lookup` or `perform` counts as `unknown`, because the step cannot tell whether the operation happened. The step suspends the execution, as `step.delay` does, and the next delivery starts again with `lookup`.

Once an effect has a result or an error, the effect record keeps it and never changes. Any other step that uses the same effect ID, in any execution, receives that outcome without calling `lookup` or `perform`.

## Definite failures

Some errors prove that the operation did not happen and never will, such as a declined card. Without `failsOn`, the step treats a declined card like a timeout, and keeps checking every `retryIn` milliseconds. `failsOn` names the errors that are final:

```ts
const charge = yield* step.effect("charge", {
  // the fields above
  failsOn: (err) => err instanceof CardDeclinedError,
});
```

When `perform` throws and `failsOn` returns `true`, the effect fails. The step records the error and throws it into the workflow. To try the charge again, use a new effect ID.

`failsOn` must return `false` for a timeout, a lost connection or a server error, because the charge may still have happened. Durable does not call `failsOn` for errors thrown by `lookup`.

## Giving up

Without `maxAttempts`, a step whose outcome stays unknown keeps checking forever. With `maxAttempts`, the step throws `WorkflowEffectError` with the code `EFFECT_OUTCOME_UNKNOWN` when that many attempts have ended uncertain:

```ts
import { WorkflowEffectError } from "@notation/durable";

try {
  yield* step.effect("charge", {
    // the fields above
    maxAttempts: 5,
  });
} catch (err) {
  if (err instanceof WorkflowEffectError && err.code === "EFFECT_OUTCOME_UNKNOWN") {
    // the charge may or may not have happened
  }
}
```

With `retryIn: 60_000` and `maxAttempts: 5`, the step gives up about four minutes after the first attempt.

## External system requirements

`step.effect` makes an operation safe only if the external system keeps these promises for each effect ID:

1. Repeated or concurrent `perform` calls make one operation and return one result.
2. `not-found` means that no call was accepted, none is running, and no earlier call can still finish.
3. `settled` returns the same result as every `perform` that succeeded, and never changes.
4. `failed`, and an error for which `failsOn` returns `true`, mean that the operation finished without success and no call can still succeed.
5. `unknown` means that the system does not know yet.

**The second promise matters most.** If `lookup` reports `not-found` for a charge that is still in flight, the step calls `perform` again, and the card is charged twice.

Many payment and cloud APIs keep the first promise with an idempotency key. Pass the effect ID as that key.

## Values

`input`, the result of `perform` and the result in a `settled` answer must be [durable values](./step-values.md). A `perform` that returns nothing, such as a delete, gives the result `undefined`.

## Changing an effect

`operation` and `input` describe the request. If a deploy changes either of them after the effect has a record, such as a new amount, the step throws `WorkflowEffectError` with the code `EFFECT_REQUEST_MISMATCH`. Neither `lookup` nor `perform` runs.

When you change `lookup` or `perform`, keep the same effect ID and the same promises. Otherwise the step may be unable to find out whether the operation happened.

## Effect records

Effect records are kept apart from executions. Cancelling an execution that has a pending effect does not stop the external operation, and the effect can still settle.

[Error reference](./error-reference.md) lists the other effect errors, including the errors that end a delivery without reaching the workflow.
