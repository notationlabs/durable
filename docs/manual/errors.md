# Errors

A workflow handles errors with `try`/`catch`, as any async function does. Durable records an error thrown in a step, so every later delivery throws the same error at the same step. This page uses a workflow that charges a customer's card for an order.

```ts
import { workflow } from "@notation/durable";
import { CardDeclinedError } from "./errors";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { orderId } = event.params;

  try {
    yield* step.run("charge-card", async () => {
      await payments.charge(orderId);
    });
  } catch (err) {
    if (err instanceof CardDeclinedError) {
      yield* step.run("email-declined", async () => {
        await mailer.sendCardDeclined(orderId);
      });
      return { status: "declined" };
    }
    throw err;
  }

  return { status: "paid" };
});
```

## Errors in steps

When a step function throws, the step records the error as its outcome and throws it into the workflow. In the example, `charge-card` throws `CardDeclinedError`, and the `catch` block sends an email.

A later delivery does not call `payments.charge` again. `charge-card` reads its record and throws the recorded error, so the workflow takes the same path through the `catch` block. `email-declined` then returns its own record.

Code outside steps runs in every delivery that reaches it. This includes code in a `catch` block, so keep work with side effects inside steps.

## Uncaught errors

An error that the workflow does not catch fails the execution. This is true of an error thrown by a step, and of an error thrown by code outside a step. The execution's status becomes `failed`, and its record keeps the error.

The SDK returns the recorded error. `sdk.triggerAndWait` rejects with it:

```ts
try {
  await sdk.triggerAndWait({ workflowId: "place-order", params: { orderId: "o-42" } });
} catch (err) {
  // err is the error that failed the execution
}
```

## What the record keeps

The record keeps these parts of an error:

- `name`, `message` and `stack`;
- `code`, if it is a string;
- `cause`; and
- the error's other own fields that are [durable values](./step-values.md).

Other fields are left out. As with step results, the step throws the decoded copy, in the first delivery as well as later ones. A field that the record leaves out is therefore missing from the error the first time too.

## Error classes

A decoded error is an instance of the class registered under its `name`. Durable registers its own error classes and the built-in ones, such as `TypeError`. A class that you define needs `registerErrorClass`, or `instanceof` does not match it:

```ts [errors.ts]
import { registerErrorClass } from "@notation/durable";

export class CardDeclinedError extends Error {
  code = "CARD_DECLINED";

  constructor(public last4: string) {
    super(`Card ending ${last4} was declined`);
    this.name = "CardDeclinedError";
  }
}

registerErrorClass("CardDeclinedError", CardDeclinedError);
```

Without the call to `registerErrorClass`, the error is a plain `Error`. It still has the name `CardDeclinedError`, the code `CARD_DECLINED` and the field `last4`, but `err instanceof CardDeclinedError` is `false`.

**Registration applies to one process only.** The worker process decodes the errors that workflows catch, so the worker must run the call to `registerErrorClass`. Calling it in the module that defines the class does this, because the workflow imports that module.

Durable sets the prototype and fields of a decoded error without calling its constructor. A constructor with required arguments, such as the one above, is therefore fine.

## Values that are not errors

A step function can throw a value that is not an `Error`:

| Thrown value | The step throws |
| --- | --- |
| A durable value, such as a string or a plain object | The same value |
| A value that is not durable, such as a class instance | `new Error(String(value))` |

## Other errors

- A step that throws `RetryableError` runs again before it throws. See [Retries](./retries.md).
- Durable's own errors, such as `WorkflowValueError`, are listed in [Error reference](./error-reference.md).
