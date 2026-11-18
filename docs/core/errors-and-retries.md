# Errors & Retries

Workflows handle errors with standard `try` and `catch` blocks. When a step throws, Notation Durable records the error as that step's outcome so later deliveries throw the same error at the same step without running the step function again. To retry transient failures before throwing to the workflow, throw `RetryableError` inside `step.run`.

```ts [src/workflows/place-order.ts]
import { RetryableError, workflow } from "@notation/durable";
import { CardDeclinedError } from "./errors";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { orderId } = event.params;

  try {
    yield* step.run("charge-card", async () => {
      const result = await payments.charge(orderId);

      if (result.status === 503) {
        throw new RetryableError("Payment gateway unavailable", {
          maxAttempts: 5,
          retryInterval: 60_000,
        });
      }

      if (result.status === 402) {
        throw new CardDeclinedError(result.last4);
      }
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

## Catching step errors across replays

When `charge-card` throws `CardDeclinedError`, Durable saves the error as the step's recorded outcome and throws it into the `catch` block. If the workflow later suspends and replays, `charge-card` reads its recorded error and throws it again without calling `payments.charge`.

Code outside steps – including code inside a `catch` block – runs on every delivery that reaches it. Keep all side effects such as `mailer.sendCardDeclined` inside `step.run`.

## Uncaught failures

An error that escapes the workflow ends the execution with status `failed` and stores the error in the execution's `outcome`. This applies both to errors thrown by steps and to errors thrown outside steps.

Callers using `sdk.triggerAndWait` or the HTTP SDK's `waitForResult` reject with the recorded error:

```ts [src/api/checkout.ts]
import { CardDeclinedError } from "../workflows/errors";
import { sdk } from "../durable/sdk";

try {
  await sdk.triggerAndWait({
    workflowId: "place-order",
    params: { orderId: "order-42" },
  });
} catch (err) {
  if (err instanceof CardDeclinedError) {
    console.log(`Declined card ending ${err.last4}`);
  }
}
```

When an execution fails, `sdk.wait` resolves with `{ status: "failed", error }` instead of throwing.

### What the error record keeps

Durable encodes thrown errors using [durable values](./step-values.md) and throws the decoded copy back to workflow code and SDK callers on both the first run and every replay. The record preserves:

- **Standard fields** – `name`, `message`, and `stack`
- **Error code** – `code`, when it is a string
- **Cause and aggregate errors** – `cause` and non-enumerable `errors` arrays as on `AggregateError`
- **Custom fields** – Own enumerable properties whose values are durable

Properties whose values are not durable – such as functions or unserializable class instances – are omitted on both the first run and replay.

## Retrying with `RetryableError`

Throw `RetryableError` from a `step.run` callback when a transient failure should re-run the step before failing:

```ts [src/workflows/sync-contact.ts]
import { RetryableError, workflow } from "@notation/durable";

export const syncContact = workflow<{ customerId: string }>(async function* (step, event) {
  const customer = yield* step.run("load-customer", async () => {
    return await db.customers.find(event.params.customerId);
  });

  yield* step.run("push-to-crm", async () => {
    const response = await crm.upsertContact(customer);
    if (response.status === 503) {
      throw new RetryableError("CRM unavailable", {
        maxAttempts: 4,
        retryInterval: 30_000,
      });
    }
  });
});
```

| Option | Behavior |
| :--- | :--- |
| `maxAttempts` | Maximum number of runs for the step function, including the first attempt |
| `retryInterval` | Fixed milliseconds to wait between failed attempts |

`RetryableError` validates both options at construction and throws a `RangeError` if the attempt count is not a positive integer or the interval is negative or non-finite.

Setting `retryInterval: 0` schedules the next attempt immediately without waiting. Any positive `retryInterval` suspends the execution between attempts and wakes it after that fixed interval; Durable does not add backoff or jitter.

### Retry exhaustion and mixed errors

Each attempt takes its retry policy from the error thrown on that attempt:

- **Attempts exhausted** – Once the final attempt throws `RetryableError`, the step records that error and throws it into workflow code to catch or fail the execution.
- **Non-retryable error** – Throwing any other error on a subsequent attempt ends retries immediately, records that error, and throws it into the workflow.
- **Changing policy** – Throwing a new `RetryableError` with a smaller `maxAttempts` stops retrying once the total attempt count reaches that new limit.

Only `step.run` and `step.poll` retry on `RetryableError`; throwing it outside a step fails the execution like any other uncaught error. [External effects](./external-effects.md) configure their own retry schedule directly on the effect step.

## Registering custom error classes

Built-in JavaScript error classes and Durable's own error classes are registered by default (see [Errors](../reference/errors.md)).

To preserve `instanceof` checks across replays and SDK boundaries for your own error classes, register each class with `registerErrorClass`:

```ts [src/workflows/errors.ts]
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

Registration applies per process. Registering the class at module top level ensures workers, HTTP servers, and SDK callers revive the class whenever they import the module.

Durable revives registered errors by setting their prototype and recorded properties without calling the constructor, so constructors with required parameters work without extra setup. Unregistered custom errors revive as plain `Error` instances while keeping their recorded metadata and durable own fields.

## Thrown non-errors

Step functions can throw values that are not `Error` instances, such as strings or plain objects:

| Thrown value | Replayed value |
| :--- | :--- |
| Durable value | Re-thrown unchanged as the same value |
| Non-durable value | Wrapped in a plain `Error` using `String` coercion |

Both the initial run and every subsequent replay throw the decoded result, so workflow `catch` blocks see the exact same value every time.

For the full catalog of built-in workflow and SDK error classes and codes, see [Errors](../reference/errors.md).
