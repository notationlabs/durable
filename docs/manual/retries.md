# Retries

A step that calls an unreliable service can ask Durable to run it again later. The step function throws `RetryableError`, and Durable runs the function again on a schedule. This page uses a workflow that copies a customer's details to a CRM, whose API sometimes responds with `503 Service Unavailable`.

```ts
import { RetryableError, workflow } from "@notation/durable";

export const syncContact = workflow<{ customerId: string }>(async function* (step, event) {
  const { customerId } = event.params;

  const customer = yield* step.run("load-customer", async () => {
    return await db.customers.find(customerId);
  });

  yield* step.run("push-to-crm", async () => {
    const response = await crm.upsertContact(customer);

    if (response.status === 503) {
      throw new RetryableError("CRM unavailable", {
        maxAttempts: 5,
        retryInterval: 60_000,
      });
    }
  });
});
```

| Option | Meaning |
| --- | --- |
| `maxAttempts` | The most times to run the step function, including the first. A positive integer |
| `retryInterval` | Milliseconds between attempts. A finite number that is not negative |

The `RetryableError` constructor throws a `RangeError` if either option is out of range.

## How a retry runs

When `push-to-crm` throws `RetryableError`, Durable records the failed attempt and ends the delivery. The execution's status becomes `suspended`, as it does at a delay. No process waits for the interval to pass.

After `retryInterval` milliseconds, the dispatcher starts a new delivery. `load-customer` returns its record, and `push-to-crm` calls its function again. This repeats until the function returns or the attempts run out.

The interval is the same between every pair of attempts. Durable does not add backoff or jitter.

## When the attempts run out

When the last attempt throws `RetryableError`, the step throws that error into the workflow. The workflow can catch it:

```ts
try {
  yield* step.run("push-to-crm", async () => {
    // as above
  });
} catch (err) {
  yield* step.run("flag-for-review", async () => {
    await db.customers.flagSyncFailed(customerId);
  });
}
```

If the workflow does not catch it, the execution fails. See [Errors](./errors.md).

## Errors that are not retried

Any other error ends the step at once. The step records the error and throws it into the workflow, without running the function again:

```ts
if (response.status === 400) {
  throw new Error("CRM rejected the contact");
}
```

**Each attempt takes its policy from the error it throws.** Durable does not remember the options of an earlier `RetryableError`. If the first attempt throws `RetryableError` with `maxAttempts: 5`, and the second throws a plain `Error`, the step throws the plain `Error` after two attempts.

## Where `RetryableError` applies

Only `step.run` and `step.poll` retry on `RetryableError`. `step.poll` uses it to check its condition again, and throws it when the checks run out. See [Steps](./steps.md#steppoll).

Thrown anywhere else, such as in workflow code outside a step, `RetryableError` is an ordinary error. [External effects](./effects.md) have their own options for trying again.
