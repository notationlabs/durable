# Steps

A step is a unit of work inside a workflow. Durable records the outcome of each step, and a step that has a record does not run again. This page covers `step.run`, `step.delay` and `step.poll`, using a workflow that deploys a website.

```ts
export const deploySite = workflow<{ siteId: string }>(async function* (step, event) {
  const { siteId } = event.params;

  const build = yield* step.run("start-build", async () => {
    return await builds.start(siteId);
  });

  yield* step.poll("wait-for-build", { retryInterval: 10_000, maxAttempts: 30 }, async () => {
    const status = await builds.status(build.id);
    return status === "finished";
  });

  yield* step.run("publish", async () => {
    await hosting.publish(siteId, build.id);
  });

  yield* step.delay("settle", 5 * 60_000);

  yield* step.run("purge-cache", async () => {
    await cdn.purge(siteId);
  });
});
```

## `step.run`

`step.run` calls a function and records what it returns. The function can be sync or async:

```ts
const build = yield* step.run("start-build", async () => {
  return await builds.start(siteId);
});
```

If the function throws, `step.run` records the error and throws it into the workflow. See [Errors](./errors.md).

## Deliveries

A worker runs a workflow from the first line every time it picks the workflow up. Each of these runs is a delivery.

In a delivery, a step that already has a record returns its recorded result and does not call its function. `start-build` therefore calls `builds.start` once, however many deliveries `deploySite` takes. A step without a record calls its function and records the result.

## `step.delay`

`step.delay` pauses the workflow for a number of milliseconds:

```ts
yield* step.delay("settle", 5 * 60_000);
```

A delay ends the delivery, and the worker process stops. No process waits for the five minutes to pass. When they have passed, the dispatcher starts a new delivery. That delivery returns the recorded results of the earlier steps, passes `settle`, and runs `purge-cache`.

## `step.poll`

`step.poll` checks a condition at an interval until the condition is true:

```ts
yield* step.poll("wait-for-build", { retryInterval: 10_000, maxAttempts: 30 }, async () => {
  const status = await builds.status(build.id);
  return status === "finished";
});
```

| Option | Meaning |
| --- | --- |
| `retryInterval` | Milliseconds between checks |
| `maxAttempts` | The most checks to make, including the first |

Each check that returns `false` ends the delivery, in the same way as a delay. The next check runs in a new delivery, `retryInterval` milliseconds later.

| The condition | `step.poll` |
| --- | --- |
| Returns `true` | Returns, and the workflow continues |
| Returns `false` on every one of `maxAttempts` checks | Throws `RetryableError` with the message `Polling reached max retries` |
| Throws an error | Throws that error, without checking again |

The workflow can catch either error with `try`/`catch`.

## Execution status

Each execution has a status. The SDK returns it, and it shows how far the execution has got:

| Status | Meaning |
| --- | --- |
| `admitted` | The execution is recorded, and no delivery has finished yet |
| `running` | A worker is running a delivery |
| `suspended` | The last delivery stopped at a step that waits, such as a delay or a poll |
| `completed` | The workflow returned |
| `failed` | The workflow threw an error that it did not catch |
| `cancelled` | The execution was [cancelled](./cancellation.md) |

`completed`, `failed` and `cancelled` are final. Durable does not run workflow code for an execution again once it has one of these statuses.
