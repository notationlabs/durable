# Steps

Each step wraps a discrete unit of work. 

When a step is yielded, the runtime persists the result, schedules any retries, or yields control back to the workflow to continue execution. 

On subsequent workflow executions, the runtime replays the previously cached value so the step function does not re-execute. Each step finds its record by its step key, the string passed as the first argument. See [Step keys](#step-keys).

Note: Steps must be yielded with `yield*`; plain `yield` produces a runtime error.

## `step.run`

`step.run` executes a function, caches its return value, and replays that cached result on subsequent workflow invocation without re-executing the function.

```ts
const result = yield* step.run("fetch-user", () => {
  return fetchUser(userId);
});
```

`step.run` accepts sync or async functions:

```ts
const data = yield* step.run("fetch-data", async () => {
  const res = await fetch("https://api.example.com/data");
  return res.json();
});
```

## Step values

The runtime records each step's result in the step's record, and replay reads the result back from that record. A step's result must therefore be a durable value, so that replay returns exactly what the first run saw:

| Durable                                                       | Not durable                                       |
| ------------------------------------------------------------- | ------------------------------------------------- |
| `null`, booleans, finite numbers, strings, `bigint`           | Functions and symbols                             |
| `undefined`, as a whole value, a property or an array item    | Class instances, such as `new Reservation()`      |
| Valid `Date`s, and `Map`s and `Set`s of durable values        | `NaN`, `Infinity`, invalid `Date`s, sparse arrays, cycles |
| Errors, including their class, `code`, `cause` and fields     | Objects with symbol keys, getters or setters      |
| Arrays and plain objects made of durable values               | Subclasses of `Array`, `Date`, `Map` and `Set`    |

A `Date`, `Map`, `Set` or `bigint` reads back as the same type, so a step can return one directly:

```ts
const reservation = yield* step.run("reserve-stock", async () => ({
id: "r-42",
expiresAt: new Date(Date.now() + 15 * 60_000),
}));
reservation.expiresAt; // a Date, on the first run and on replay
```

A step that returns a value that is not durable, such as a class instance, is rejected on its first run:

```ts
const hold = yield* step.run("reserve-stock", async () => new Reservation("r-42"));
// Throws WorkflowValueError with code "INVALID_STEP_VALUE" and stepKey "reserve-stock"
```

The rejection is recorded as the step's error, so replay throws the same `WorkflowValueError`, and workflow code can catch it like any other step error. This is important because the mistake surfaces in the first test run, rather than on a replay hours later. TypeScript catches most of these values earlier with the `Durable<T>` type, which rejects a function anywhere in a value. It cannot tell a class instance from a plain object, so only the runtime check rejects class instances. Every API that records a value checks it: `step.run` results, workflow params and results, `step.start` and `step.call` params, `step.effect` inputs and results, store state (through the store's schema output) and the values `select`, `when` and `take` return. Interfaces, optional properties and unions with `undefined` are accepted. A value typed `any` is not checked, and one typed `unknown` is rejected until it is narrowed, since it may hold anything; the runtime still checks both.

Workflow code never receives the object the step function returned. On the first run, `step.run` encodes the result, decodes it again, and returns the decoded copy, which is the same value replay reads from the record:

```ts
const order = { id: 42, note: undefined };
const recorded = yield* step.run("load", async () => order);

recorded === order; // false: a decoded copy
recorded; // { id: 42, note: undefined }: the undefined property keeps its key
```

Plain objects stay plain objects. An object with `name`, `message` and `stack` properties is not an error, and an object with a `$type` key reads back unchanged.

The same rule applies to the values that store steps record, to store state (see [Durable Stores](./stores.md#durable-state)) and to the workflow's own result. A workflow that returns a class instance fails with `WorkflowValueError` (`INVALID_STEP_VALUE`) and the step key `$$workflow-result$$`.

### Schemas

A step that needs a class instance returns a durable form of it and passes a schema that rebuilds the instance. The schema is any [Standard Schema](https://standardschema.dev), such as one from Zod or Valibot:

```ts
const price = yield* step.run(
"quote",
async () => ({ amount: "12.50", currency: "GBP" }),
{ schema: MoneyInput.transform((value) => new Money(value)) }
);
// price is a Money, on the first run and on replay
```

`step.run` validates the decoded value with the schema on the first run and on every replay, and returns the schema's output. The step function must still return a durable value, because the record stores the schema's input, not its output.

The step record notes whether the schema accepted the value on the first run. If the schema rejects it, the raw value is still recorded, and both the first run and replay throw `WorkflowValueError` with the code `STEP_SCHEMA_INVALID`. A newer schema that rejects, on replay, a value the first run accepted does not throw: the delivery diverges with `STEP_SCHEMA_CHANGED` (see [Schema changes](./replay-stability.md#schema-changes)).

### Errors in step records

An error thrown by a step function is recorded too, and replay throws it again. The record keeps the error's `name`, `message`, `stack`, a string `code`, its `cause`, and its own enumerable properties that are durable values. Other properties are left out, on the first run as well as on replay.

Replay revives an error as the class registered under its `name`. A custom class must be registered for `instanceof` to hold:

```ts
import { registerErrorClass } from "yieldstar";

class CourierError extends Error {
code = "COURIER_DOWN";
constructor(public courier: string) {
  super(`${courier} is down`);
  this.name = "CourierError";
}
}
registerErrorClass("CourierError", CourierError);

try {
yield* step.run("ship", () => courier.book(order));
} catch (err) {
if (err instanceof CourierError) {
  yield* step.run("ship-backup", () => backupCourier.book(order));
}
}
```

Revival sets the prototype and fields without calling the constructor, so a constructor with required arguments is fine. Registration is per process: a worker process must register the class too, or it revives the error as a plain `Error` with the same `name`, `message`, `code`, `cause` and fields.

These classes are registered by default: `RetryableError`, `WorkflowStartError`, `WorkflowJoinError`, `WorkflowCancelledError`, `WorkflowRaceError`, `WorkflowReplayError`, `WorkflowValueError`, `WorkflowEffectError`, `EffectDeliveryError`, `EffectClientInvariantError`, `StoreValidationError`, `TypeError`, `RangeError`, `SyntaxError`, `ReferenceError`, `EvalError`, `URIError` and `AggregateError`.

A thrown value that is not an error, such as a string or a plain object, is rethrown as it was thrown. A thrown value that is not durable, such as a class instance, is recorded and rethrown as `Error(String(value))`.

## `step.delay`

`step.delay` pauses the workflow for a given number of milliseconds. When a workflow reaches a delay, the runtime records the unfinished step and the delivery ends as `suspended` with `wakeAt` set to the time the delay elapses. The execution record becomes due at that time.

Once it is due, the dispatcher delivers the execution again. All steps before the delay replay from cache, and execution continues from the next step.

```ts
yield* step.delay("cooling-off", 30_000); // pause for 30 seconds
```

This makes long pauses (waiting for a cooling-off period, spacing out API calls, scheduling future work) durable across process restarts without blocking a thread.

## `step.poll`

`step.poll` repeatedly evaluates a predicate at a fixed interval until it returns `true` or exhausts its retry budget. It composes `step.run` with `RetryableError` internally, so each attempt is cached and retried on the same schedule as any other step.

```ts
yield* step.poll("deployment-ready", { retryInterval: 1000, maxAttempts: 10 }, async () => {
  const status = await checkDeployment(deployId);
  return status === "ready";
});
```

If the predicate returns `false` on every attempt, the step throws after `maxAttempts`. If the predicate throws a non-retryable error, execution stops immediately.

## `step.all` and `step.race`

`step.all` and `step.race` run several branches of steps concurrently within one delivery and record their outcome under their own key. `step.all` returns every branch's result in order; `step.race` returns the index and value of the first branch to succeed. See [Concurrent Steps](./concurrent-steps.md).

```ts
const [stock, quote] = yield* step.all("check-order", [
  step.run("check-stock", () => warehouse.inStock(sku)),
  step.run("quote-shipping", () => courier.quote(address)),
]);
```

## `step.patched`

`step.patched(id)` returns whether this execution reached this point after the patch `id` was deployed. An execution whose history already continues past this point gets `false`, and one that reaches it at the end of its history records a marker and gets `true`. It lets you insert a step before steps that in-flight executions have already recorded:

```ts
yield* step.run("charge", () => payments.charge(orderId));
if (yield* step.patched("send-receipt")) {
  yield* step.run("receipt", () => mail.receipt(orderId));
}
yield* step.delay("courier", 2 * DAY);
```

It cannot be called inside a `step.all` or `step.race` branch. See [Patches](./replay-stability.md#patches).

## `step.start`

`step.start` starts another workflow as its own execution and returns `{ executionId }`. It takes a step key and the target workflow:

```ts
const target = yield* step.start("ship", {
  workflowId: "ship-order",
  params: { orderId: event.params.orderId },
});
```

The start is identified by the calling execution's ID and the step key. If a delivery stops after the target is started but before the step is recorded, the next delivery sends the same request and receives the same target, so the workflow is never started twice. The target's execution record links back to the caller and its step as `parent`, and inherits the caller's `context`.

`params` must be a durable value (see [Step values](#step-values)), and TypeScript checks this with `Durable<T>`. Keys may be in any order, and `{ a: undefined }` is a different request from `{}`.

A rejected start throws a `WorkflowStartError`, which workflow code can catch. Its `code` is:

- `INVALID_START_PARAMETERS` when `params` are not a durable value or `workflowId` is empty; or
- `START_REQUEST_MISMATCH` when the step was already started with a different `workflowId`, `params` or `detached` option, for example after a deploy changed the step.

The rejection is recorded, so replay throws the same error. If the runtime fails while starting the target, the error is an infrastructure error: it does not reach workflow code, and the next delivery tries the same start again.

When the caller is [cancelled](./cancellation.md#children), the runtime cancels the target too. Pass `{ detached: true }` as a third argument to start a target that should outlive the caller, such as a receipt email:

```ts
yield* step.start(
  "receipt",
  { workflowId: "send-receipt", params: { orderId } },
  { detached: true }
);
```

The target's `parent` link records `detached: true`, and the target receives no cancellation propagated from the caller. `detached` defaults to `false`, and a value that is not a boolean throws a `TypeError`.

## `step.join`

`step.join` waits until another execution is terminal, and returns its result. It takes a step key and the target's execution ID:

```ts
const { executionId } = yield* step.start("ship", {
  workflowId: "ship-order",
  params: { orderId },
});

const shipment = yield* step.join<{ tracking: string }>("ship:join", executionId);
```

The type parameter is the target's result type. It is not checked against the target.

The step reads the target's execution record:

- If the target has completed, the step records its result and returns it.
- If the target has failed, the step records and throws a `WorkflowJoinError` with code `JOINED_EXECUTION_FAILED`. Its `executionId` is the target's ID, and its `cause` is the target's recorded error, with at least its `name` and `message`.
- If the target was [cancelled](./cancellation.md), the step records and throws a `WorkflowJoinError` with code `JOINED_EXECUTION_CANCELLED`. Its `reason` and `source` are the target's cancelled outcome's.
- If the target is not terminal, the step registers a join waiter on the target and the delivery ends as `suspended`. When the target becomes terminal, the runtime wakes every joiner of the target, and the step reads the target again.

The step records the outcome under its key, so replay returns the result or throws the error without reading the target again. Workflow code can catch `WorkflowJoinError`, and its `code` and `executionId` are the same on replay.

```ts
import { WorkflowJoinError } from "yieldstar";

try {
  const shipment = yield* step.join("ship:join", executionId);
} catch (err) {
  if (err instanceof WorkflowJoinError && err.code === "JOINED_EXECUTION_FAILED") {
    yield* step.run("refund", () => payments.refund(orderId));
  }
}
```

The other codes are:

- `JOIN_TARGET_NOT_FOUND` when no execution has the target ID;
- `JOIN_TARGET_PRUNED` when the target was [pruned](./retention.md) before the step read it, so its outcome is gone; and
- `JOIN_SELF` when the target is the calling execution.

All three are recorded, so replay throws them without reading the target.

A workflow can join any execution, not only one it started, and several executions can join one target. The runtime does not detect cycles: if A joins B and B joins A, both wait forever unless they use a deadline or one of them is [cancelled](./cancellation.md).

The runtime records a terminal outcome and the wakes for its join waiters in one atomic write, so a crash between the target's last delivery and the joiner's next delivery does not lose the wake. A join waiter stays registered until the joiner records the step, and the write that records the step deletes it. While a joiner that is not terminal has a waiter on the target, a retention policy does not prune the target, so a woken joiner always reads the outcome. See [Joins](./retention.md#joins).

### Joining with a deadline

With a `deadline`, the step returns a `TimedJoinResult<R>` instead of the result:

```ts
const shipped = yield* step.join<{ tracking: string }>("ship:join", executionId, {
  deadline: Date.now() + 3 * 24 * 60 * 60 * 1000,
});

if (shipped.status === "timed-out") {
  yield* step.run("apologise", () => email.sendDelayNotice(orderId));
} else {
  // shipped.result is the target's result
}
```

```ts
type TimedJoinResult<R> =
  | { status: "completed"; result: R }
  | { status: "timed-out" };
```

The deadline follows the rules of a [timed `when`](./store-waiting.md#waiting-with-a-deadline):

- The first evaluation records the deadline in the unfinished step, before it registers the join waiter. Later evaluations use the recorded deadline and ignore the argument, so a deadline computed from `Date.now()` does not move on replay.
- Each evaluation reads the target before it compares the deadline with the clock. A terminal target wins, even when the deadline has passed.
- An unresolved evaluation ends the delivery as `suspended` with `wakeAt` set to the deadline.
- The first recorded result is the only one returned. A target that completes after `timed-out` is recorded does not change the step.

A failed or cancelled target still throws `WorkflowJoinError`. Only `completed` and `timed-out` are returned as values.

## `step.call`

`step.call` starts another workflow and waits for it, as `step.start` followed by `step.join`, under one step key:

```ts
const shipment = yield* step.call<typeof shipOrder>("ship", {
  workflowId: "ship-order",
  params: { orderId },
});
```

The type parameter is the child workflow. It checks `params` and types the result. It does not check that `workflowId` names that workflow, because a workflow cannot refer to the type of a router that contains it.

The step records one result, under its key, when the child is terminal. Until then, each evaluation sends the same start request, identified by the calling execution's ID and the step key as for `step.start`, and joins the child that the request names. So a delivery that stops between the start and the join does not start a second child: the next delivery receives the same child and joins it. Once the step is recorded, replay returns the result, or throws the error, without starting or joining.

The child's execution record has `parent` set to the caller's execution ID and the step key. `step.call` takes `detached` as `step.start` does, in the same options object as `deadline`:

```ts
const receipt = yield* step.call<typeof sendReceipt>(
  "receipt",
  { workflowId: "send-receipt", params: { orderId } },
  { detached: true }
);
```

`detached` alone does not make the call timed: the step returns the result, not a `TimedJoinResult`.

`step.call` throws the same errors as `step.join`. It also throws a `WorkflowStartError` if the runtime rejects the start, for example with `START_REQUEST_MISMATCH` when a deploy changed the step's `workflowId` or `params` before the child finished.

With a `deadline`, `step.call` returns a `TimedJoinResult<R>` and follows the rules in [Joining with a deadline](#joining-with-a-deadline). The deadline does not stop the child: a `timed-out` call leaves the child running.

## `step.effect`

`step.effect` calls an external system, such as a payment provider or a cloud API, so that the operation happens once even when a delivery stops partway through. A `step.run` that charges a card can charge it twice: if the worker crashes after the provider takes the money but before the step is recorded, the next delivery runs the step function again. `step.effect` closes that gap by asking the provider what happened before it tries again.

```ts
const payment = yield* step.effect("charge", {
  effectId: `acct-1:charge:order-${orderId}`,
  operation: "charge-v1",
  input: { amount: 4999 },
  lookup: (effectId) => payments.lookup(effectId),
  perform: (effectId) => payments.charge(effectId, { amount: 4999 }),
  retryIn: 60_000,
});
```

The step key names where the call happens in this execution. `effectId` names the operation in the external system. These are different things: a support tool that checks on the same payment has no `charge` step, but it uses the same effect ID. Every caller that means one operation must use the same `effectId`, and no caller may reuse it for another, so build it from everything that identifies the operation, such as the provider account, the tenant, the kind of operation and the business identity.

| Field       | Description                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------ |
| `effectId`  | Identifies the operation in the external system. A non-empty string                             |
| `operation` | Names the version of the behaviour of `input`, `lookup` and `perform`. A non-empty string        |
| `input`     | A plain object of durable values that describes the request                                      |
| `lookup`    | Asks the external system what it knows about `effectId`                                          |
| `perform`   | Performs the operation under `effectId` and resolves with its result                             |
| `retryIn`   | Milliseconds to wait before looking up an uncertain outcome again. Finite and not negative       |
| `failsOn`     | Optional. Returns `true` when an error thrown by `perform` is a definite failure                 |
| `maxAttempts` | Optional. How many attempts may end uncertain before the step gives up. A positive integer     |

Before it calls `perform`, the runtime writes a pending effect record for the effect ID, so the record always exists before the operation can start. When a later attempt finds the record still pending, an earlier call may have started or finished, so it calls `lookup` instead of `perform`. `lookup` resolves with one of four answers:

```ts
type EffectLookup<T> =
  | { status: "settled"; result: T } // the operation happened; here is its result
  | { status: "failed"; error: Error } // the operation failed and never will succeed
  | { status: "not-found" } // no call exists, is running, or can still finish
  | { status: "unknown" }; // the system cannot say yet
```

| Situation                                      | What the step does                                                          |
| ---------------------------------------------- | --------------------------------------------------------------------------- |
| The step is already recorded                   | Returns or throws the recorded value, without calling anything              |
| No effect record exists                        | Calls `perform`                                                             |
| The effect is pending                          | Calls `lookup`                                                              |
| `lookup` returns `not-found`                   | Calls `perform`                                                             |
| `perform` resolves, or `lookup` returns `settled` | Settles the effect with the result, records the step and returns the result |
| `perform` rejects and `failsOn` returns `true`, or `lookup` returns `failed` | Fails the effect with the error, records the step and throws the error |
| The effect is already settled or failed        | Records and returns the result, or records and throws the error, without calling `lookup` or `perform` |
| `lookup` returns `unknown`, or a callback rejects | Suspends the execution until `retryIn` has passed, then begins again      |
| The attempt ends uncertain and is attempt number `maxAttempts` | Records and throws a `WorkflowEffectError` with code `EFFECT_OUTCOME_UNKNOWN` |

A rejected `lookup` or `perform` counts as `unknown`, because the step cannot tell whether the operation happened. The step suspends, as `step.delay` does, and the delivery ends as `suspended` with `wakeAt` set to now plus `retryIn`. On the next delivery the step begins again and looks up the effect. If another execution settles or fails the effect while this one waits, the runtime wakes this one straight away, and it reads the stored outcome.

Once settled or failed, an effect never changes. The outcome is stored in the effect record before the step is recorded, so a crash between the two returns the stored outcome on the next delivery without calling the external system. Every execution that begins the same effect receives that outcome.

### Definite failures

Many errors prove that an operation did not happen: a declined card, a request that failed validation, a quota that is used up. Without `failsOn`, the step treats these errors like a timeout. It looks the effect up again after `retryIn`, and it keeps looking until the effect settles. `failsOn` tells the step which errors are final:

```ts
const payment = yield* step.effect("charge", {
  ...chargeEffect,
  failsOn: (err) => err instanceof CardDeclinedError,
});
```

When `perform` throws and `failsOn` returns `true`, the runtime fails the effect with the error, records it as the step's error and throws it to workflow code. Replay throws the same error, and every execution that begins the same effect ID receives it without calling `lookup` or `perform`. To try the operation again, use a new effect ID. A thrown value that is not an `Error` is stored as `new Error(String(value))`. Register the error's class with `registerErrorClass` so that replay throws an instance of it (see [Errors in step records](#errors-in-step-records)).

`failsOn` must return `true` only when the provider has finished the operation and it did not succeed. A timeout, a lost connection or a server error leaves the outcome uncertain, so `failsOn` must return `false` for them. `failsOn` is not called for errors thrown by `lookup`: a lookup that fails proves nothing about the operation.

When `lookup` finds an operation that the provider records as failed, it can return `{ status: "failed", error }`, and the step fails the effect in the same way.

### Giving up

Without `maxAttempts`, an effect whose outcome stays unknown keeps the step suspended indefinitely. For example, a `lookup` that throws because a credential was revoked retries every `retryIn` milliseconds. `maxAttempts` sets how many attempts may end uncertain. When an attempt ends uncertain and it is attempt number `maxAttempts`, the step records and throws a `WorkflowEffectError` with code `EFFECT_OUTCOME_UNKNOWN` instead of suspending:

```ts
try {
  yield* step.effect("charge", { ...chargeEffect, maxAttempts: 5 });
} catch (err) {
  if (err instanceof WorkflowEffectError && err.code === "EFFECT_OUTCOME_UNKNOWN") {
    // the charge may or may not have happened
  }
}
```

A delivery that ends with an `EffectDeliveryError` does not count as an attempt. The effect stays pending, because the operation may still have happened. A later step that begins the same effect ID calls `lookup` before it calls `perform`, and it counts its own attempts. With `retryIn: 60_000` and `maxAttempts: 5`, the step gives up about four minutes after the first attempt.

`input`, the result of `perform` and a settled `lookup` result must be durable values (see [Step values](#step-values)). A `perform` that resolves with nothing, such as a delete, settles the effect with `undefined`. TypeScript checks `input` and the result with `Durable<T>`.

### The external system's guarantees

`step.effect` can only make an operation safe if the external system keeps these promises for each effect ID:

1. Repeated or concurrent `perform` calls make one operation and return one result.
2. `not-found` proves that no call was accepted, is running or has completed, and that no earlier call can still finish. A charge still in flight must not be reported as `not-found`, or a second `perform` charges again.
3. `settled` returns the same result as every successful `perform`, and never changes.
4. `failed`, and an error for which `failsOn` returns `true`, mean that the operation finished without success and no call can still succeed.
5. `unknown` means neither the result nor its absence is known yet.

Many payment and cloud APIs keep the first promise with an idempotency key; pass the effect ID as that key. If an API cannot keep these promises, `step.effect` does not make it safe to call twice.

If workflow code changes while an effect is pending, the new code must still use the same `effectId`, and its `lookup` and `perform` must keep the same guarantees. Otherwise the runtime may be unable to find out what happened or to repeat the operation safely.

### Effect errors

`step.effect` throws a `WorkflowEffectError`, which workflow code can catch. Its `code` is:

- `INVALID_EFFECT_REQUEST` when `effectId` or `operation` is not a non-empty string, `input` is not a plain object of durable values, `retryIn` is not finite and non-negative, `maxAttempts` is not a positive integer, `lookup` or `perform` is not a function, or `failsOn` is given and is not a function. Nothing is sent to the runtime's effect records.
- `EFFECT_REQUEST_MISMATCH` when the effect already exists with a different `operation` or `input`, for example after a deploy changed the amount. Neither callback runs.
- `EFFECT_OUTCOME_UNKNOWN` when the outcome is still unknown after `maxAttempts` attempts (see [Giving up](#giving-up)).

```ts
try {
  yield* step.effect("charge", { ...chargeEffect, input: { amount: 5999 } });
} catch (err) {
  if (err instanceof WorkflowEffectError && err.code === "EFFECT_REQUEST_MISMATCH") {
    // the payment was begun with another amount
  }
}
```

Each is recorded as the step's error, so replay throws the same class and code. Inputs match when their canonical encodings match, so key order does not matter and `{ a: undefined }` differs from `{}`.

The step does not authorize a result: a workflow that submits the same `effectId`, `operation` and `input` receives the stored result.

These failures are not the workflow's to handle, so they do not reach workflow code. The step records nothing, the delivery ends without an outcome and the execution is delivered again after the redelivery delay. The delivery's error is an `InfrastructureError` whose `cause` is an `EffectDeliveryError` with one of these codes:

| Code                        | Cause                                                                      |
| --------------------------- | -------------------------------------------------------------------------- |
| `EFFECT_CLIENT_UNAVAILABLE` | The runtime's effect records could not be reached. Usually recovers by itself |
| `EFFECT_HANDLER_INVALID`    | `lookup` returned a malformed answer, a result is not a durable value, or `failsOn` threw |
| `EFFECT_RUNTIME_INVARIANT`  | The runtime's effect records hold invalid data or behaved incorrectly      |

The last two need an operator's repair if they persist. The effect stays pending until a delivery settles it.

If two calls settle one effect with different outcomes, which only happens when the external system breaks its guarantees, the first outcome stays and is returned. The runtime logs a warning with both outcomes and keeps a permanent conflict report on the effect record for operators. Two failures with different errors do not conflict: the first error stays, without a report.

Effect records outlive executions. Cancelling or pruning an execution with a pending effect does not cancel the external operation, and the effect may still settle.

## Step keys

Each step takes a key as its first argument, and the step's record is stored under that key. Replay finds the record by the key, so the key must stay the same across deploys for as long as executions that recorded it are in flight.

```ts
yield* step.run("charge", () => payments.charge(orderId));
yield* step.delay("courier", 2 * DAY);
yield* step.poll("deployment-ready", opts, predicate);
```

Inside a loop, put something that identifies the iteration in the key:

```ts
for (let i = 0; i < items.length; i++) {
  yield* step.run(`process-item:${i}`, () => processItem(items[i]));
}
```

If two steps in the same execution use the same key, the runtime throws: `"Each step in a loop must have a unique cache key."`

`step.run`, `step.delay` and `step.poll`, and a store's `get` and `when`, can still be called without a key. **Keyless steps are deprecated.** A keyless step takes its key from a hash of its file, line and column, so editing lines above it, reformatting or bundling the file changes its key. The driver logs a deprecation warning when a keyless step has no record and runs, and a runtime created with `requireStepKeys: true` rejects such a step with `WorkflowReplayError` (`STEP_KEY_REQUIRED`). See [Replay Stability](./replay-stability.md#step-keys).

`step.start` and `step.call` always take a key, because the key identifies the start request as well as the cached step. Keep it stable across deploys: a different key starts a different target. `step.join`, `step.effect`, `step.all` and `step.race` always take a key too; an effect itself is identified by its `effectId`.

A deploy that changes a recorded step's key, or removes a recorded step, does not run the step again. The execution's next delivery diverges and the execution is parked until an operator clears it. Workflow versions and `step.patched` let you change a workflow while executions are in flight. See [Replay Stability](./replay-stability.md).

## Errors

Errors fall into two groups, and only the first reaches workflow code.

| Workflow errors (catchable)                                                   | Infrastructure errors (not catchable)                          |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------- |
| Errors thrown by your step functions                                         | The heap or store database throws                              |
| Errors thrown by your store updaters, selectors, claims and initial states    | A worker crashes or exits before reporting an outcome          |
| `StoreValidationError` (a state failed its schema)                            | The execution client throws during `step.start` or `step.call` |
| `WorkflowStartError` (`step.start` or `step.call` was rejected)               | `step.effect` cannot reach or trust its effect records, or a callback returns a malformed answer (`EffectDeliveryError`) |
| `WorkflowJoinError` (a joined execution failed, was cancelled or does not exist, or a workflow joined itself) | The join client throws during `step.join` or `step.call`       |
| `WorkflowValueError` (a value is not durable, or failed its step's schema)    |                                                                |
| `WorkflowEffectError` (`step.effect` was invalid or mismatched)               |                                                                |
| `WorkflowCancelledError` (a [cancellation](./cancellation.md) was delivered at this step) |                                                    |
| `WorkflowReplayError` (a keyless step under `requireStepKeys`, or `step.patched` inside a combinator branch) | |

Steps do not call runtime clients themselves. A store step yields a request, and the driver makes the call outside workflow code, then resumes the step with the result. If the call fails with an infrastructure error, the driver does not resume the step, so a `try/catch` around it never runs:

```ts
const order = step.store(OrderStore, { id: event.params.orderId });

try {
  yield* order.update("mark-paid", (draft) => {
    draft.status = "paid";
  });
} catch (err) {
  // Runs for StoreValidationError or an error thrown by the updater.
  // Does not run if SQLite throws: the delivery is abandoned instead.
}
```

An infrastructure error is not an outcome. The delivery is not acknowledged, the execution record keeps its status, and the runtime delivers the execution again after a redelivery delay (1 second, doubling with each consecutive failed delivery, up to 5 minutes). Completed steps replay from the heap, so the retry resumes where the failed delivery stopped.

A divergence is neither kind of error. When the code no longer matches the execution's recorded history, the driver ends the delivery without throwing into workflow code and parks the execution. See [Divergence](./replay-stability.md#divergence).
