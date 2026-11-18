# Step Values

Notation Durable records every step result, workflow parameter, workflow return value, and store state update, then decodes the recorded value on replay. Every recorded value must survive encoding and decoding unchanged across deliveries.

```ts [src/workflows/reserve-stock.ts]
import { workflow } from "@notation/durable";

export const reserveStock = workflow<{ sku: string; qty: number }>(
  async function* (step, event) {
    const reservation = yield* step.run("reserve-stock", async () => {
      return {
        id: "res-42",
        expiresAt: new Date(Date.now() + 15 * 60 * 1000),
        lines: new Map([[event.params.sku, BigInt(event.params.qty)]]),
        tags: new Set(["priority"]),
        note: undefined,
      };
    });

    // Date, Map, Set, bigint, and undefined properties revive with their exact types.
    return {
      reservationId: reservation.id,
      expiresAt: reservation.expiresAt,
      reservedQty: reservation.lines.get(event.params.sku) ?? 0n,
      hasPriority: reservation.tags.has("priority"),
      note: reservation.note,
    };
  }
);
```

## Durable and non-durable types

Durable supports JSON primitives plus undefined values, bigints, dates, maps, sets, and errors:

| Value kind | Durability rule |
| :--- | :--- |
| Primitives | `null`, booleans, finite numbers, strings, `bigint`, and `undefined` |
| Collections | Plain arrays, plain objects, `Map`, and `Set` of durable values |
| Dates | Valid built-in `Date` instances without own properties |
| Errors | `Error` instances with standard metadata and durable own fields |
| Functions and symbols | Rejected everywhere, including as object keys or collection entries |
| Non-finite numbers | `NaN`, `Infinity`, and `-Infinity` are rejected |
| Custom class instances | Rejected outside plain objects, arrays, `Date`, `Map`, `Set`, and `Error` |
| Subclasses and cycles | Subclasses of `Array`, `Date`, `Map`, or `Set`, sparse arrays, accessors, and reference cycles are rejected |

Plain objects keep `undefined` properties and literal `$type` keys unchanged, and `-0` normalizes to `0`. Shared references in sibling branches encode cleanly as long as the graph contains no cycles.

### Custom error classes

Durable preserves standard error properties, causes, nested `AggregateError` lists, and durable custom fields on thrown `Error` instances. Non-durable fields attached to an error are omitted rather than failing the step.

Register custom `Error` subclasses with `registerErrorClass` at startup so replay revives their prototype without invoking the constructor:

```ts [src/errors.ts]
import { registerErrorClass } from "@notation/durable";

export class OutOfStockError extends Error {
  code = "OUT_OF_STOCK";
  sku: string;

  constructor(sku: string) {
    super(`Insufficient inventory for ${sku}`);
    this.name = "OutOfStockError";
    this.sku = sku;
  }
}

registerErrorClass("OutOfStockError", OutOfStockError);
```

Standard JavaScript errors and Durable's built-in error classes register automatically (see [Errors](../reference/errors.md)). Unregistered subclasses revive as plain `Error` instances while keeping their recorded properties.

If workflow or step code throws a non-durable value that is not an error, Durable converts it to a plain `Error` stringified from that value on both the first run and replay.

## Where durability applies

Durable enforces value durability at every persistence boundary:

- **Step results** – Outputs from `step.run`, concurrent branch steps, and `step.effect` callbacks.
- **Workflow inputs and outputs** – Parameters, request context, workflow return values, and child start payloads.
- **Effect inputs** – Request input objects passed to [external effects](./external-effects.md).
- **Store state and selections** – Initial state, committed updates, and values returned by store reads and waits.

## Non-durable rejections

TypeScript checks durability at compile time and rejects return types containing functions or symbols before the code runs. Because TypeScript cannot distinguish a custom class instance from a plain object with the same shape, Durable also validates every value at runtime.

When a step returns a non-durable value at runtime, Durable records `WorkflowValueError` with `code: "INVALID_STEP_VALUE"` and `stepKey` as the step's error, then throws it without re-running the step function on replay:

```ts [src/workflows/bad-reservation.ts]
import { WorkflowValueError, workflow } from "@notation/durable";

class StockHold {
  constructor(public id: string) {}
}

export const reserveStock = workflow(async function* (step) {
  try {
    return yield* step.run("reserve-stock", async () => {
      return new StockHold("hold-42");
    });
  } catch (err) {
    if (err instanceof WorkflowValueError && err.code === "INVALID_STEP_VALUE") {
      return { status: "invalid-value", failedStep: err.stepKey };
    }
    throw err;
  }
});
```

Returning a non-durable value from the workflow itself fails the execution with `WorkflowValueError` (`INVALID_STEP_VALUE`). Triggering an execution with non-durable parameters or context throws `ExecutionAdmissionError` immediately.

## Decode-copy guarantees

A step never returns the exact object reference produced by its callback. On the first run, Durable encodes the return value and decodes a fresh copy before handing it back to the workflow:

```ts [src/workflows/decoded-copy.ts]
import { workflow } from "@notation/durable";

export const copyCheck = workflow(async function* (step) {
  const original = { id: "res-42", note: undefined };

  const reservation = yield* step.run("reserve-stock", async () => original);

  // reservation !== original even on the first delivery.
  return { sameRef: reservation === original, reservation };
});
```

Returning the decoded copy ensures the first delivery sees the exact same structure, `Object.keys`, normalized `-0`, and revived error shapes that subsequent replays read from the step record.

## Step schemas

When a step needs a richer runtime type such as a class instance, return a durable representation from the step function and pass a [Standard Schema](https://standardschema.dev) validator via `{ schema }`. Durable validates the decoded value through the schema on the first run and on every replay, returning the schema's output type:

```ts [src/workflows/quote-price.ts]
import { WorkflowValueError, workflow } from "@notation/durable";
import * as v from "valibot";

class Money {
  constructor(
    public readonly amount: string,
    public readonly currency: string
  ) {}
}

const MoneySchema = v.pipe(
  v.object({
    amount: v.string(),
    currency: v.string(),
  }),
  v.transform((raw) => new Money(raw.amount, raw.currency))
);

export const quoteOrder = workflow(async function* (step) {
  try {
    const price = yield* step.run(
      "quote-price",
      async () => ({ amount: "12.50", currency: "GBP" }),
      { schema: MoneySchema }
    );

    return `${price.amount} ${price.currency}`;
  } catch (err) {
    if (err instanceof WorkflowValueError && err.code === "STEP_SCHEMA_INVALID") {
      return "invalid-quote";
    }
    throw err;
  }
});
```

Any Standard Schema v1 library such as Zod or Valibot works with `{ schema }`. The step function must return the schema's durable input type, while the step returns the schema's output type.

If validation returns issues on the first run, Durable records the raw result with a rejected verdict and throws `WorkflowValueError` with `code: "STEP_SCHEMA_INVALID"` on the first run and every replay, even if a later deployment relaxes the schema. If a value fails both durability encoding and schema validation, `INVALID_STEP_VALUE` takes precedence.
