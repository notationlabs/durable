# Step values

Durable records each step's result, and a later delivery reads the result back from the record. A step result must therefore be a value that Durable can record and read back unchanged. This page calls these durable values. It uses a workflow that reserves stock for an order.

## Durable values

| Durable | Not durable |
| --- | --- |
| `null`, booleans, finite numbers, strings, `bigint` | Functions and symbols |
| `undefined`, as a whole value, a property or an array item | Class instances |
| Valid `Date`s, and `Map`s and `Set`s of durable values | `NaN`, `Infinity`, invalid `Date`s, sparse arrays and cycles |
| Errors, with their class, `code`, `cause` and other fields | Objects with symbol keys, getters or setters |
| Arrays and plain objects of durable values | Subclasses of `Array`, `Date`, `Map` and `Set` |

A `Date`, `Map`, `Set` or `bigint` reads back as the same type:

```ts
const reservation = yield* step.run("reserve-stock", async () => {
  return {
    id: "r-42",
    expiresAt: new Date(Date.now() + 15 * 60_000),
  };
});

reservation.expiresAt; // a Date, in every delivery
```

## Where the rule applies

These values must be durable:

- the result of `step.run`;
- the params and result of a workflow;
- the params of a [child workflow](./child-workflows.md);
- the input and result of an [effect](./effects.md); and
- the state of a [store](./stores.md), and the values its selectors return.

## Values that are not durable

A step that returns a value that is not durable fails the first time it runs:

```ts
const reservation = yield* step.run("reserve-stock", async () => {
  return new Reservation("r-42");
});
// throws WorkflowValueError, with code "INVALID_STEP_VALUE"
```

Durable records the error as the step's outcome, so every later delivery throws the same error. The workflow can catch it. A mistake therefore shows up the first time the step runs, and not in a later delivery.

TypeScript catches most of these values before the code runs. The step functions reject a result type that contains a function. TypeScript cannot tell a class instance from a plain object, so only the check at run time rejects class instances.

## Copies

A step never returns the object its function returned. Durable encodes the result for the record, decodes it again, and returns the decoded copy. This is the same value that later deliveries read from the record:

```ts
const original = { id: "r-42", note: undefined };

const reservation = yield* step.run("reserve-stock", async () => original);

reservation === original; // false
reservation; // { id: "r-42", note: undefined }
```

## Schemas

A step that needs a class instance returns a durable form of it, and passes a schema that builds the instance. The schema is any [Standard Schema](https://standardschema.dev), such as one from Zod or Valibot:

```ts
const price = yield* step.run(
  "quote-price",
  async () => {
    return { amount: "12.50", currency: "GBP" };
  },
  {
    schema: MoneySchema.transform((value) => new Money(value)),
  }
);

price; // a Money, in every delivery
```

The record holds the durable form. Every delivery passes it through the schema, and the step returns the schema's output.

If the schema rejects the value, the step throws `WorkflowValueError` with the code `STEP_SCHEMA_INVALID`, in the first delivery and every later one.
