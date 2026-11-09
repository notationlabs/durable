# Step keys

Every step has a key. Durable stores each step's record under its key, and a delivery finds a step's record by its key. This page uses a workflow that sends payment reminders for a list of invoices.

```ts
yield* step.run("load-invoices", async () => {
  return await invoices.overdue(customerId);
});
```

## Keys must be unique

Two steps in one execution cannot have the same key. A second step with a key that the execution has already used fails the execution, with the error `Each step in a loop must have a unique cache key.` The workflow cannot catch this error.

A step inside a loop therefore needs a key that changes on each pass:

```ts
for (const invoice of overdue) {
  yield* step.run(`remind:${invoice.id}`, async () => {
    await mailer.sendReminder(invoice);
  });
}
```

## Steps without a key

`step.run`, `step.delay` and `step.poll` accept a call without a key:

```ts
const overdue = yield* step.run(async () => {
  return await invoices.overdue(customerId);
});
```

Durable then makes a key from the position of the call in the source code: the file path, line and column. This key is the same in every delivery, as long as the code does not change.

**A key made from the source position changes when the code moves.** These changes all move a call:

- editing a line above the step;
- deploying the code to a different folder; and
- bundling or minifying the code differently.

An execution that is suspended when such a change is deployed finds no record under the new key. Its next delivery runs the step again. For a reminder step, the customer receives a second reminder.

A keyless step inside a loop also fails, because every pass has the same source position.

Give every step a key. The steps for child workflows, effects and concurrency always take one, and so do most store steps.

## Changing a workflow

Durable looks up records only by key. It does not check that the steps of an execution still come in the same order. For an execution that is suspended while new code is deployed:

| Change | Effect on the next delivery |
| --- | --- |
| Remove a step that has a record | The record is ignored |
| Add a step before the point the execution reached | The new step runs |
| Rename a step that has a record | The step runs again under the new key |
| Change the code inside a step that has a record | The step returns its old record |
| Reorder steps that have records | Each step returns its own record |

Keep a step's key when you change its code, unless the step must run again.
