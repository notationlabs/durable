# Introduction

Durable is a TypeScript library for running workflows and keeping state that outlives any single process. It has two parts: **workflows** and **stores**.

Durable runs inside your application on Bun or Node, and keeps its records in a SQLite file. It needs no separate server.

## Workflows

A workflow is an async generator function made of steps. Durable records the result of each step as it finishes. A workflow can therefore pause for days, survive a restart and retry a failed step, without repeating the steps that already finished.

```ts
import { workflow } from "@notation/durable";

export const onboarding = workflow<{ email: string }>(async function* (step, event) {
  const { email } = event.params;

  yield* step.run("send-welcome", async () => {
    await sendEmail(email, "Welcome to Acme");
  });

  yield* step.delay("wait-three-days", 3 * DAY);

  yield* step.run("send-tips", async () => {
    await sendEmail(email, "Three tips for your first week");
  });
});
```

Use workflows for:

- sequences that wait between steps, such as onboarding emails and trial reminders;
- calls to unreliable services, retried on a schedule;
- payments and other operations that must happen once;
- processes that start other workflows and wait for their results; and
- work that runs several independent steps at the same time.

## Stores

A store is a piece of state with a name, an ID and a schema. It lives outside any one workflow. Workflows read and update a store as steps, and can wait until its state meets a condition. Code outside workflows, such as an HTTP handler, can read and write it too.

This workflow waits until a manager approves an expense:

```ts
import { defineStore, workflow } from "@notation/durable";
import * as v from "valibot";

export const Approval = defineStore(
  "approval",
  v.object({ approved: v.boolean() }),
  { initial: () => ({ approved: false }) }
);

export const expense = workflow<{ expenseId: string }>(async function* (step, event) {
  const approval = step.store(Approval, { id: event.params.expenseId });

  yield* approval.when("approved", (state) => state.approved);

  yield* step.run("pay-expense", async () => {
    await payExpense(event.params.expenseId);
  });
});
```

An HTTP handler records the manager's decision, and the waiting workflow continues:

```ts
const approval = runtime.stores.store(Approval, expenseId);

await approval.update((draft) => {
  draft.approved = true;
});
```

Use stores for:

- approvals and other decisions that wait for a person;
- agent conversations and mailboxes that receive messages over time; and
- state that several workflows share.

## Next steps

- [Installation](./installation.md)
- [Quick start](./quickstart.md) builds a small application that runs a workflow.
