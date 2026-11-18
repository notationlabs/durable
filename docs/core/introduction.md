# Introduction

Notation Durable is a TypeScript workflow and state library that runs locally or anywhere with SQLite. Workflows are async generator functions whose steps persist their outcomes as they complete, so an execution can pause for days, survive restarts, and resume from its last completed step.

## Workflows

A workflow is an async generator function wrapped with `workflow()`. Each durable action inside the generator runs through `yield*`, which records the step result before the function advances to the next line.

This onboarding workflow sends a welcome email, waits three days without holding a process open, and sends a follow-up email:

::code-group

```ts [src/email.ts]
export async function sendEmail(to: string, subject: string) {
  console.log(`Sending "${subject}" to ${to}`);
}
```

```ts [src/workflows/onboarding.ts]
import { workflow } from "@notation/durable";
import { sendEmail } from "../email";

const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;

export const onboarding = workflow<{ email: string }, void>(async function* (
  step,
  event
) {
  const { email } = event.params;

  yield* step.run("send-welcome", async () => {
    await sendEmail(email, "Welcome to Acme");
  });

  yield* step.delay("wait-three-days", THREE_DAYS_MS);

  yield* step.run("send-tips", async () => {
    await sendEmail(email, "Three tips for your first week");
  });
});
```

::

Use workflows for:

- **Delayed sequences** – Onboarding emails, trial reminders, and scheduled follow-ups that pause for hours or days between steps.
- **Retried calls** – Requests to external services that retry on a configured interval when transient failures occur.
- **Idempotent side effects** – Payments and external mutations guarded against duplicate execution after a crash.
- **Child workflows** – Coordinator flows that start other workflows and wait for their outcomes.
- **Concurrent branches** – Independent steps executed together and combined with all-settled, fail-fast, or race semantics.

## Stores

A store is durable state identified by a name, a Standard Schema validator, and an ID. Stores live outside any single execution – workflows read, update, and wait on stores as steps, while external code such as an HTTP route handler reads and mutates the same store through your runtime instance.

This example defines an `approval` store, pauses an expense workflow until a manager approves it, and records the approval from application code:

::code-group

```ts [src/stores/approval.ts]
import { defineStore } from "@notation/durable";
import * as v from "valibot";

export const Approval = defineStore(
  "approval",
  v.object({ approved: v.boolean() }),
  { initial: () => ({ approved: false }) }
);
```

```ts [src/payments.ts]
export async function payExpense(expenseId: string) {
  console.log(`Paying expense ${expenseId}`);
}
```

```ts [src/workflows/expense.ts]
import { workflow } from "@notation/durable";
import { payExpense } from "../payments";
import { Approval } from "../stores/approval";

export const expense = workflow<{ expenseId: string }, void>(async function* (
  step,
  event
) {
  const approval = step.store(Approval, { id: event.params.expenseId });

  yield* approval.when("approved", (state) => state.approved);

  yield* step.run("pay-expense", async () => {
    await payExpense(event.params.expenseId);
  });
});
```

```ts [src/durable/runtime.ts]
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/bun";

const db = createSqliteDb({ path: "./.db/durable.sqlite" });

export const runtime = new SqliteRuntime(db);
```

```ts [src/routes/approve.ts]
import { runtime } from "../durable/runtime";
import { Approval } from "../stores/approval";

export async function approveExpense(expenseId: string) {
  const approval = runtime.stores.store(Approval, expenseId);

  await approval.update((draft) => {
    draft.approved = true;
  });
}
```

::

Once `approveExpense` sets `approved` to true, Durable automatically schedules the waiting workflow to resume and run the payment step.

Use stores for:

- **Approvals and callbacks** – Workflows that wait for a person or webhook to record a decision.
- **Mailboxes and queues** – Long-lived state that receives messages over time and lets workflows claim items atomically.
- **Shared coordination** – Counters, locks, or records shared across multiple workflows and HTTP handlers.

## How Durable runs in your app

Every workflow execution progresses through a sequence of deliveries. During a delivery, a worker process runs the workflow generator from the top, returns previously recorded step results immediately, and executes new steps until the workflow finishes or suspends at a wait condition.

While an execution is suspended – waiting out a `step.delay`, an unsatisfied `step.poll`, or a `store.when` – no worker process stays running. When the wake time arrives or the awaited store updates, a dispatcher launches a fresh worker delivery to replay finished steps and continue from the waiting step.

Follow the [Quickstart](./quickstart.md) to wire up a SQLite runtime, worker process, and dispatcher in a runnable Bun application.
