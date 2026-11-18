# Workflows

A workflow is an async generator function wrapped with `workflow()`. Notation Durable runs the generator inside deliveries, passing a step runner, an execution event, and a logger.

```ts [src/workflows/send-invoice.ts]
import { workflow } from "@notation/durable";
import { invoices } from "../invoices";
import { mailer } from "../mailer";

type Params = {
  invoiceId: string;
  email: string;
};

type Result = {
  sentTo: string;
};

export const sendInvoice = workflow<Params, Result>(async function* (
  step,
  event,
  logger
) {
  const { invoiceId, email } = event.params;

  const pdf = yield* step.run("render-pdf", async () => {
    return await invoices.renderPdf(invoiceId);
  });

  yield* step.run("send-email", async () => {
    await mailer.send({ to: email, attachment: pdf.url });
  });

  logger.info({ invoiceId }, "Invoice sent");

  return { sentTo: email };
});
```

## Function signature

The wrapped generator function receives three arguments:

| Argument | Description |
| :--- | :--- |
| `step` | Step runner providing `run`, `delay`, `poll`, `store`, `start`, `join`, `call`, `effect`, `all`, and `race` |
| `event` | `WorkflowEvent` containing the execution's ID, workflow ID, input `params`, and `context` map |
| `logger` | Configured `pino` logger instance |

Every step call must use `yield*` so control delegates into the step generator. Calling a step with plain `yield` instead of `yield*` fails the execution immediately with an error.

`createWorkflow` is an alias for `workflow` – both functions share the exact same implementation and type signature.

## Type parameters

`workflow<Params, Result, Context>` accepts up to three optional type parameters:

| Type parameter | Description |
| :--- | :--- |
| `Params` | Type of `event.params` inside the workflow and the `params` payload required when triggering |
| `Result` | Return type of the async generator and the resolved value of `triggerAndWait` |
| `Context` | `ReadonlyMap` type of `event.context` |

Pass only `Params` when the workflow returns `void`, or omit all type arguments when a workflow takes no parameters:

```ts [src/workflows/signatures.ts]
const ping = workflow(async function* (step) { /* ... */ });
const welcome = workflow<{ email: string }>(async function* (step, event) { /* ... */ });
```

Both `Params` and `Result` must be [durable values](./step-values.md). Passing a non-durable type such as a function produces a compile-time error, and returning a non-durable value at runtime fails the execution with `WorkflowValueError`.

## WorkflowEvent

The second argument, `event`, has type `WorkflowEvent<Params, Context>` and exposes four fields:

| Field | Description |
| :--- | :--- |
| `workflowId` | Key under which this workflow is registered in the router |
| `executionId` | Unique string identifier for this execution |
| `params` | Input payload of type `Params` supplied when triggering the execution |
| `context` | `ReadonlyMap` of metadata attached to the execution |

Callers pass `context` as a plain object when triggering an execution, and Durable converts it to a `ReadonlyMap` before delivering it to the workflow:

::code-group

```ts [src/trigger.ts]
await sdk.trigger({
  workflowId: "send-invoice",
  params: { invoiceId: "inv-42", email: "ada@example.com" },
  context: { requestedBy: "user-7" },
});
```

```ts [src/workflows/send-invoice.ts]
const requestedBy = event.context.get("requestedBy");
```

::

HTTP trigger middleware can also populate `context` entries before admission, such as the authenticated user ID from an incoming request. See [HTTP Server](./http-server.md) for middleware configuration.

## Workflow router

`createWorkflowRouter` registers workflows under string IDs and returns the map unchanged with full type inference preserved:

```ts [src/workflows/router.ts]
import { createWorkflowRouter } from "@notation/durable";
import { sendInvoice } from "./send-invoice";
import { sendReminder } from "./send-reminder";

export const router = createWorkflowRouter({
  "send-invoice": sendInvoice,
  "send-reminder": sendReminder,
});

export type Router = typeof router;
```

Durable uses `router` to look up the workflow matching each execution's `workflowId`, while callers import `type Router` so TypeScript checks workflow IDs, `params`, and return values at compile time.

Admission does not check `workflowId` against the router because the caller and worker may run in separate processes. If an execution is triggered with an ID that is absent from the worker's router, the execution fails on its first delivery with `No workflow registered for "<workflowId>"`.
