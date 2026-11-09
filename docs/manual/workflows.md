# Defining workflows

A workflow is an async generator function wrapped with `workflow()`. This page uses a workflow that sends an invoice to a customer.

```ts
import { workflow } from "@notation/durable";

type Params = {
  invoiceId: string;
  email: string;
};

type Result = {
  sentTo: string;
};

export const sendInvoice = workflow<Params, Result>(async function* (step, event, logger) {
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

## Arguments

The function receives three arguments:

| Argument | Contains |
| --- | --- |
| `step` | The step functions: `run`, `delay`, `poll` and the others in this manual |
| `event` | The workflow ID, the execution ID, the params and the context |
| `logger` | The `pino` logger that the worker was created with |

Each step is called with `yield*`. A plain `yield` is not a step: it fails the execution with the error `Step runners must yield a StepKey object`.

## Types

`workflow` takes three optional type arguments:

| Type argument | Default | Describes |
| --- | --- | --- |
| `Params` | any object, or no params | `event.params` |
| `Result` | any durable value | The value the workflow returns |
| `Context` | `ReadonlyMap<any, any>` | `event.context` |

When the workflow is registered in a router, these types reach the SDK. TypeScript then checks the params at every call that starts the workflow, and types the result that the SDK returns.

Params and the result must be durable values, such as plain objects, strings and dates. [Step values](./step-values.md) lists them.

## Event

| Field | Type | Contains |
| --- | --- | --- |
| `workflowId` | `string` | The ID the workflow has in the router |
| `executionId` | `string` | The ID of this execution |
| `params` | `Params` | The params the execution was started with |
| `context` | `ReadonlyMap` | Extra values the caller attached to the execution |

The caller passes `context` as a plain object, and the workflow reads it as a map:

```ts
await sdk.trigger({
  workflowId: "send-invoice",
  params: { invoiceId: "inv-42", email: "ada@example.com" },
  context: { requestedBy: "user-7" },
});
```

```ts
const requestedBy = event.context.get("requestedBy");
```

HTTP middleware can also set context values, such as the ID of the user who made the request. See [HTTP server](./http-server.md).

## Router

A router gives each workflow an ID:

```ts
import { createWorkflowRouter } from "@notation/durable";

export const router = createWorkflowRouter({
  "send-invoice": sendInvoice,
  "send-reminder": sendReminder,
});

export type Router = typeof router;
```

The worker uses the router to find the workflow for each execution. The SDK uses the `Router` type to check workflow IDs and params.

The SDK accepts an execution for any workflow ID it is given, including one that the router does not have. That execution fails when a worker runs it, with the error `No workflow registered for "<id>"`.

`createWorkflow` is another name for `workflow`.
