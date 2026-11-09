# Introduction

Durable is a workflow engine that provides a durable runtime for JavaScript generator functions.

Workflows are checkpointed with primitives such as `yield* step.run()` and `yield* step.delay()`. The runtime persists step results and resumes delayed, retried, or state-waiting workflows across process restarts.

```ts
import { workflow } from "@notation/durable";

const checkout = workflow(async function* (step) {
  const order = yield* step.run(() => createOrder());
  yield* step.delay(30_000);
  const payment = yield* step.run(() => chargeCard(order.id));
  return yield* step.run(() => confirmOrder(order.id, payment.id));
});
```


## How it compares

|                       | **Durable**                   | **Temporal**            | **Inngest**            | **Cloudflare Workflows**    |
| --------------------- | ------------------------------- | ----------------------- | ---------------------- | --------------------------- |
| **State persistence** | SQLite file                     | Temporal Server cluster | Inngest Cloud (SaaS)   | Cloudflare Durable Objects  |
| **Infrastructure**    | Process plus a file             | Multi-service cluster   | SaaS platform          | Cloudflare Workers platform |
| **Self-hostable**     | Yes, single service             | Yes, but complex        | No                     | No                          |
| **Embeddable**        | Yes                             | No                      | No                     | No                          |
| **Portability**       | Runs anywhere JS runs           | Needs Temporal Server   | Needs Inngest platform | Needs Cloudflare            |
| **Deployment safety** | Out-of-order step detection     | Manual versioning       | Manual versioning      | Manual versioning           |

## Execution model

The execution model works by replaying the generator. When a step completes, the runtime snapshots its return value to a persistent heap and either advances to the next step or yields control back to the orchestrator. When the orchestrator later resumes the workflow, the runtime feeds each previously completed step its stored result instead of re-executing it, so the generator picks up exactly where it stopped.

This differs from `await step.run()` engines such as Inngest and Cloudflare Workflows, which intercept Promises through platform-specific wrappers. 

Durable uses `yield*`, JavaScript's native generator delegation, as a bi-directional protocol between the workflow and the runtime. 

## Packages

Durable is published as two packages. `@notation/durable` has one entry point for each part:

| Entry point                     | Role                                                                                                                      |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `@notation/durable`             | SDK with `workflow`, `createWorkflowRouter`, `RetryableError`, `WorkflowRunner`, `ExecutionDispatcher`, and the local and HTTP SDKs |
| `@notation/durable/runtime`     | Base types, the `Runtime` contract and the state helpers for runtime authors                                             |
| `@notation/durable/sqlite`      | `SqliteRuntime`: driver-agnostic SQLite execution records, heap, and stores                                               |
| `@notation/durable/sqlite/bun`  | `createSqliteDb` for `bun:sqlite`                                                                                         |
| `@notation/durable/sqlite/node` | `createSqliteDb` for `node:sqlite`                                                                                        |
| `@notation/durable/worker`      | Subprocess invoker and worker process                                                                                     |
| `@notation/durable/http`        | Runtime-neutral HTTP routes and middleware using standard `Request` and `Response`                                        |

`@notation/durable-conformance` contains the Vitest qualification suites for third-party runtimes and store connectors. It has `@notation/durable` as a peer dependency.

A runtime has four clients, and a dispatcher drives them:

- The **execution client** keeps one durable record per execution: its event, its status and outcome, and when it is next due.
- The **heap** stores step results so they survive restarts.
- The **store** client holds schema-validated shared state and the waiters of suspended workflows.
- The **effect** client records the external operations that `step.effect` calls, so a replay reads their results instead of calling again.
- The **dispatcher** (`ExecutionDispatcher`) claims due executions, hands each to an invoker, and records the outcome.
