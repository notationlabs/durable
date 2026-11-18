# Stores

A store holds schema-validated state outside any single execution, identified by a store name and ID. Multiple workflows and [external application handlers](./external-store-access.md) can read, update, and wait on the same store concurrently.

```ts [src/stores/conversation.ts]
import { defineStore } from "@notation/durable";
import * as v from "valibot";

const MessageSchema = v.object({
  id: v.string(),
  role: v.picklist(["user", "assistant"]),
  text: v.string(),
});

export const Conversation = defineStore(
  "conversation",
  v.object({
    status: v.picklist(["open", "closed"]),
    inbox: v.array(MessageSchema),
    messages: v.array(MessageSchema),
  }),
  {
    initial: () => ({ status: "open", inbox: [], messages: [] }),
  }
);
```

## Defining a store

`defineStore(name, schema, { initial })` binds a store name, a [Standard Schema](https://standardschema.dev) (such as Zod, Valibot, or ArkType), and a synchronous `initial(id)` factory:

- **`name`** – Identifies the store family across workflows and external clients
- **`schema`** – Validates state on creation and every update
- **`initial(id)`** – Returns the starting state synchronously when a store ID is accessed for the first time

Store states must consist of [durable values](./step-values.md). Functions and arbitrary class instances are rejected at compile time and runtime.

## Obtaining handles and lazy creation

Inside a workflow, call `step.store(definition, { id? })` to obtain a `WorkflowStore` handle:

```ts [src/workflows/append-message.ts]
import { workflow } from "@notation/durable";
import { Conversation } from "../stores/conversation";

export const appendMessage = workflow<{ conversationId: string; text: string }>(
  async function* (step, event) {
    const conversation = step.store(Conversation, {
      id: event.params.conversationId,
    });

    yield* conversation.update("append", (draft) => {
      draft.messages.push({
        id: event.executionId,
        role: "user",
        text: event.params.text,
      });
    });

    const snapshot = yield* conversation.get("read-after");
    return snapshot.state;
  }
);
```

`step.store` is synchronous, takes no `yield*`, and records no step. Call it as often as needed across loops or helper functions; handles with the same definition and ID target the same store. Omitting `{ id }` defaults the store ID to `event.executionId`, giving each execution an isolated store.

Neither `defineStore` nor `step.store` creates a store record. Instead, the first read, update, or wait operation on a missing store invokes `initial(id)` and inserts the initial state at `version: 0` with a fresh `instanceId` in the same atomic write.

If two callers operate on a missing store at the same time, Notation Durable records one initial state and applies both operations cleanly. If `initial(id)` throws, the error reaches workflow code from the first operation that triggers creation.

## Reading with `get` and `select`

Every operation on a `WorkflowStore` handle is a durable step driven with `yield*`:

```ts [src/workflows/read-conversation.ts]
const snapshot = yield* conversation.get("load");
// snapshot.state      -> current Conversation state
// snapshot.version    -> 0 on creation, +1 per mutation
// snapshot.instanceId -> unique UUID for this store instance

const lastMessage = yield* conversation.select("last-message", (state) => {
  return state.messages.at(-1);
});
```

`get` accepts an optional step key (`conversation.get()`), while `select` requires an explicit key. `select` passes a cloned copy of the state to your synchronous selector and records only the selector's return value (which must be a durable value). Mutating the argument inside `select` never affects stored state.

Because `get` and `select` are recorded steps, replays return the originally recorded snapshot or selection without reading the store again. Use a fresh step key to observe newer state later in the workflow.

## Mutating with `update`

`update(key, updater)` runs a synchronous updater against a mutable state draft, validates the result against the schema, increments the store version, and returns the updated state along with both version numbers.

In addition to plain objects and arrays, drafts support direct mutations on `Date`, `Map`, and `Set` values:

```ts [src/workflows/update-ledger.ts]
import { defineStore, workflow } from "@notation/durable";
import * as v from "valibot";

export const Ledger = defineStore(
  "ledger",
  v.object({
    openedAt: v.date(),
    totals: v.map(v.string(), v.bigint()),
    seenIds: v.set(v.string()),
    notes: v.array(v.string()),
  }),
  {
    initial: () => ({
      openedAt: new Date(),
      totals: new Map(),
      seenIds: new Set(),
      notes: [],
    }),
  }
);

export const recordEntry = workflow<{ entryId: string; item: string; cents: bigint }>(
  async function* (step, event) {
    const ledger = step.store(Ledger, { id: "main" });

    return yield* ledger.update("apply-entry", (draft) => {
      draft.openedAt.setUTCMilliseconds(0);
      draft.totals.set(
        event.params.item,
        (draft.totals.get(event.params.item) ?? 0n) + event.params.cents
      );
      draft.seenIds.add(event.params.entryId);
      draft.notes.push(`Recorded ${event.params.entryId}`);
    });
  }
);
```

An updater can either mutate `draft` in place and return `undefined`, or return a full replacement state object. Both paths run schema validation and track changed paths so waiting workflows wake accurately.

Updaters must be synchronous, deterministic, and side-effect-free; returning a Promise throws an error. Because concurrent writers can trigger compare-and-swap retries before commit, `updater` may run more than once for a single `update` call.

Once committed, replays return the recorded `StoreUpdateResult` without running `updater` again – even if a crash occurred between the store commit and recording the step.

## Conditional updates and deletions

When a workflow reads a snapshot, performs async work, and only wants to commit if the store has not changed in the meantime, use `updateFrom(key, snapshot, updater)` or `deleteFrom(key, snapshot)`:

```ts [src/workflows/close-conversation.ts]
const snapshot = yield* conversation.get("before-summary");

const summary = yield* step.run("summarize", async () => {
  return await assistant.summarize(snapshot.state.messages);
});

const result = yield* conversation.updateFrom("close", snapshot, (draft) => {
  draft.status = "closed";
  draft.messages.push({ id: "summary", role: "assistant", text: summary });
});

if (!result.updated) {
  // Store changed or was recreated since snapshot; read again with a new key to retry.
}
```

`updateFrom` checks both `instanceId` and `version` against `snapshot`:

| Result | Fields |
| :--- | :--- |
| `{ updated: true }` | `state`, `previousVersion`, and `version` |
| `{ updated: false }` | `expectedInstanceId`, `actualInstanceId`, `expectedVersion`, and `actualVersion` |

`deleteFrom(key, snapshot)` deletes the store only if both `instanceId` and `version` still match `snapshot`:

| Result | Fields |
| :--- | :--- |
| `{ deleted: true }` | Store and its waiters are removed |
| `{ deleted: false }` | `reason: "conflict"`, `expectedInstanceId`, `actualInstanceId`, `expectedVersion`, and `actualVersion` |

Checking `instanceId` ensures that deleting and recreating a store at `version: 0` cannot be confused with an earlier instance that happened to have the same version number. After a deletion, the next store operation creates a fresh instance from `initial(id)` with a new `instanceId` and `version: 0`.

## Validation and `StoreValidationError`

Durable validates the state against `schema` on creation and on every mutation:

- **Schema rejection** – Throws `StoreValidationError`, leaves the store unchanged (or uncreated), and lets workflow `try`/`catch` handle the error.
- **Non-durable value** – Throws `WorkflowValueError` with code `INVALID_STEP_VALUE` and leaves the store unchanged.

Unlike `get`, which allows omitting its step key outside loops, every other store method requires an explicit step key unique within the execution.
