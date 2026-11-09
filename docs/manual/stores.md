# Stores

A store is a piece of state with a name, an ID and a schema. It lives outside any one execution, so several workflows, and code outside workflows, can read and update it. This page and the next two use a store that holds a conversation between a user and an AI assistant.

## Defining a store

`defineStore` defines a kind of store. It takes a name, a schema and the state that a new store starts with:

```ts [conversation.ts]
import { defineStore } from "@notation/durable";
import * as v from "valibot";

const Message = v.object({
  id: v.string(),
  role: v.picklist(["user", "assistant"]),
  text: v.string(),
});

export const Conversation = defineStore(
  "conversation",
  v.object({
    status: v.picklist(["open", "closed"]),
    inbox: v.array(Message),
    messages: v.array(Message),
  }),
  {
    initial: () => ({ status: "open", inbox: [], messages: [] }),
  }
);
```

| Field | Holds |
| --- | --- |
| `status` | Whether the user can still send messages |
| `inbox` | User messages that the assistant has not answered yet |
| `messages` | The conversation so far |

The schema can be any [Standard Schema](https://standardschema.dev), such as one from Zod, Valibot or ArkType. Durable checks the state against the schema every time a store is created or changed.

## Store IDs

One definition describes many stores. Each store has an ID, and the definition's name and the ID together identify the store. Here, each conversation is a store with the conversation's ID.

A workflow gets a handle to one store with `step.store`:

```ts
const conversation = step.store(Conversation, { id: event.params.conversationId });
```

`step.store` is not a step, and it does not take `yield*`. It reads and records nothing, so a workflow can call it as often as it needs to. Two handles with the same name and ID refer to the same store.

Without an `id`, the store's ID is the execution's ID, so each execution gets its own store.

## Creating a store

No operation creates a store on its own. Instead, an operation on a store that does not exist creates it first, from `initial`. A workflow can therefore update a conversation without checking that it exists.

`initial` receives the store's ID, and must return the state synchronously. Durable calls it only when the store does not exist. If two executions update a missing store at the same time, Durable creates the store once, and applies both updates to it.

## Reading

Each operation on a handle is a step, and takes `yield*`. `get` returns a snapshot of the store:

```ts
const snapshot = yield* conversation.get("load-conversation");

snapshot.state;      // the state, as the schema describes it
snapshot.version;    // 0 when the store is created, and 1 more after each change
snapshot.instanceId; // a new ID each time the store is created
```

`select` runs a function on the state, and records only what the function returns:

```ts
const lastMessage = yield* conversation.select("last-message", (state) => {
  return state.messages.at(-1);
});
```

**A read records what it saw.** As with any step, a later delivery returns the record, not the store's current state. A new read step, with a new key, reads the current state.

## Updating

`update` changes the state through a draft. The function changes the draft, and Durable saves the result:

```ts
yield* conversation.update(`reply:${message.id}`, (draft) => {
  draft.messages.push({ id: `${message.id}-reply`, role: "assistant", text: reply });
});
```

The function can also return a whole new state instead of changing the draft. Either way, Durable checks the new state against the schema, saves it and adds 1 to the version.

The update function must be synchronous, and must change nothing except the draft. If another writer changes the store at the same moment, Durable runs the function again on the newer state. The function can therefore run more than once.

A delivery that repeats an update reads the step's record and does not apply the update again. This holds even if the worker crashed after the store changed but before the step was recorded.

## Updating from a snapshot

Some updates are only correct if the store has not changed since the workflow read it. `updateFrom` takes the snapshot, and applies the update only if the store still has that version:

```ts
const snapshot = yield* conversation.get("before-summary");

const summary = yield* step.run("summarise", async () => {
  return await assistant.summarise(snapshot.state.messages);
});

const result = yield* conversation.updateFrom("close", snapshot, (draft) => {
  draft.status = "closed";
  draft.messages.push({ id: "summary", role: "assistant", text: summary });
});

if (!result.updated) {
  // A message arrived while the summary was written. The store is unchanged.
}
```

| `updated` | Meaning | Other fields |
| --- | --- | --- |
| `true` | The update was applied | `state`, `version` and `previousVersion` |
| `false` | The store changed or was created again since the snapshot. The update function did not run | The expected and actual `instanceId` and `version` |

`updateFrom` compares the instance ID as well as the version. A store that was deleted and created again can reach the same version number, but it has a new instance ID.

To try again after `updated: false`, read the store again in a new step and call `updateFrom` with a new key.

## Deleting

`deleteFrom` deletes a store if it still matches a snapshot:

```ts
const snapshot = yield* conversation.get("before-delete");
const result = yield* conversation.deleteFrom("delete", snapshot);

if (!result.deleted) {
  // The store changed, or was created again, since the snapshot.
}
```

The next operation on a deleted store creates it again from `initial`, with a new instance ID and version 0.

## Invalid state

An update that the schema rejects throws `StoreValidationError`, and the store does not change. The workflow can catch the error.

State must also be made of [durable values](./step-values.md). An update that stores a value that is not durable, such as a class instance, throws `WorkflowValueError` with the code `INVALID_STEP_VALUE`, and the store does not change.

## Step keys

`get` accepts a call without a key, as `step.run` does. `select`, `update`, `updateFrom` and `deleteFrom` always take a key. As with any step, each key must be unique within the execution. See [Step keys](./step-keys.md).

## Next

- [Waiting on state](./store-waiting.md) pauses a workflow until a store's state meets a condition.
- [External store access](./store-external.md) reads and writes stores from code outside workflows, such as an HTTP handler.
