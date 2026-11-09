# External store access

Code outside workflows, such as an HTTP handler or a script, can read and write stores too. A write from outside wakes the workflows that are waiting on the state it changed. This is how events from outside reach a waiting workflow.

This page uses the `Conversation` store from [Stores](./stores.md), and an HTTP handler that adds the user's messages to it. The `answerMessages` workflow from [Waiting on state](./store-waiting.md) waits for those messages.

```ts [src/api/messages.ts]
import { runtime } from "../durable/runtime";
import { Conversation } from "../conversation";

export async function postMessage(conversationId: string, message: { id: string; text: string }) {
  const conversation = runtime.stores.store(Conversation, conversationId);

  await conversation.update((draft) => {
    draft.inbox.push({ id: message.id, role: "user", text: message.text });
  });
}
```

`runtime` is the runtime from the [Quick start](./quickstart.md), in `src/durable/runtime.ts`.

## Store client

`runtime.stores` is the runtime's store client. `runtime.stores.store` takes a store definition and an ID, and returns a handle to one store.

**Take the store client from the runtime.** The runtime's store client also writes to the execution records. When a write changes state that a workflow is waiting on, the store client makes that execution due, in the same write. A dispatcher, in this process or another one, then delivers it.

## Handles

A handle from outside a workflow has these methods:

| Method | Does |
| --- | --- |
| `get()` | Returns a snapshot: `state`, `version` and `instanceId` |
| `update(fn)` | Changes the state through a draft |
| `updateFrom(snapshot, fn)` | Changes the state if the store still matches the snapshot |
| `deleteFrom(snapshot)` | Deletes the store if it still matches the snapshot |

They work as the workflow operations of the same name do, with two differences. They are plain async functions, so they take `await` and not `yield*`. And they are not steps, so they take no key and Durable keeps no record of them.

As in a workflow, an operation on a store that does not exist creates it from `initial` first. The handler can therefore add a message before any workflow has used the conversation.

## Repeated requests

Durable does not record external writes, so it cannot tell a repeated request from a new one. If a client sends the same message twice, the handler above adds it twice.

A failed `update` does not prove that nothing was written either. The write can succeed and the promise still reject, for example when the connection drops.

To make a write safe to repeat, check the state in the update function:

```ts
await conversation.update((draft) => {
  const seen = [...draft.inbox, ...draft.messages].some((m) => m.id === message.id);

  if (!seen) {
    draft.inbox.push({ id: message.id, role: "user", text: message.text });
  }
});
```

## Writing from a snapshot

A support tool closes conversations that have no unanswered messages. It reads the conversation, checks the inbox, and then closes it. A message can arrive between the read and the write, so the tool uses `updateFrom`, which applies the change only if the store still has the version it read:

```ts
export async function closeIfAnswered(conversationId: string) {
  const conversation = runtime.stores.store(Conversation, conversationId);
  const snapshot = await conversation.get();

  if (snapshot.state.inbox.length > 0) {
    return false;
  }

  const result = await conversation.updateFrom(snapshot, (draft) => {
    draft.status = "closed";
  });

  return result.updated;
}
```

`result.updated` is `false` if the store changed after the read, for example because a message arrived. See [Updating from a snapshot](./stores.md#updating-from-a-snapshot) for the fields of the result.

## Listing and deleting

The store client has two methods for administrative tasks:

| Method | Does |
| --- | --- |
| `runtime.stores.listStores(Conversation)` | Returns the IDs of every `conversation` store, in ascending order |
| `runtime.stores.deleteStore({ definition: Conversation, id })` | Deletes the store, whatever its state. Does nothing if the store does not exist |

`deleteStore` is the only operation that never creates a store. Use `deleteFrom` when the decision to delete depends on what the store held.

## Invalid state

As in a workflow, `update` and `updateFrom` throw `StoreValidationError` if the schema rejects the new state, and the store does not change.
