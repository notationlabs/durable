# External Store Access

Code outside workflows – such as HTTP route handlers, webhook receivers, and admin scripts – can read and update stores through `runtime.stores`. Every mutation written through `runtime.stores` wakes any waiting workflow whose [`when` or `take`](./waiting-on-state.md) selector watches the changed paths.

```ts [src/api/messages.ts]
import { runtime } from "../durable/runtime";
import { Conversation } from "../stores/conversation";

export async function postMessage(
  conversationId: string,
  message: { id: string; text: string }
) {
  const conversation = runtime.stores.store(Conversation, conversationId);

  await conversation.update((draft) => {
    const seen = [...draft.inbox, ...draft.messages].some((m) => m.id === message.id);
    if (!seen) {
      draft.inbox.push({ id: message.id, role: "user", text: message.text });
    }
  });
}
```

## Accessing a store handle

`runtime.stores.store(definition, id)` returns a `RuntimeStore` handle bound to a store definition and ID. Always obtain the store client from the runtime passed to your server or SDK so store commits and execution wakes share the same atomic write.

Unlike workflow store handles from `step.store`, `RuntimeStore` methods return standard Promises (`await`, not `yield*`), take no step keys, and record no workflow step entries:

| Method | Behavior |
| :--- | :--- |
| `get` | Returns `{ state, version, instanceId }` |
| `update` | Applies the updater atomically and returns `{ state, previousVersion, version }` |
| `updateFrom` | Applies the updater only if `instanceId` and `version` match the snapshot |
| `deleteFrom` | Deletes the store only if `instanceId` and `version` match the snapshot |

Just like inside workflows, reading or updating a missing store ID creates it from the initial factory at version 0 in the same atomic transaction.

## Making external updates idempotent

Because external writes are not tied to a workflow step key, Notation Durable cannot automatically deduplicate repeated external calls. In addition, a rejected `update` promise does not prove the write rolled back – a connection drop or process error can occur after the store commit succeeded.

To make external writes safe to retry, check domain identifiers inside the updater itself as `postMessage` does above (`seen`). Updaters must be synchronous, deterministic, and side-effect-free because concurrent writers may cause `updater` to re-run against a newer snapshot before committing.

## Conditional updates and deletions

Use `updateFrom` when external code reads state, makes a decision, and must abort if a concurrent writer modified the store before the write:

```ts [src/api/close-conversation.ts]
import { runtime } from "../durable/runtime";
import { Conversation } from "../stores/conversation";

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

If another update arrived between your read and `updateFrom`, `result.updated` is false with the expected and actual instance IDs and versions, leaving the store unchanged.

`deleteFrom(snapshot)` applies the same check for deletions, returning `{ deleted: true }` or `{ deleted: false, reason: "conflict", ... }` when the store changed or was recreated since `snapshot`.

## Listing and deleting stores

`runtime.stores` also provides two administrative methods directly on the client:

| Method | Behavior |
| :--- | :--- |
| `listStores` | Returns all existing store IDs for the definition sorted in ascending order |
| `deleteStore` | Removes the store and its waiters unconditionally |

`deleteStore` is the only operation that never invokes `initial(id)`; calling it on a missing store is a no-op. Use `deleteFrom` instead when deletion depends on the store still matching a prior snapshot.

## Schema validation outside workflows

`update` and `updateFrom` – as well as `initial(id)` on first access – validate the resulting state against `definition.schema` and verify that all values are [durable values](./step-values.md) before committing:

- **Schema rejection** – Throws `StoreValidationError` and leaves the store unchanged (or uncreated).
- **Non-durable value** – Throws `WorkflowValueError` with code `INVALID_STEP_VALUE` and leaves the store unchanged.
