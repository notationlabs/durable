# Waiting on state

A workflow can pause until a store's state meets a condition. `when` waits and returns a value from the state. `take` waits and also changes the state, so that only one workflow receives each value.

This page uses the `Conversation` store from [Stores](./stores.md), and a workflow that answers each message the user sends. The workflow closes the conversation after 30 minutes without a message.

```ts
import { workflow } from "@notation/durable";
import { Conversation } from "./conversation";

export const answerMessages = workflow<{ conversationId: string }>(async function* (step, event) {
  const conversation = step.store(Conversation, { id: event.params.conversationId });

  for (let turn = 0; ; turn++) {
    const waited = yield* conversation.when(
      `wait:${turn}`,
      (state) => state.inbox.length > 0,
      { deadline: Date.now() + 30 * MINUTE }
    );

    if (waited.status === "timed-out") {
      yield* conversation.update("close", (draft) => {
        draft.status = "closed";
      });
      return;
    }

    const message = yield* conversation.take(
      `take:${turn}`,
      (state) => state.inbox[0],
      (draft) => {
        const message = draft.inbox.shift()!;
        draft.messages.push(message);
      }
    );

    const reply = yield* step.run(`reply:${turn}`, async () => {
      return await model.reply(message.text);
    });

    yield* conversation.update(`save-reply:${turn}`, (draft) => {
      draft.messages.push({ id: `${message.id}-reply`, role: "assistant", text: reply });
    });
  }
});
```

## `when`

`when` runs a function, the selector, on the store's state. If the selector's result is a match, `when` returns it. Otherwise the workflow waits.

```ts
const firstMessage = yield* conversation.when("first-message", (state) => state.inbox[0]);
```

| The selector returns | `when` |
| --- | --- |
| `undefined`, `null` or `false` | Waits |
| Any other value, including `0` and `""` | Returns the value |

`when` records the value it returns, as other steps do. It does not change the store.

## How a waiting workflow wakes

When the selector does not match, Durable ends the delivery, and the execution's status becomes `suspended`. No process runs while the workflow waits.

While the selector runs, Durable notes which parts of the state it reads. A later change to any of those parts wakes the workflow:

| Selector | Change | Wakes the workflow |
| --- | --- | --- |
| `(state) => state.inbox.length > 0` | A message is added to `inbox` | Yes |
| `(state) => state.inbox.length > 0` | `status` or `messages` changes | No |
| `(state) => state.status === "closed"` | `status` changes | Yes |

The woken workflow runs the selector again on the new state. If the selector still does not match, the workflow waits again.

The change can come from another workflow or from code outside workflows, such as an HTTP handler. See [External store access](./store-external.md).

Durable saves the change and the wake in one write, so a crash cannot keep one and lose the other. Several changes that arrive before the workflow runs lead to one delivery.

## Selectors

A selector must be synchronous, and must not change the state. A selector that assigns to the state throws `Store selectors must not mutate state`.

The selector's result must be a [durable value](./step-values.md), because `when` records it.

If the selector throws, `when` records the error and throws it into the workflow, as `step.run` does.

## `take`

Two workflows that wait with `when` for the same message both receive it. `take` makes sure that only one does. It takes a selector, as `when` does, and a claim function that changes the state:

```ts
const message = yield* conversation.take(
  `take:${turn}`,
  (state) => state.inbox[0],
  (draft) => {
    const message = draft.inbox.shift()!;
    draft.messages.push(message);
  }
);
```

When the selector matches, Durable runs the claim function on a draft and saves the result, in one write. Here the claim moves the message from `inbox` to `messages`, so the next selector that reads `inbox[0]` sees a different message.

**If two workflows take at the same moment, only one write succeeds.** The other workflow runs its selector again on the new state, and receives the next message or waits.

The claim function receives the draft and the selected value. It follows the same rules as an [update function](./stores.md#updating), and it always takes a key.

## Deadlines

`when` takes a `deadline`, as a time in milliseconds since the Unix epoch. With a deadline, `when` returns an object that says whether the selector matched in time:

```ts
const waited = yield* conversation.when(
  `wait:${turn}`,
  (state) => state.inbox.length > 0,
  { deadline: Date.now() + 30 * MINUTE }
);
```

| `status` | Meaning |
| --- | --- |
| `matched` | The selector matched. `value` holds its result |
| `timed-out` | The deadline passed first |

`when` with a deadline follows these rules:

- **The first delivery that waits records the deadline.** Later deliveries use the recorded deadline, so `Date.now() + 30 * MINUTE` does not move forward on each delivery.
- **The selector runs first.** A selector that matches returns `matched`, even if the deadline has passed.
- **The first result is final.** Once `when` records `timed-out`, a later message does not change the result.

A `when` with a deadline always takes a key. `take` has no deadline, so the example waits with `when` first and then takes the message.

## Step keys

`when` without a deadline accepts a call without a key. Inside a loop, such as the one in the example, every step needs a key that changes on each pass. See [Step keys](./step-keys.md).
