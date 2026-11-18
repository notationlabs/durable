# Waiting on State

Workflows can pause until a store's state meets a condition using `when`, wait with a timeout using `{ deadline }`, or atomically claim items from shared queues and mailboxes using `take`.

```ts [src/workflows/answer-messages.ts]
import { workflow } from "@notation/durable";
import { Conversation } from "../stores/conversation";

const THIRTY_MINUTES = 30 * 60 * 1000;

export const answerMessages = workflow<{ conversationId: string }>(
  async function* (step, event) {
    const conversation = step.store(Conversation, {
      id: event.params.conversationId,
    });

    for (let turn = 0; ; turn++) {
      const deadline = yield* step.run(`deadline:${turn}`, () => Date.now() + THIRTY_MINUTES);

      const waited = yield* conversation.when(
        `wait:${turn}`,
        (state) => state.inbox.length > 0,
        { deadline }
      );

      if (waited.status === "timed-out") {
        yield* conversation.update("close", (draft) => {
          draft.status = "closed";
        });
        return "closed";
      }

      const message = yield* conversation.take(
        `take:${turn}`,
        (state) => state.inbox[0],
        (draft) => {
          const next = draft.inbox.shift()!;
          draft.messages.push(next);
        }
      );

      const reply = yield* step.run(`reply:${turn}`, async () => {
        return await model.reply(message.text);
      });

      yield* conversation.update(`save-reply:${turn}`, (draft) => {
        draft.messages.push({
          id: `${message.id}-reply`,
          role: "assistant",
          text: reply,
        });
      });
    }
  }
);
```

## Waiting with `when([key,] selector)`

`when` evaluates a synchronous `selector(state)` function against the store:

```ts [src/workflows/wait-for-open.ts]
const firstMessage = yield* conversation.when((state) => state.inbox[0]);
const namedWait = yield* conversation.when("first-message", (state) => state.inbox[0]);
```

| Selector return | Behavior |
| :--- | :--- |
| Non-matching value | Suspends the execution until a matching store change wakes it |
| Matching durable value | Records and returns the value immediately without modifying the store |

Returning `undefined`, `null`, or `false` keeps the step waiting, while any other value – including empty strings or zero – resolves `when` immediately.

Untimed `when` accepts an optional key as its first argument; inside loops, pass a unique key per iteration such as `` `wait:${turn}` ``. Selectors must be synchronous and read-only, and any thrown error is recorded as the step outcome for replay.

## Path tracking and wake triggers

When `selector` does not match, Notation Durable ends the delivery with status `suspended` and records the property paths the selector read while evaluating. A later mutation (from another workflow or [external store access](./external-store-access.md)) wakes the execution in the same atomic write only when its changed paths intersect those read paths:

| Selector expression | Wake trigger |
| :--- | :--- |
| `state.inbox.length > 0` | Wakes on changes to `state.inbox` and ignores `state.status` |
| `state.messages[0]?.role` | Wakes on changes to `state.messages[0]` or `state.messages` |

Mutations inside `Date`, `Map`, or `Set` values also wake waiters watching that value's path. Multiple intersecting updates arriving before the workflow runs collapse into a single wake. When woken, the execution replays and re-evaluates `selector` against the current state; if it still does not match, it registers a fresh waiter and suspends again.

## Timed waits with `when(key, selector, { deadline })`

Pass `{ deadline }` (an absolute epoch timestamp in milliseconds) to bound how long `when` waits. Timed `when` requires an explicit step key and returns a `TimedWhenResult<R>`:

```ts [src/workflows/wait-approval.ts]
import { workflow, type TimedWhenResult } from "@notation/durable";
import { Approval } from "../stores/approval";

export const awaitApproval = workflow<{ orderId: string; deadline: number }>(
  async function* (step, event) {
    const approval = step.store(Approval, { id: event.params.orderId });

    const outcome: TimedWhenResult<"approved" | "rejected"> = yield* approval.when(
      "decision",
      (state) => state.decision,
      { deadline: event.params.deadline }
    );

    return outcome;
  }
);
```

| Result | Meaning |
| :--- | :--- |
| `{ status: "matched", value }` | Selector returned a matching value |
| `{ status: "timed-out" }` | `deadline` passed before the selector matched |

Timed `when` follows three ordering rules:

- **Recorded deadline** – The first evaluation that suspends records `deadline` with the unfinished step. Subsequent deliveries compare against that recorded deadline, so recomputing `Date.now() + offset` on replay does not push the timeout forward.
- **Selector runs first** – Each evaluation runs `selector` before checking `deadline`. If the store already satisfies the selector when the execution wakes at or after `deadline`, the step records `{ status: "matched", value }`.
- **First result wins** – Once `{ status: "timed-out" }` is recorded, later store changes cannot turn it into `matched`.

In tests using a custom runtime clock, step delays, retries, and `deadline` checks advance with the mock clock rather than `Date.now()`. Compute deadlines from workflow parameters or fake system time in your test runner so clocks stay aligned.

## Atomic queues and mailboxes with `take`

When two workflows use `when` to wait on the same inbox item, both wake and receive the same item. Use `take(key, selector, claim)` to select and claim an item in one atomic compare-and-swap commit so each item goes to at most one consumer:

```ts [src/workflows/claim-job.ts]
const job = yield* queue.take(
  "claim-next",
  (state) => state.jobs.find((item) => !item.claimedBy),
  (draft, selected) => {
    selected.claimedBy = event.executionId;
  }
);
```

`take` requires an explicit step key, a synchronous `selector`, and a synchronous `claim(draft, selected)` callback:

1. When the selector returns `undefined`, `null`, or `false`, `take` commits nothing, registers a path-tracked waiter, and suspends like `when`.
2. When it matches, the claim callback runs against the draft, validates the updated state against the schema, and commits atomically.
3. If two executions race on the same item, only one commit succeeds; the other re-runs the selector on the updated state and either claims the next available item or suspends.

`take` returns the matched value reflecting any claim-time edits. Returning a Promise or failing validation leaves the store unchanged and throws `StoreValidationError` or a synchronous error, while returning a non-durable selection throws `WorkflowValueError`.

Once `take` commits, its result is recorded under `stepKey` so replays return the claimed item without running `selector` or `claim` again.
