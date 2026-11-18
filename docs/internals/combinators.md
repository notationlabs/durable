# Concurrent Combinators

`step.all` and `step.race` drive multiple branch generators concurrently inside a single delivery without duplicating branch payloads in the combinator's step record.

## Spawning branches and concurrency limits

Both `step.all` and `step.race` yield their key and cache check, then pass their branch generators and options into the combinator runner:

1. `BranchesStepKey` – Identifies the combinator step without acting as a cancellation point.
2. `StepCacheCheck` – Checks the heap for an already settled `StepBranchesDecision`.
3. `StepBranches` – Hands the branches, mode, and concurrency cap to the combinator driver.

Both combinators default to unbounded concurrency unless `step.all` sets a positive `concurrency` option. Each combinator initializes a `Limiter` and drives every child branch concurrently.

Before executing an uncached non-combinator step, a branch acquires a concurrency slot from every enclosing combinator's `Limiter` and releases it when the step settles. Cached steps and nested combinators skip slot acquisition.

## `DecisionLock` and waiter isolation

Every `Combinator` guards decision settlement with a reader-writer `DecisionLock` (`packages/sdk/src/internal/branches.ts`) so losing branches cannot register trailing waiters after the decision write deletes them:

```mermaid [DecisionLock coordination across branch calls and decision writes]
sequenceDiagram
  participant B as Branch Driver
  participant L as DecisionLock
  participant H as HeapClient / Stores
  B->>L: shared across ancestor combinators
  alt branch.stopped is true
    B-->>L: returns STOPPED without client call
  else branch still active
    B->>H: registerWaiter / effectCall / runtimeCall
  end
  Note over L,H: Winning or failing branch settles combinator
  B->>L: set combinator.decision synchronously
  B->>L: exclusive writeStep drains active shared calls
  L->>H: writeStep(StepBranchesDecision, deleteWaitersOf: held)
```

Store waits, external effects, and runtime calls inside a branch acquire `DecisionLock` in shared mode across all enclosing combinators from outermost to innermost, checking whether the branch has stopped before calling any client.

When a combinator decides, settlement sets the decision synchronously first so losing branches stop immediately, then takes the lock exclusively to drain active shared calls before writing `StepBranchesDecision` and removing held waiters.

## Replay bounding with `StepBranchesDecision`

`StepBranchesDecision` records only the `BranchesDecision` outcome rather than duplicating child payloads; replay re-drives each branch through its own heap records to reconstruct the result tuple or error:

| Decision | Condition |
| :--- | :--- |
| `"fulfilled"` | Every branch in fail-fast `step.all` returned |
| `"rejected"` | First branch at `index` in fail-fast `step.all` threw |
| `"settled"` | Every branch in settle-mode `step.all` returned or threw |
| `"won"` | First branch at `index` in `step.race` returned |
| `"failed"` | Every branch in `step.race` threw `WorkflowRaceError` |
| `"cancelled"` | Cancellation landed across branches inside the combinator |

Under `"rejected"` and `"won"`, `recordDecision` captures two lists from non-deciding branches:

- **`keys`** – Every step key reached by non-deciding branches and their nested branches before the decision.
- **`held`** – The subset of `keys` where a branch or nested combinator was actively waiting (`branch.at`) when the decision landed.

On replay, a limited branch stops if it enters a step key outside the recorded boundary or reaches a non-combinator step in `held`. Combinator keys bypass the hold check so nested combinators still replay down to their own held leaves.

Any non-combinator step a limited branch already passed must hold a completed record rather than an active delay, or replay throws a divergence error.

## Branch abandonment and wake propagation

JavaScript promises cannot cancel mid-callback when a combinator settles. Once a decision marks losing branches stopped, an in-flight `step.run` callback finishes and saves its record before the branch exits without advancing further; the combinator resolves only after every branch leaves active execution.

Combinators wait to decide until all branches finish replaying and no cancellation barrier wait is pending. In `step.race`, a branch that completes without first waiting out an elapsed delay beats a branch whose delay elapsed during the same delivery.

When all branches suspend or stop without a decision, the combinator collects suspended wake times and yields a `StepDelay` for the earliest wake, or forwards the first suspension if no wake time is set.
