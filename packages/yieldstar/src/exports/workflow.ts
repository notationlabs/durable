import type { Logger } from "pino";
import type {
  Durable,
  WorkflowEvent,
  WorkflowGenerator,
  EventParams,
  EventContext,
  IfDurable,
} from "@yieldstar/core";
import type { StepRunner } from "../internal/step-runner";
import { isIterable } from "../internal/utils";
import {
  StepCancelled,
  StepUnfinished,
  UnfinishedCacheCheck,
  decodeStepRecord,
  encodeStepRecord,
} from "../internal/step-record";
import { createStepRunner } from "../internal/step-runner";
import {
  StepRuntimeCall,
  StepStoreWaitRequest,
  executeRuntimeCall,
  type RuntimeClients,
  infrastructure,
} from "../internal/runtime-call";
import { StepEffectCall, executeEffectCall } from "../internal/effect-call";
import {
  BranchesStepKey,
  CancellationBarrier,
  Combinator,
  DrivenBranch,
  STOPPED,
  StepBranches,
  StepBranchesDecision,
  type BranchesDecision,
} from "../internal/branches";
import {
  StepResponse,
  StepKey,
  StepError,
  StepResult,
  StepDelay,
  StepCacheCheck,
  StepEffectWait,
  StepInvalid,
  StepJoinWait,
  StepStoreWait,
  WorkflowResult,
  InfrastructureError,
  LeaseLostError,
  StepConflictError,
  WorkflowCancelledError,
  WorkflowRaceError,
  isLeaseLost,
  WORKFLOW_RESULT_STEP_KEY,
  type ExecutionCancellation,
  type HeapRecord,
} from "@yieldstar/core";

export type WorkflowFn<
  Params extends EventParams,
  Result,
  Context extends EventContext
> = (
  step: StepRunner,
  event: WorkflowEvent<Params, Context>,
  logger: Logger
) => AsyncGenerator<any, Result>;

/**
 * A workflow function whose params and result are durable values. Params are
 * an input, so a function type cannot check them; a missing property named
 * for the problem does instead.
 */
export type DurableWorkflowFn<Params, Result> = IfDurable<
  Params,
  { "Workflow params must be durable values": Durable<Params> }
> &
  IfDurable<
    Result,
    (step: any, event: any, logger: any) => AsyncGenerator<any, Durable<Result>>
  >;

/**
 * Wraps a workflow function in a generator that advances its steps and yields
 * control to the executor whenever a step needs async work.
 */
export function workflow<
  Params extends EventParams,
  Result,
  Context extends EventContext
>(
  workflowFn: WorkflowFn<Params, Result, Context> &
    DurableWorkflowFn<Params, Result>
): WorkflowGenerator<Params, Result, Context> {
  return async function* workflowGenerator({
    event,
    lease,
    signal,
    cancellation,
    heapClient,
    storeClient,
    executionClient,
    effectClient,
    joinClient,
    clock,
    logger,
  }) {
    const stepRunner = createStepRunner({ event, clock });
    const workflowIterator = workflowFn(stepRunner, event, logger);
    const { executionId } = event;
    const clients: RuntimeClients = {
      stores: storeClient,
      executions: executionClient,
      joins: joinClient,
    };
    // Shared by every branch, so a key repeated across branches is rejected.
    const stepKeys = new Set<string>();

    /** Writes one attempt of a step. A lost lease or a conflicting record ends the delivery. */
    async function writeStep(params: {
      stepKey: string;
      stepAttempt: number;
      done: boolean;
      stepResponseJson: string;
      deleteWaitersOf?: readonly string[];
    }) {
      stopIfAborted(signal);
      const written = await infrastructure(() =>
        heapClient.writeStep({ lease, ...params })
      );
      if (isLeaseLost(written)) {
        throw new InfrastructureError(new LeaseLostError(executionId));
      }
      // The existing record stands. Ending the delivery as a retry makes the
      // next delivery replay it.
      if (
        written.stepResponseJson !== params.stepResponseJson ||
        written.meta.done !== params.done
      ) {
        throw new InfrastructureError(
          new StepConflictError(executionId, params.stepKey)
        );
      }
    }

    /**
     * Records the cancellation at every parked step, and at each unsettled
     * combinator above one, in one write. Returns the error to throw into
     * workflow code at every parked step. A lost lease or a conflicting record ends
     * the delivery.
     */
    async function deliverCancellation(
      request: ExecutionCancellation,
      parked: readonly { branch: DrivenBranch; stepKey: string; stepAttempt: number }[]
    ) {
      const err = new WorkflowCancelledError({ reason: request.reason, source: request.source });
      const thrown = encodeStepRecord(new StepCancelled(err), parked[0]!.stepKey).json;
      const combinators: Combinator[] = [];
      for (const { branch } of parked) {
        for (let combinator = branch.parent; combinator; combinator = combinator.owner.parent) {
          if (!combinator.decision && !combinators.includes(combinator)) {
            combinators.push(combinator);
          }
        }
      }
      const cancelled = encodeStepRecord(
        new StepBranchesDecision({ decision: "cancelled", err }),
        ""
      );
      const steps = [
        ...parked.map(({ stepKey, stepAttempt }) => ({
          stepKey,
          stepAttempt,
          stepResponseJson: thrown,
        })),
        ...combinators.map(({ stepKey, stepAttempt }) => ({
          stepKey,
          stepAttempt,
          stepResponseJson: cancelled.json,
        })),
      ];
      // Set before the write, so no combinator settles another way meanwhile.
      // A failed write ends the delivery.
      for (const combinator of combinators) {
        combinator.decision = (cancelled.response as StepBranchesDecision).decision;
      }
      stopIfAborted(signal);
      const written = await infrastructure(() =>
        heapClient.recordCancellation({ lease, steps })
      );
      if (isLeaseLost(written)) {
        throw new InfrastructureError(new LeaseLostError(executionId));
      }
      steps.forEach((step, i) => {
        const stored = written[i];
        if (stored?.stepResponseJson !== step.stepResponseJson || stored.meta.done !== true) {
          throw new InfrastructureError(new StepConflictError(executionId, step.stepKey));
        }
      });
      return (decodeStepRecord(thrown) as StepCancelled).err;
    }

    // A request is delivered once. Replay of a delivered request reads the
    // recorded throw instead of the request.
    const barrier = new CancellationBarrier(
      cancellation && cancellation.deliveredAt === undefined ? cancellation : undefined,
      deliverCancellation
    );

    /**
     * Advances one generator, the workflow's or a branch's, until it returns,
     * suspends or stops. An error that workflow code throws rejects
     * with a `WorkflowThrew`. Any other rejection ends the delivery.
     */
    async function drive(
      iterator: AsyncGenerator<any, any, any>,
      branch: DrivenBranch
    ): Promise<DriveOutcome> {
      const next = async (value?: unknown) => {
        stopIfAborted(signal);
        try {
          return await iterator.next(value);
        } catch (err) {
          throw new WorkflowThrew(err);
        }
      };
      const throwInto = async (err: unknown) => {
        stopIfAborted(signal);
        try {
          return await iterator.throw(err);
        } catch (thrown) {
          throw new WorkflowThrew(thrown);
        }
      };

      let iteratorResult: IteratorResult<any>;
      let nextIteratorResult: IteratorResult<any> | null = null;
      // A step that produced a result resumes with its decoded record, which
      // is what it returns to workflow code.
      let resumeWith: StepResult | undefined;
      let afterDelay = false;

      while (true) {
        let stepAttempt = 0;

        // Throwing a step error into the workflow already advanced it.
        if (nextIteratorResult) {
          iteratorResult = nextIteratorResult;
          nextIteratorResult = null;
        } else {
          branch.at = undefined;
          iteratorResult = await next(resumeWith);
        }
        resumeWith = undefined;

        if (iteratorResult.done) {
          return { status: "returned", value: iteratorResult.value, afterDelay };
        }
        if (!(iteratorResult.value instanceof StepKey)) {
          throw new Error("Step runners must yield a StepKey object");
        }
        // A stopped branch reaches no key after its combinator decided, so
        // the keys recorded with the decision are all the keys it reached.
        if (branch.stopped) return { status: "stopped" };
        const stepKey = iteratorResult.value.key;
        const isCombinator = iteratorResult.value instanceof BranchesStepKey;
        // On replay, a branch that a recorded decision stopped stops where
        // the first run stopped it: at a key it did not reach, or after
        // reaching a key it was waiting at.
        if (!branch.allows(stepKey)) return { status: "stopped" };
        if (stepKeys.has(stepKey)) {
          throw new Error("Each step in a loop must have a unique cache key.");
        }
        stepKeys.add(stepKey);
        branch.reach(stepKey);
        if (!isCombinator && branch.holds(stepKey)) return { status: "stopped" };
        // The first run resumed the branch past this step before the decision.
        const passed = !isCombinator && branch.limited;

        let cached = await infrastructure(() =>
          heapClient.readStep({
            executionId,
            stepKey,
          })
        );

        if (cached?.meta) {
          stepAttempt = cached.meta.attempt + 1;
        }

        const recorded = cached ? decodeStepRecord(cached.stepResponseJson) : null;
        // A recorded step after a delay means the delay was passed in an
        // earlier delivery.
        if (cached) afterDelay = false;
        if (passed && !cached?.meta.done) {
          throw new Error(`Step "${stepKey}" no longer reaches its recorded outcome on replay`);
        }

        let thrown: { err: unknown } | undefined;
        if (cached?.meta.done && recorded instanceof StepCancelled) {
          thrown = { err: recorded.err };
        } else if (!isCombinator && !passed && isUnfinishedStep(cached, recorded, clock())) {
          // A combinator is not a cancellation point. A store or join wait
          // checks its condition before it counts as caught up, so a wait
          // satisfied before this delivery wins against a delay that elapsed.
          const checksFirst =
            !barrier.pending && (recorded instanceof StepStoreWait || recorded instanceof StepJoinWait);
          if (!checksFirst) branch.markLive();
          if (branch.stopped) return { status: "stopped" };
          // A branch that a recorded decision reads from passed this step on
          // the first run, and reaches it again only when the clock reads
          // earlier. It suspends there, and is not cancelled there.
          if (barrier.pending && !(cached?.meta.done && branch.finishedBefore)) {
            // A delay is recorded done when it starts, so its cancellation
            // is written as the step's next attempt, which replay reads.
            const err = await barrier.park(branch, stepKey, stepAttempt);
            if (err === STOPPED) return { status: "stopped" };
            thrown = { err };
          }
        }
        // A stopped branch still starts the branches of a combinator it
        // reached, so that replay, which starts them, runs the same code.
        if (branch.stopped && !isCombinator) return { status: "stopped" };
        if (thrown) {
          // The step is suspended at its StepKey, so the throw reaches
          // workflow code without running the step.
          branch.at = undefined;
          afterDelay = false;
          nextIteratorResult = await throwInto(thrown.err);
          continue;
        }

        // A step with an unfinished record runs again. A step that yields an
        // UnfinishedCacheCheck receives the record.
        let unfinished: typeof cached = null;
        if (cached?.meta.done === false) {
          unfinished = cached;
          cached = null;
        }

        iteratorResult = await next();
        const cacheCheck = iteratorResult.value;
        if (iteratorResult.done || !(cacheCheck instanceof StepCacheCheck)) {
          throw new Error("Step runners must yield a CacheCheck object");
        }

        const live = !cached && !isCombinator;
        if (live) {
          await branch.acquireSlot();
          if (branch.stopped) {
            branch.releaseSlot();
            return { status: "stopped" };
          }
        }
        let action: StepAction;
        try {
          action = await settleStep({
            branch,
            next,
            stepKey,
            stepAttempt,
            cached,
            recorded,
            passed,
            cachedResponse: cached
              ? recorded
              : unfinished && cacheCheck instanceof UnfinishedCacheCheck
                ? new StepUnfinished(recorded!)
                : null,
          });
        } finally {
          if (live) branch.releaseSlot();
        }

        // A stopped branch's step that was in flight is recorded, but the
        // branch's generator is not resumed.
        if (branch.stopped || branch.holds(stepKey)) return { status: "stopped" };
        if ("throw" in action) {
          branch.at = undefined;
          afterDelay = false;
          nextIteratorResult = await throwInto(action.throw);
          continue;
        }
        if ("resume" in action) {
          resumeWith = action.resume;
          if (action.delay) afterDelay = true;
          continue;
        }
        return action;
      }
    }

    /**
     * Runs one step past its cache check: makes its runtime calls, records
     * its response, and says how its generator continues.
     */
    async function settleStep(params: {
      branch: DrivenBranch;
      next: (value?: unknown) => Promise<IteratorResult<any>>;
      stepKey: string;
      stepAttempt: number;
      cached: HeapRecord | null;
      recorded: StepResponse | null;
      /** The first run resumed the branch past this step, so a delay resumes whatever the clock reads. */
      passed: boolean;
      cachedResponse: StepResponse | null;
    }): Promise<StepAction> {
      const { branch, next, stepKey, cached, recorded } = params;
      let { stepAttempt } = params;
      let suspendedOnStore = false;

      // Resuming the step runs the step function unless it is cached.
      let iteratorResult = await next(params.cachedResponse);
      let stepResponse = iteratorResult.value;

      // A step that needs a runtime client yields a request, and the call is
      // made here, outside workflow code. An infrastructure error rejects the
      // delivery without resuming the step. A stopped branch makes no new
      // call, and a call in flight finishes before a combinator records its
      // decision (see DecisionLock).
      while (
        stepResponse instanceof StepRuntimeCall ||
        stepResponse instanceof StepEffectCall ||
        stepResponse instanceof StepStoreWaitRequest
      ) {
        stopIfAborted(signal);
        if (branch.stopped) return { status: "stopped" };
        if (stepResponse instanceof StepStoreWaitRequest) {
          const { waiter } = stepResponse;
          // 1. Record the unfinished step. A timed wait's deadline must be
          //    durable before the waiter can wake a redelivery.
          const record = encodeStepRecord(stepResponse, stepKey);
          await writeStep({
            stepKey,
            stepAttempt,
            done: false,
            stepResponseJson: record.json,
          });
          // 2. Register the waiter.
          const registered = await branch.shared(async () =>
            branch.stopped
              ? STOPPED
              : executeRuntimeCall(
                  new StepRuntimeCall(({ stores }) => stores.registerWaiter(waiter)),
                  clients
                )
          );
          if (registered === STOPPED) return { status: "stopped" };
          // 3. Suspend.
          if (registered.ok) {
            stepResponse = record.response;
            suspendedOnStore = true;
            break;
          }
          // The failure is recorded as the next attempt.
          stepAttempt++;
          iteratorResult = await next(registered);
          stepResponse = iteratorResult.value;
          continue;
        }
        const call = stepResponse;
        const callResult = await branch.shared(async () => {
          if (branch.stopped) return STOPPED;
          return call instanceof StepEffectCall
            ? executeEffectCall(call.request, {
                effects: effectClient,
                executionId,
                // Each earlier attempt of an effect step that ended
                // uncertain wrote one unfinished record.
                attempt: stepAttempt,
                clock,
                assertLease: () => stopIfAborted(signal),
                logger,
              })
            : executeRuntimeCall(call, clients);
        });
        if (callResult === STOPPED) return { status: "stopped" };
        iteratorResult = await next(callResult);
        stepResponse = iteratorResult.value;
      }

      if (stepResponse instanceof StepBranches) {
        if (cached && !(recorded instanceof StepBranchesDecision)) {
          throw new Error(`Step "${stepKey}" has a record that is not a combinator decision`);
        }
        return runBranches(
          stepResponse,
          branch,
          stepKey,
          stepAttempt,
          cached ? (recorded as StepBranchesDecision) : undefined
        );
      }

      // TODO: the iterable check should come before the StepKey check.
      // TODO: not throwing here causes a critical error; should the workflow break?
      if (isIterable(stepResponse)) {
        console.log("[ERR] Steps must be yielded using yield*\n");
        stepResponse = new StepInvalid();
      } else if (!(stepResponse instanceof StepResponse)) {
        console.log(
          `[ERR] Iterators should yield a StepResponse. Got ${JSON.stringify(
            stepResponse
          )}`
        );
        stepResponse = new StepInvalid();
      }

      let stepResponseJson: string | undefined;
      if (!cached) {
        const record = encodeStepRecord(stepResponse, stepKey);
        stepResponseJson = record.json;
        stepResponse = record.response;
      }

      const needsRetry =
        !cached?.meta.done &&
        stepResponse instanceof StepError &&
        // 1-indexed vs 0-indexed
        stepResponse.maxAttempts > stepAttempt + 1;
      // A watched effect stays unfinished, so the next delivery begins it
      // again; its waiter is deleted when the step's result is recorded.
      const needsWake =
        !cached?.meta.done &&
        (stepResponse instanceof StepStoreWait ||
          stepResponse instanceof StepJoinWait ||
          stepResponse instanceof StepEffectWait);

      if (!cached && stepResponseJson !== undefined && !suspendedOnStore) {
        await writeStep({
          stepKey,
          stepAttempt,
          done: !needsRetry && !needsWake,
          stepResponseJson,
        });
      }

      if (stepResponse instanceof StepInvalid) {
        throw new Error(
          "Workflow yielded an invalid step. Steps must be yielded with yield* from a step runner."
        );
      }
      if (stepResponse instanceof StepError) {
        if (needsRetry) {
          return {
            status: "suspended",
            response: new StepDelay(clock() + stepResponse.retryInterval),
          };
        }
        return { throw: stepResponse.err };
      }
      if (stepResponse instanceof StepDelay && (params.passed || stepResponse.wakeAt <= clock())) {
        return { resume: undefined, delay: true };
      }
      if (stepResponse instanceof StepResult) {
        return { resume: stepResponse };
      }
      return { status: "suspended", response: stepResponse };
    }

    /**
     * Drives the branches of `step.all` or `step.race` concurrently, records
     * the decision under `stepKey` once the combinator settles, and resolves
     * once no branch is running. A `recorded` decision is applied to the
     * branches' replay instead; under `rejected` or `won`, the other branches
     * stop where the record says. Rejects with an error that ends the
     * delivery.
     */
    function runBranches(
      request: StepBranches,
      owner: DrivenBranch,
      stepKey: string,
      stepAttempt: number,
      recorded: StepBranchesDecision | undefined
    ): Promise<StepAction> {
      const { combinator: kind, mode, concurrency, branches } = request.request;
      const combinator = new Combinator(owner, stepKey, stepAttempt, concurrency);
      combinator.decision = recorded?.decision;
      combinator.recorded = recorded !== undefined;
      owner.inner = combinator;
      const children = branches.map((_, i) => new DrivenBranch(combinator, i));
      combinator.branches.push(...children);
      combinator.keys.push(...branches.map(() => []));
      if (recorded) {
        const { decision } = recorded;
        if (
          (decision.decision === "rejected" || decision.decision === "won") &&
          decision.index >= branches.length
        ) {
          throw new Error(`Step "${stepKey}" no longer reaches its recorded outcome on replay`);
        }
        combinator.replayKeys = new Set(recorded.keys);
        combinator.replayHeld = new Set(recorded.held);
      }
      const states: BranchState[] = children.map(() => ({ status: "running" }));
      // Indexes of branches that returned or threw, in the order they did.
      const finished: number[] = [];
      let writing = false;
      let done = false;

      return new Promise<StepAction>((resolve, reject) => {
        const settle = (fn: () => StepAction) => {
          done = true;
          barrier.enter();
          try {
            resolve(fn());
          } catch (err) {
            reject(err);
          }
        };

        const evaluate = () => {
          if (done || writing) return;
          const running = states.some((state) => state.status === "running");
          const fatal = combinator.fatal;
          if (fatal) {
            if (!running) {
              settle(() => {
                throw fatal.err;
              });
            }
            return;
          }
          // The combinator decides only once every branch has caught up with
          // its records, so the decision deletes the waiters of every step a
          // branch will reach. While a cancellation is pending, it waits for
          // every branch: the delivery may land in one still running.
          if (
            !combinator.decision &&
            !owner.stopped &&
            !owner.limited &&
            !combinator.replaying &&
            !(barrier.pending && running)
          ) {
            const decision = decide(kind, mode, states, finished);
            if (decision) {
              recordDecision(decision);
              return;
            }
          }
          if (running) return;
          const { decision } = combinator;
          // A branch the decision reads from suspends on replay when the
          // clock reads earlier than on the first run. The combinator
          // suspends with it, as a single delay would.
          if (decision && readsFrom(decision, states).some((state) => state.status === "suspended")) {
            settle(() => ({ status: "suspended", response: earliestWake(states) }));
            return;
          }
          if (
            decision &&
            (decision.decision !== "cancelled" ||
              states.every((state) => state.status === "returned" || state.status === "threw"))
          ) {
            settle(() => applyDecision(stepKey, decision, states));
            return;
          }
          if (owner.stopped || owner.limited) {
            settle(() => ({ status: "stopped" }));
            return;
          }
          settle(() => ({ status: "suspended", response: earliestWake(states) }));
        };

        const recordDecision = (decision: BranchesDecision) => {
          // Set first, so no branch starts new work from here on.
          combinator.decision = decision;
          writing = true;
          barrier.enter();
          // Only these decisions stop branches. Under the others every branch
          // has finished, so its waiters are already deleted. A key that is
          // not held has a done record, which deleted its waiters, so only
          // held keys can still have waiters.
          const stops = decision.decision === "rejected" || decision.decision === "won";
          const keys = stops ? combinator.keysOutside(decision.index) : [];
          const held = stops ? combinator.heldOutside(decision.index) : [];
          combinator.lock
            .exclusive(() =>
              writeStep({
                stepKey,
                stepAttempt,
                done: true,
                stepResponseJson: encodeStepRecord(
                  new StepBranchesDecision(decision, keys, held),
                  stepKey
                ).json,
                ...(held.length > 0 ? { deleteWaitersOf: held } : {}),
              })
            )
            .catch((err) => {
              combinator.fatal ??= { err };
              barrier.releaseStopped();
            })
            .finally(() => {
              writing = false;
              evaluate();
              barrier.leave();
            });
        };

        combinator.evaluate = evaluate;
        barrier.enter(children.length);
        children.forEach((child) => {
          const i = child.index;
          // The state is set in the same task as markDone, so no evaluation
          // sees a branch that has an outcome but is still replaying.
          const finish = (state: BranchState) => {
            states[i] = state;
            if (state.status === "returned" || state.status === "threw") finished.push(i);
            child.markDone();
            barrier.leave();
          };
          drive(branches[i]!, child).then(finish, (err) => {
            if (err instanceof WorkflowThrew && !InfrastructureError.is(err.err)) {
              finish({ status: "threw", err: err.err });
              return;
            }
            states[i] = { status: "stopped" };
            combinator.fatal ??= { err: err instanceof WorkflowThrew ? err.err : err };
            barrier.releaseStopped();
            finish({ status: "stopped" });
          });
        });
        evaluate();
        // The owner does not count as running while it waits for its branches.
        barrier.leave();
      });
    }

    async function recordWorkflowResult(value: unknown) {
      const stepKey = WORKFLOW_RESULT_STEP_KEY;
      const cached = await infrastructure(() =>
        heapClient.readStep({ executionId, stepKey })
      );
      // The workflow result is encoded on every delivery, so that an invalid
      // result fails the same way whether or not it is already recorded.
      const record = encodeStepRecord(new WorkflowResult(value), stepKey);
      if (!cached) {
        await writeStep({ stepKey, stepAttempt: 0, done: true, stepResponseJson: record.json });
      }
      // Only an invalid workflow result is recorded as a step error under
      // the workflow result's key. The workflow has finished, so it fails.
      if (record.response instanceof StepError) throw record.response.err;
      return record.response as WorkflowResult<Result>;
    }

    let outcome: DriveOutcome;
    try {
      outcome = await drive(workflowIterator, new DrivenBranch());
    } catch (err) {
      throw err instanceof WorkflowThrew ? err.err : err;
    }
    switch (outcome.status) {
      case "returned":
        return await recordWorkflowResult(outcome.value);
      case "suspended":
        yield outcome.response;
        throw new Error("A suspended delivery cannot be resumed");
      case "stopped":
        throw new Error("The workflow stopped without an outcome");
    }
  };
}

/** An error that workflow code threw, as opposed to one the driver raised. */
class WorkflowThrew {
  constructor(readonly err: unknown) {}
}

type DriveOutcome =
  /** `afterDelay`: the branch passed an elapsed delay in this delivery and reached no recorded step after it. */
  | { status: "returned"; value: unknown; afterDelay?: boolean }
  | { status: "suspended"; response: StepResponse }
  | { status: "stopped" };

type StepAction =
  | { resume: StepResult | undefined; delay?: true }
  | { throw: unknown }
  | Exclude<DriveOutcome, { status: "returned" }>;

type BranchState =
  | { status: "running" }
  | { status: "threw"; err: unknown }
  | DriveOutcome;

/** The combinator's decision once `finished` (in order) have returned or thrown, if it has one. */
function decide(
  kind: "all" | "race",
  mode: "fail-fast" | "settle",
  states: readonly BranchState[],
  finished: readonly number[]
): BranchesDecision | undefined {
  if (kind === "race") {
    // A branch that passed an elapsed delay in this delivery loses to one
    // that returned without, as a timed `when` or `join` checks its
    // condition before its deadline.
    const returned = finished.filter((i) => states[i]!.status === "returned");
    const winner =
      returned.find((i) => !(states[i] as { afterDelay?: boolean }).afterDelay) ?? returned[0];
    if (winner !== undefined) return { decision: "won", index: winner };
    return states.every((state) => state.status === "threw") ? { decision: "failed" } : undefined;
  }
  if (mode === "fail-fast") {
    const failed = finished.find((i) => states[i]!.status === "threw");
    if (failed !== undefined) return { decision: "rejected", index: failed };
    return states.every((state) => state.status === "returned")
      ? { decision: "fulfilled" }
      : undefined;
  }
  return finished.length === states.length ? { decision: "settled" } : undefined;
}

/** The branch states that the combinator's value or error is read from under `decision`. */
function readsFrom(decision: BranchesDecision, states: readonly BranchState[]): readonly BranchState[] {
  switch (decision.decision) {
    case "rejected":
    case "won":
      return [states[decision.index]!];
    case "cancelled":
      return [];
    default:
      return states;
  }
}

/**
 * The combinator's value or error under `decision`, read from what its
 * branches returned or threw. Throws if the branches no longer reach the
 * recorded outcome, as when workflow code changed.
 */
function applyDecision(
  stepKey: string,
  decision: BranchesDecision,
  states: readonly BranchState[]
): StepAction {
  const diverged = () =>
    new Error(`Step "${stepKey}" no longer reaches its recorded outcome on replay`);
  const value = (i: number) => {
    const state = states[i];
    if (state?.status !== "returned") throw diverged();
    return state.value;
  };
  const error = (i: number) => {
    const state = states[i];
    if (state?.status !== "threw") throw diverged();
    return state.err;
  };
  switch (decision.decision) {
    case "fulfilled":
      return { resume: new StepResult(states.map((_, i) => value(i))) };
    case "rejected":
      return { throw: error(decision.index) };
    case "settled":
      return {
        resume: new StepResult(
          states.map((state, i) =>
            state.status === "returned"
              ? { status: "fulfilled", value: state.value }
              : { status: "rejected", error: error(i) }
          )
        ),
      };
    case "won":
      return {
        resume: new StepResult({ index: decision.index, value: value(decision.index) }),
      };
    case "failed":
      return { throw: new WorkflowRaceError(stepKey, states.map((_, i) => error(i))) };
    case "cancelled":
      return { throw: decision.err };
  }
}

/**
 * The suspension of a combinator whose branches suspended. The runner reads
 * only `wakeAt`, so this is a delay at the earliest wake time, or the first
 * suspension if none has a wake time.
 */
function earliestWake(states: readonly BranchState[]): StepResponse {
  const suspended = states.flatMap((state) =>
    state.status === "suspended" ? [state.response as StepResponse & { wakeAt?: number }] : []
  );
  if (suspended.length === 0) throw new Error("A combinator suspended without a suspended branch");
  const wakeTimes = suspended.flatMap(({ wakeAt }) => (wakeAt === undefined ? [] : [wakeAt]));
  return wakeTimes.length > 0 ? new StepDelay(Math.min(...wakeTimes)) : suspended[0]!;
}

export const createWorkflow = workflow;

/**
 * Whether a cancellation is delivered at this step: it has no record, an
 * unfinished record (a store, join or effect wait, or a retry backoff), or a
 * delay that has not elapsed.
 */
function isUnfinishedStep(
  cached: { meta: { done: boolean } } | null,
  recorded: StepResponse | null,
  now: number
) {
  if (!cached || !cached.meta.done) return true;
  return recorded instanceof StepDelay && recorded.wakeAt > now;
}

/**
 * Ends the delivery once its signal has aborted, before it runs more workflow
 * code or makes another fenced write. Like a lost lease found by a fenced
 * write, it rejects with an infrastructure error, which is not acknowledged.
 */
function stopIfAborted(signal: AbortSignal) {
  if (signal.aborted) throw new InfrastructureError(signal.reason);
}
