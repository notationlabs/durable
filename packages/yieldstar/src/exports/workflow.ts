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
  isAbandonedRecord,
  markAbandoned,
} from "../internal/step-record";
import { createStepRunner } from "../internal/step-runner";
import {
  StepRuntimeCall,
  StepStoreWaitRequest,
  StepUnfinishedRecord,
  executeRuntimeCall,
  type RuntimeClients,
  infrastructure,
} from "../internal/runtime-call";
import { StepEffectCall, executeEffectCall } from "../internal/effect-call";
import {
  BranchesStepKey,
  CancellationBarrier,
  Combinator,
  DecisionLock,
  DrivenBranch,
  PatchStepKey,
  STOPPED,
  StepBranches,
  StepBranchesDecision,
  StepSchemaChanged,
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
  ReplayDivergenceError,
  StepConflictError,
  WorkflowCancelledError,
  WorkflowRaceError,
  WorkflowReplayError,
  isLeaseLost,
  WORKFLOW_RESULT_STEP_KEY,
  type ExecutionCancellation,
  type HeapRecord,
  type WorkflowGeneratorParams,
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

export type WorkflowOptions = {
  /**
   * A non-negative integer. The first delivery of each execution records it,
   * and every later delivery exposes the recorded version as
   * `event.workflowVersion`, whatever the code declares by then.
   * @default 1
   */
  version: number;
};

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
): WorkflowGenerator<Params, Result, Context>;
export function workflow<
  Params extends EventParams,
  Result,
  Context extends EventContext
>(
  options: WorkflowOptions,
  workflowFn: WorkflowFn<Params, Result, Context> &
    DurableWorkflowFn<Params, Result>
): WorkflowGenerator<Params, Result, Context>;
export function workflow(
  arg1: WorkflowOptions | WorkflowFn<any, any, any>,
  arg2?: WorkflowFn<any, any, any>
): WorkflowGenerator<any, any, any> {
  const workflowFn = typeof arg1 === "function" ? arg1 : arg2;
  if (typeof workflowFn !== "function") {
    throw new TypeError("workflow requires a workflow function");
  }
  const version = typeof arg1 === "function" ? 1 : arg1?.version;
  if (typeof version !== "number" || !Number.isSafeInteger(version) || version < 0) {
    throw new TypeError("workflow version must be a non-negative integer");
  }
  return Object.assign(createWorkflowGenerator(workflowFn), { workflowVersion: version });
}

function createWorkflowGenerator<
  Params extends EventParams,
  Result,
  Context extends EventContext
>(workflowFn: WorkflowFn<Params, Result, Context>) {
  return async function* workflowGenerator({
    event,
    lease,
    signal,
    cancellation,
    requireStepKeys = false,
    heapClient,
    storeClient,
    executionClient,
    effectClient,
    joinClient,
    clock,
    logger,
  }: WorkflowGeneratorParams<Params, Context>): AsyncGenerator<
    StepResponse,
    WorkflowResult<Result>,
    StepResponse
  > {
    const stepRunner = createStepRunner({ event, clock });
    const workflowIterator = workflowFn(stepRunner, event, logger);
    const { executionId } = event;
    const clients: RuntimeClients = {
      stores: storeClient,
      executions: executionClient,
      joins: joinClient,
    };
    const { workflowVersion } = event;
    // The keys this delivery requested, shared by every branch, so a key
    // repeated across branches is rejected. A recorded step is consumed once
    // its key is in here.
    const stepKeys = new Set<string>();
    // The kind each requested key was requested as, stored with its records.
    const stepKinds = new Map<string, string>();
    const lock = new DecisionLock();
    // Every step the history recorded before this delivery. Records this
    // delivery writes are for keys it requested.
    const recordedSteps = new Set(
      (await infrastructure(() => heapClient.listSteps({ executionId }))).map(
        ({ stepKey }) => stepKey
      )
    );
    // Set once replay diverges. No fenced write is made after it.
    let divergence: ReplayDivergenceError | undefined;

    function diverge(code: ReplayDivergenceError["code"], message: string, stepKey?: string) {
      divergence ??= new ReplayDivergenceError(code, message, stepKey);
      return divergence;
    }

    /** Whether a branch must start no new work: it is abandoned, or replay has diverged. */
    function stopped(branch: DrivenBranch) {
      return branch.stopped || divergence !== undefined;
    }

    function firstUnconsumed(): string | undefined {
      for (const stepKey of recordedSteps) {
        if (!stepKeys.has(stepKey)) return stepKey;
      }
      return undefined;
    }

    /**
     * Ends the delivery as diverged when a recorded step has not been
     * consumed. `stepKey` names the step that would have run, if any.
     */
    function assertConsumed(stepKey?: string) {
      const unconsumed = firstUnconsumed();
      if (unconsumed === undefined) return;
      throw diverge(
        "NONDETERMINISTIC_REPLAY",
        stepKey === undefined
          ? `Replay did not reach recorded step "${unconsumed}"`
          : `Step "${stepKey}" would run, but replay did not reach recorded step "${unconsumed}"`,
        stepKey
      );
    }

    /** Writes one attempt of a step. A lost lease or a conflicting record ends the delivery. */
    async function writeStep(params: {
      stepKey: string;
      stepAttempt: number;
      done: boolean;
      stepResponseJson: string;
      deleteWaitersOf?: readonly string[];
    }) {
      if (divergence) throw divergence;
      stopIfAborted(signal);
      const stepKind = stepKinds.get(params.stepKey);
      const written = await infrastructure(() =>
        heapClient.writeStep({
          lease,
          ...params,
          ...(stepKind !== undefined ? { stepKind } : {}),
          workflowVersion,
        })
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
     * Records the delivery of `request` at every parked step in one write,
     * with their waiters deleted and `deliveredAt` set. Each combinator above
     * a parked step that has not settled records `cancelled` in the same
     * write. Returns the error to throw into workflow code at each step. A
     * lost lease or a conflicting record ends the delivery, as in `writeStep`.
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
      ].map((step) => {
        const stepKind = stepKinds.get(step.stepKey);
        return stepKind !== undefined ? { ...step, stepKind } : step;
      });
      // Set before the write, so no combinator settles another way meanwhile.
      // A failed write ends the delivery.
      for (const combinator of combinators) {
        combinator.decision = (cancelled.response as StepBranchesDecision).decision;
      }
      if (divergence) throw divergence;
      stopIfAborted(signal);
      const written = await infrastructure(() =>
        heapClient.recordCancellation({ lease, steps, workflowVersion })
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
      return parked.map(() => (decodeStepRecord(thrown) as StepCancelled).err);
    }

    // A request is delivered once. Replay of a delivered request reads the
    // recorded throw instead of the request.
    const barrier = new CancellationBarrier(
      cancellation && cancellation.deliveredAt === undefined ? cancellation : undefined,
      deliverCancellation
    );

    /**
     * Advances one generator, the workflow's or a branch's, until it returns,
     * suspends or is abandoned. An error that workflow code throws rejects
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

      while (true) {
        let stepAttempt = 0;

        // Throwing a step error into the workflow already advanced it.
        if (nextIteratorResult) {
          iteratorResult = nextIteratorResult;
          nextIteratorResult = null;
        } else {
          iteratorResult = await next(resumeWith);
        }
        resumeWith = undefined;

        if (iteratorResult.done) {
          return { status: "returned", value: iteratorResult.value };
        }
        if (iteratorResult.value instanceof StepSchemaChanged) {
          throw diverge(
            "STEP_SCHEMA_CHANGED",
            iteratorResult.value.message,
            iteratorResult.value.stepKey
          );
        }
        if (!(iteratorResult.value instanceof StepKey)) {
          throw new Error("Step runners must yield a StepKey object");
        }
        const requested = iteratorResult.value;
        const stepKey = requested.key;
        const isCombinator = requested instanceof BranchesStepKey;
        if (requested instanceof PatchStepKey && branch.parent) {
          // Whether a branch passed the patch point depends on how branches
          // interleave, so a patch is rejected inside any branch. A repeat
          // of the ID in another branch throws the same error unrecorded.
          const err = new WorkflowReplayError(
            "PATCH_IN_COMBINATOR",
            `step.patched("${requested.patchId}") cannot be called inside a step.all or step.race branch`,
            stepKey
          );
          if (!stepKeys.has(stepKey)) {
            stepKeys.add(stepKey);
            stepKinds.set(stepKey, "patched");
            branch.reach(stepKey);
            const cached = await infrastructure(() =>
              heapClient.readStep({ executionId, stepKey })
            );
            if (!cached && !branch.stopped) {
              await writeStep({
                stepKey,
                stepAttempt: 0,
                done: true,
                stepResponseJson: encodeStepRecord(new StepError(err), stepKey).json,
              });
            }
          }
          if (branch.abandoned || branch.stopped) return { status: "stopped" };
          nextIteratorResult = await throwInto(err);
          continue;
        }
        if (stepKeys.has(stepKey)) {
          throw new Error("Each step in a loop must have a unique cache key.");
        }
        stepKeys.add(stepKey);
        if (requested.kind !== undefined) stepKinds.set(stepKey, requested.kind);
        branch.reach(stepKey);

        let cached = await infrastructure(() =>
          heapClient.readStep({
            executionId,
            stepKey,
          })
        );

        const recordedKind = cached?.meta.kind;
        if (recordedKind !== undefined && requested.kind !== undefined && recordedKind !== requested.kind) {
          throw diverge(
            "NONDETERMINISTIC_REPLAY",
            `Step "${stepKey}" is recorded as a ${recordedKind} step, but replay requested it as a ${requested.kind} step`,
            stepKey
          );
        }

        if (requested instanceof PatchStepKey) {
          // A marker is recorded only at the edge of the history, so the
          // check below is all a patch needs.
          let applies = cached !== null;
          if (!cached && firstUnconsumed() === undefined) {
            await writeStep({
              stepKey,
              stepAttempt: 0,
              done: true,
              stepResponseJson: encodeStepRecord(new StepResult(true), stepKey).json,
            });
            applies = true;
          }
          resumeWith = new StepResult(applies);
          continue;
        }

        if (cached?.meta) {
          stepAttempt = cached.meta.attempt + 1;
        }

        const recorded = cached ? decodeStepRecord(cached.stepResponseJson) : null;

        // Outside every combinator, steps run one at a time, so a step about
        // to do new work must find the whole history consumed. Inside a
        // combinator, branches interleave differently on each delivery, and
        // the check waits for the end of the delivery.
        // A recorded delay followed by records the original run made after it
        // was passed, even if this host's clock reads earlier than its wakeAt.
        const passed =
          !branch.parent &&
          cached?.meta.done === true &&
          recorded instanceof StepDelay &&
          firstUnconsumed() !== undefined;
        const unfinishedStep = !passed && isUnfinishedStep(cached, recorded, clock());
        if (!branch.parent && !isCombinator && unfinishedStep) {
          assertConsumed(stepKey);
        }

        let thrown: { err: unknown } | undefined;
        if (cached?.meta.done && recorded instanceof StepCancelled) {
          thrown = { err: recorded.err };
        } else if (!isCombinator && unfinishedStep) {
          // A stopped branch replays its recorded steps and stops at the
          // first step that needs new work. A combinator is excluded: one
          // without a record is still driven so that replay reaches the keys
          // its branches reached, and it is not a cancellation point.
          if (stopped(branch)) return { status: "stopped" };
          if (barrier.pending && !isCombinator) {
            // A delay is recorded done when it starts, so its cancellation
            // is written as the step's next attempt, which replay reads.
            const err = await barrier.park(branch, stepKey, stepAttempt);
            if (err === STOPPED) return { status: "stopped" };
            thrown = { err };
          }
        }
        if (!thrown && !cached && requested.keyless) {
          if (requireStepKeys) {
            const record = encodeStepRecord(
              new StepError(
                new WorkflowReplayError(
                  "STEP_KEY_REQUIRED",
                  `A step without a key has no record, and the runtime requires step keys. Pass an explicit key to the step at this call site.`,
                  stepKey
                )
              ),
              stepKey
            );
            await writeStep({ stepKey, stepAttempt, done: true, stepResponseJson: record.json });
            thrown = { err: (record.response as StepError).err };
          } else {
            logger.warn(
              { executionId, stepKey },
              "Deprecated: a step without a key takes its key from its call site, so editing the workflow file can make replay run it again. Pass an explicit key."
            );
          }
        }
        if (thrown) {
          // The step is suspended at its StepKey, so the throw reaches
          // workflow code without running the step.
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
          if (stopped(branch)) {
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

        if ("throw" in action) {
          // An abandoned branch is not resumed with an error raised after it
          // was abandoned, so its catch and finally blocks do not run. An
          // error recorded before then is thrown again, so replay consumes
          // the steps its catch block recorded.
          if (branch.abandoned && (!cached || isAbandonedRecord(cached.stepResponseJson))) {
            return { status: "stopped" };
          }
          nextIteratorResult = await throwInto(action.throw);
          continue;
        }
        if ("resume" in action) {
          resumeWith = action.resume;
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
      /** A recorded delay that counts as elapsed whatever the clock reads. */
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
        stepResponse instanceof StepStoreWaitRequest ||
        stepResponse instanceof StepUnfinishedRecord
      ) {
        stopIfAborted(signal);
        if (stopped(branch)) return { status: "stopped" };
        if (stepResponse instanceof StepUnfinishedRecord) {
          await writeStep({
            stepKey,
            stepAttempt,
            done: false,
            stepResponseJson: encodeStepRecord(stepResponse.response, stepKey).json,
          });
          stepAttempt++;
          iteratorResult = await next();
          stepResponse = iteratorResult.value;
          continue;
        }
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
          const registered = await lock.shared(async () =>
            stopped(branch)
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
        const callResult = await lock.shared(async () => {
          if (stopped(branch)) return STOPPED;
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
        const outcome = await runBranches(
          stepResponse,
          branch,
          stepKey,
          stepAttempt,
          cached ? (recorded as StepBranchesDecision).decision : undefined
        );
        switch (outcome.status) {
          case "value":
            return { resume: new StepResult(outcome.value) };
          case "threw":
            return { throw: outcome.err };
          default:
            return outcome;
        }
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
        // An error raised after the branch was abandoned is never thrown into
        // it, so the record says so and replay does not throw it either.
        if (stepResponse instanceof StepError && !needsRetry && branch.abandoned) {
          stepResponseJson = markAbandoned(stepResponseJson);
        }
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
        return { resume: undefined };
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
     * branches' replay instead. Rejects with an error that ends the delivery.
     */
    function runBranches(
      request: StepBranches,
      owner: DrivenBranch,
      stepKey: string,
      stepAttempt: number,
      recorded: BranchesDecision | undefined
    ): Promise<BranchesOutcome> {
      const { combinator: kind, mode, concurrency, branches } = request.request;
      const combinator = new Combinator(owner, stepKey, stepAttempt, concurrency);
      combinator.decision = recorded;
      const children = branches.map((_, i) => new DrivenBranch(combinator, i));
      const states: BranchState[] = branches.map(() => ({ status: "running" }));
      // Indexes of branches that returned or threw, in the order they did.
      const finished: number[] = [];
      let writing = false;
      let done = false;

      return new Promise<BranchesOutcome>((resolve, reject) => {
        const settle = (fn: () => BranchesOutcome) => {
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
          // While a cancellation is pending, a combinator with a branch still
          // replaying waits: the delivery may land in that branch.
          if (!combinator.decision && !owner.stopped && !(barrier.pending && running)) {
            const decision = decide(kind, mode, states, finished);
            if (decision) {
              recordDecision(decision);
              return;
            }
          }
          if (running) return;
          const { decision } = combinator;
          if (
            decision &&
            (decision.decision !== "cancelled" ||
              states.every((state) => state.status === "returned" || state.status === "threw"))
          ) {
            settle(() =>
              applyDecision(stepKey, decision, states, () =>
                diverge(
                  "NONDETERMINISTIC_REPLAY",
                  `Step "${stepKey}" no longer reaches its recorded outcome on replay`,
                  stepKey
                )
              )
            );
            return;
          }
          if (owner.stopped) {
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
          const json = encodeStepRecord(new StepBranchesDecision(decision), stepKey).json;
          lock
            .exclusive(() =>
              writeStep({
                stepKey,
                stepAttempt,
                done: true,
                stepResponseJson: json,
                deleteWaitersOf: children.flatMap((child) => child.keys),
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

        barrier.enter(children.length);
        children.forEach((child, i) => {
          drive(branches[i]!, child)
            .then(
              (outcome) => {
                states[i] = outcome;
              },
              (err) => {
                if (err instanceof WorkflowThrew && !InfrastructureError.is(err.err)) {
                  states[i] = { status: "threw", err: err.err };
                  return;
                }
                states[i] = { status: "stopped" };
                combinator.fatal ??= { err: err instanceof WorkflowThrew ? err.err : err };
                barrier.releaseStopped();
              }
            )
            .then(() => {
              const { status } = states[i]!;
              if (status === "returned" || status === "threw") finished.push(i);
              evaluate();
              barrier.leave();
            });
        });
        evaluate();
        // The owner does not count as running while it waits for its branches.
        barrier.leave();
      });
    }

    async function recordWorkflowResult(value: unknown) {
      const stepKey = WORKFLOW_RESULT_STEP_KEY;
      stepKeys.add(stepKey);
      stepKinds.set(stepKey, "result");
      assertConsumed();
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
      const thrown = err instanceof WorkflowThrew ? err.err : err;
      // A failed or cancelled outcome is terminal, so the whole history must
      // be consumed before it is recorded.
      if (!InfrastructureError.is(thrown) && !ReplayDivergenceError.is(thrown)) {
        assertConsumed();
      }
      throw thrown;
    }
    switch (outcome.status) {
      case "returned":
        return await recordWorkflowResult(outcome.value);
      case "suspended":
        assertConsumed();
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
  | { status: "returned"; value: unknown }
  | { status: "suspended"; response: StepResponse }
  | { status: "stopped" };

type StepAction =
  | { resume: StepResult | undefined }
  | { throw: unknown }
  | Exclude<DriveOutcome, { status: "returned" }>;

type BranchState =
  | { status: "running" }
  | { status: "threw"; err: unknown }
  | DriveOutcome;

type BranchesOutcome =
  | { status: "value"; value: unknown }
  | { status: "threw"; err: unknown }
  | Exclude<DriveOutcome, { status: "returned" }>;

/** The combinator's decision once `finished` (in order) have returned or thrown, if it has one. */
function decide(
  kind: "all" | "race",
  mode: "fail-fast" | "settle",
  states: readonly BranchState[],
  finished: readonly number[]
): BranchesDecision | undefined {
  if (kind === "race") {
    const winner = finished.find((i) => states[i]!.status === "returned");
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

/**
 * The combinator's value or error under `decision`, read from what its
 * branches returned or threw. Throws `diverged()` if the branches no longer
 * reach the recorded outcome, as when workflow code changed.
 */
function applyDecision(
  stepKey: string,
  decision: BranchesDecision,
  states: readonly BranchState[],
  diverged: () => Error
): BranchesOutcome {
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
      return { status: "value", value: states.map((_, i) => value(i)) };
    case "rejected":
      return { status: "threw", err: error(decision.index) };
    case "settled":
      return {
        status: "value",
        value: states.map((state, i) =>
          state.status === "returned"
            ? { status: "fulfilled", value: state.value }
            : { status: "rejected", error: error(i) }
        ),
      };
    case "won":
      return {
        status: "value",
        value: { index: decision.index, value: value(decision.index) },
      };
    case "failed":
      return {
        status: "threw",
        err: new WorkflowRaceError(stepKey, states.map((_, i) => error(i))),
      };
    case "cancelled":
      return { status: "threw", err: decision.err };
  }
}

/** The suspension with the earliest wake time, or the first if none has one. */
function earliestWake(states: readonly BranchState[]): StepResponse {
  let earliest: (StepResponse & { wakeAt?: number }) | undefined;
  for (const state of states) {
    if (state.status !== "suspended") continue;
    const response = state.response as StepResponse & { wakeAt?: number };
    if (
      !earliest ||
      (response.wakeAt !== undefined &&
        (earliest.wakeAt === undefined || response.wakeAt < earliest.wakeAt))
    ) {
      earliest = response;
    }
  }
  if (!earliest) throw new Error("A combinator suspended without a suspended branch");
  return earliest;
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
