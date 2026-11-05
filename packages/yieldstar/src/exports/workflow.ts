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
  StepUnfinished,
  UnfinishedCacheCheck,
  decodeStepRecord,
  encodeStepRecord,
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
  StepResponse,
  StepKey,
  StepError,
  StepResult,
  StepDelay,
  StepCacheCheck,
  StepEffectWait,
  StepInvalid,
  StepStoreWait,
  WorkflowResult,
  InfrastructureError,
  LeaseLostError,
  StepConflictError,
  isLeaseLost,
  WORKFLOW_RESULT_STEP_KEY,
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

    /** Writes one attempt of a step. A lost lease or a conflicting record ends the delivery. */
    async function writeStep(params: {
      stepKey: string;
      stepAttempt: number;
      done: boolean;
      stepResponseJson: string;
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

    let iteratorResult: IteratorResult<any> | null = null;
    let nextIteratorResult: IteratorResult<any> | null = null;
    // A step that produced a result resumes with its decoded record, which
    // is what it returns to workflow code.
    let resumeWith: StepResult | undefined;
    const stepKeys = new Set<string>();

    while (true) {
      let stepAttempt = 0;
      let stepKey: string;

      // Throwing a step error into the workflow already advanced it.
      if (nextIteratorResult) {
        iteratorResult = nextIteratorResult;
        nextIteratorResult = null;
      } else {
        stopIfAborted(signal);
        iteratorResult = await workflowIterator.next(resumeWith);
      }
      resumeWith = undefined;

      if (iteratorResult.done) {
        stepKey = WORKFLOW_RESULT_STEP_KEY;
      } else if (!(iteratorResult.value instanceof StepKey)) {
        throw new Error("Step runners must yield a StepKey object");
      } else {
        stepKey = iteratorResult.value.key;
        if (stepKeys.has(stepKey)) {
          throw new Error("Each step in a loop must have a unique cache key.");
        }
        stepKeys.add(stepKey)
      }

      let cached = await infrastructure(() =>
        heapClient.readStep({
          executionId,
          stepKey,
        })
      );

      if (cached?.meta) {
        stepAttempt = cached.meta.attempt + 1;
      }

      // A step with an unfinished record runs again. A step that yields an
      // UnfinishedCacheCheck receives the record.
      let unfinished: typeof cached = null;
      if (cached?.meta.done === false) {
        unfinished = cached;
        cached = null;
      }

      if (!iteratorResult.done) {
        iteratorResult = await workflowIterator.next();
      }

      let stepResponse: StepResponse;
      let suspendedOnStore = false;

      if (iteratorResult.done) {
        stepResponse = new WorkflowResult(iteratorResult.value);
      } else {
        stepResponse = iteratorResult.value;
      }

      if (stepResponse instanceof StepCacheCheck) {
        const cachedResponse = cached
          ? decodeStepRecord(cached.stepResponseJson)
          : unfinished && stepResponse instanceof UnfinishedCacheCheck
            ? new StepUnfinished(decodeStepRecord(unfinished.stepResponseJson))
            : null;
        // Resuming the step runs the step function unless it is cached.
        stopIfAborted(signal);
        iteratorResult = await workflowIterator.next(cachedResponse);
        stepResponse = iteratorResult.value;

        // A step that needs a runtime client yields a request. Make the call
        // here, outside workflow code, and resume the step with its result.
        // An infrastructure error rejects the delivery without resuming it.
        while (
          stepResponse instanceof StepRuntimeCall ||
          stepResponse instanceof StepEffectCall ||
          stepResponse instanceof StepStoreWaitRequest ||
          stepResponse instanceof StepUnfinishedRecord
        ) {
          stopIfAborted(signal);
          if (stepResponse instanceof StepUnfinishedRecord) {
            await writeStep({
              stepKey,
              stepAttempt,
              done: false,
              stepResponseJson: encodeStepRecord(stepResponse.response, stepKey).json,
            });
            stepAttempt++;
            iteratorResult = await workflowIterator.next();
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
            const registered = await executeRuntimeCall(
              new StepRuntimeCall(({ stores }) => stores.registerWaiter(waiter)),
              clients
            );
            // 3. Suspend.
            if (registered.ok) {
              stepResponse = record.response;
              suspendedOnStore = true;
              break;
            }
            // The failure is recorded as the next attempt.
            stepAttempt++;
            iteratorResult = await workflowIterator.next(registered);
            stepResponse = iteratorResult.value;
            continue;
          }
          const callResult =
            stepResponse instanceof StepEffectCall
              ? await executeEffectCall(stepResponse.request, {
                  effects: effectClient,
                  executionId,
                  // Each earlier attempt of an effect step that ended
                  // uncertain wrote one unfinished record.
                  attempt: stepAttempt,
                  clock,
                  assertLease: () => stopIfAborted(signal),
                  logger,
                })
              : await executeRuntimeCall(stepResponse, clients);
          iteratorResult = await workflowIterator.next(callResult);
          stepResponse = iteratorResult.value;
        }
      } else if (!(stepResponse instanceof WorkflowResult)) {
        throw new Error("Step runners must yield a CacheCheck object");
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
      // The workflow result is encoded on every delivery, so that an invalid
      // result fails the same way whether or not it is already recorded.
      if (!cached || stepResponse instanceof WorkflowResult) {
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
          stepResponse instanceof StepEffectWait);

      if (!cached && stepResponseJson !== undefined && !suspendedOnStore) {
        await writeStep({
          stepKey,
          stepAttempt,
          done: !needsRetry && !needsWake,
          stepResponseJson,
        });
      }

      if (stepResponse instanceof WorkflowResult) {
        return stepResponse;
      }

      // Only an invalid workflow result is recorded as a step error under
      // the workflow result's key. The workflow has finished, so it fails.
      if (iteratorResult.done && stepResponse instanceof StepError) {
        throw stepResponse.err;
      }

      if (stepResponse instanceof StepInvalid) {
        break;
      }

      if (stepResponse instanceof StepError) {
        if (needsRetry) {
          yield new StepDelay(clock() + stepResponse.retryInterval);
        } else {
          stopIfAborted(signal);
          nextIteratorResult = await workflowIterator.throw(stepResponse.err);
          continue;
        }
      }

      if (stepResponse instanceof StepDelay) {
        if (stepResponse.wakeAt <= clock()) {
          continue;
        }
      }

      if (stepResponse instanceof StepResult) {
        resumeWith = stepResponse;
      } else {
        yield stepResponse;
      }
    }

    throw new Error(
      "Workflow yielded an invalid step. Steps must be yielded with yield* from a step runner."
    );
  };
}

export const createWorkflow = workflow;

/**
 * Ends the delivery once its signal has aborted, before it runs more workflow
 * code or makes another fenced write. Like a lost lease found by a fenced
 * write, it rejects with an infrastructure error, which is not acknowledged.
 */
function stopIfAborted(signal: AbortSignal) {
  if (signal.aborted) throw new InfrastructureError(signal.reason);
}
