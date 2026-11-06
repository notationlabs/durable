import type { Logger } from "pino";
import type {
  WorkflowGenerator,
  WorkflowEvent,
  EventParams,
  ExecutionLease,
  WorkflowRunOutcome,
} from "..";
import type { DeliveryEvent } from "../base/event";
import {
  InfrastructureError,
  ReplayDivergenceError,
  StepDelay,
  StepEffectWait,
  StepJoinWait,
  StepStoreWait,
  WorkflowCancelledError,
  WorkflowResult,
} from "..";
import type { Runtime } from "../base/runtime";
import type { DeliveryOptions } from "../base/invoker";
import { EventContext } from "../base/event";

export class WorkflowRunner<
  Router extends Record<string, WorkflowGenerator<any, any, any>>
> {
  private runtime: Runtime;
  private router: Router;
  private logger: Logger;

  constructor(runtime: Runtime, options: { router: Router; logger: Logger }) {
    this.runtime = runtime;
    this.router = options.router;
    this.logger = options.logger;
  }

  /**
   * Runs one delivery under `options.lease` and resolves with its outcome.
   * Rejects only with an infrastructure error, which must not be acknowledged
   * as an outcome; a lost lease is one, and so is an aborted `options.signal`.
   *
   * A workflow that throws `WorkflowCancelledError` ends `cancelled` only when
   * the delivery carried a cancellation request; otherwise it ends `failed`.
   * A replay that diverges from the recorded history ends `diverged`.
   *
   * The event's `workflowVersion` is `options.workflowVersion` when the
   * record has one. Otherwise it is 1 when the execution already has step
   * records, and the version the workflow declares when it has none. The
   * outcome carries it, so the acknowledgement can record it.
   */
  run = async (
    deliveryEvent: DeliveryEvent,
    options: DeliveryOptions & { logger?: Logger }
  ): Promise<WorkflowRunOutcome> => {
    const { lease, signal, cancellation, logger = this.logger } = options;
    const workflow = this.router[deliveryEvent.workflowId];

    if (!workflow) {
      return {
        status: "failed",
        error: new Error(`No workflow registered for "${deliveryEvent.workflowId}"`),
      };
    }

    // An execution with history but no recorded version was first delivered
    // before versions were recorded, by code that declared none.
    const workflowVersion =
      options.workflowVersion ??
      ((await this.hasHistory(deliveryEvent.executionId)) ? 1 : (workflow.workflowVersion ?? 1));
    const event: WorkflowEvent = { ...deliveryEvent, workflowVersion };
    const outcome = await this.runEvent(workflow, event, {
      lease,
      signal,
      cancellation,
      logger,
    });
    return { ...outcome, workflowVersion };
  };

  private async hasHistory(executionId: string) {
    try {
      return (await this.runtime.heap.listStepKeys({ executionId })).length > 0;
    } catch (err) {
      throw new InfrastructureError(err);
    }
  }

  private async runEvent(
    workflow: WorkflowGenerator<any, any, any>,
    event: WorkflowEvent,
    options: Pick<DeliveryOptions, "lease" | "signal" | "cancellation"> & { logger: Logger }
  ): Promise<WorkflowRunOutcome> {
    const { lease, signal, cancellation, logger } = options;

    let response:
      | WorkflowResult<unknown>
      | StepDelay
      | StepEffectWait
      | StepStoreWait
      | StepJoinWait;
    try {
      response = await this.runWorkflows({
        workflow,
        logger,
        event,
        lease,
        signal,
        cancellation,
      });
    } catch (err) {
      if (InfrastructureError.is(err)) throw err;
      if (ReplayDivergenceError.is(err)) {
        logger.warn(
          { executionId: event.executionId, code: err.code, stepKey: err.stepKey },
          `Replay diverged from the recorded history; the execution is parked until its divergence is cleared: ${err.message}`
        );
        return {
          status: "diverged",
          code: err.code,
          ...(err.stepKey !== undefined ? { stepKey: err.stepKey } : {}),
        };
      }
      if (cancellation && err instanceof WorkflowCancelledError) {
        return { status: "cancelled" };
      }
      return { status: "failed", error: toError(err) };
    }

    switch (response.type) {
      case "workflow-result":
        return { status: "completed", result: response.result };
      case "step-delay":
      case "effect-wait":
      case "store-wait":
      case "join-wait":
        return { status: "suspended", wakeAt: response.wakeAt };
    }
  }

  private async runWorkflows<
    Params extends EventParams,
    Result,
    Context extends EventContext
  >(params: {
    workflow: WorkflowGenerator<Params, Result, Context>;
    event: WorkflowEvent<Params, Context>;
    lease: ExecutionLease;
    signal: AbortSignal;
    cancellation: DeliveryOptions["cancellation"];
    logger: Logger;
  }): Promise<
    WorkflowResult<Result> | StepDelay | StepEffectWait | StepStoreWait | StepJoinWait
  > {
    const { workflow, event, lease, signal, cancellation, logger } = params;

    const workflowIterator = workflow({
      event,
      lease,
      signal,
      ...(cancellation ? { cancellation } : {}),
      requireStepKeys: this.runtime.requireStepKeys,
      heapClient: this.runtime.heap,
      storeClient: this.runtime.stores,
      executionClient: this.runtime.executions,
      effectClient: this.runtime.effects,
      joinClient: this.runtime.joins,
      clock: this.runtime.clock,
      logger,
    });

    const iteratorResult = await workflowIterator.next();
    const stageResponse = iteratorResult.value;

    if (iteratorResult.done) {
      return stageResponse as WorkflowResult<Result>;
    }

    // A delay's `wakeAt` is the absolute time it elapses, which is exactly
    // the `wakeAt` the suspended outcome reports.
    if (stageResponse instanceof StepDelay) {
      return stageResponse;
    }

    if (
      stageResponse instanceof StepEffectWait ||
      stageResponse instanceof StepStoreWait ||
      stageResponse instanceof StepJoinWait
    ) {
      return stageResponse;
    }

    throw new Error(
      "Workflow runner critical error. This is likely because your worklow and worker are referencing different versions of @yieldstar/core."
    );
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
