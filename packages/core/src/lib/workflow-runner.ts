import type { Logger } from "pino";
import type { WorkflowGenerator } from "../base/workflow";
import type { WorkflowEvent, EventParams } from "../base/event";
import type { ExecutionLease, WorkflowRunOutcome } from "../base/execution";
import { InfrastructureError, WorkflowCancelledError } from "../base/execution";
import {
  StepDelay,
  StepEffectWait,
  StepJoinWait,
  StepStoreWait,
  WorkflowResult,
} from "../base/step";
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
   */
  run = async (
    event: WorkflowEvent,
    options: DeliveryOptions & { logger?: Logger }
  ): Promise<WorkflowRunOutcome> => {
    const { lease, signal, cancellation, logger = this.logger } = options;
    const workflow = this.router[event.workflowId];

    if (!workflow) {
      return {
        status: "failed",
        error: new Error(`No workflow registered for "${event.workflowId}"`),
      };
    }

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
  };

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
      "Workflow runner critical error. This is likely because your worklow and worker are referencing different versions of @notation/durable."
    );
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
