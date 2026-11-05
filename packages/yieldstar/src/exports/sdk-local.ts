import type {
  EventParamsOf,
  ExecutionClient,
  ExecutionListRequest,
  ExecutionObserver,
  ExecutionView,
  ExecutionViewPage,
  ExecutionWaitOptions,
  TriggerAck,
  TriggerEvent,
  WaitResult,
  WorkflowGeneratorReturnType,
  WorkflowInvoker,
  WorkflowRouter,
} from "@yieldstar/core";
import { ExecutionAdmissionError, createExecutionObserver } from "@yieldstar/core";

export function createLocalSdk<W extends WorkflowRouter>(
  runtime: { executions: ExecutionClient },
  options?: {
    /**
     * Optional. When the dispatcher runs in this process, `wait` listens to
     * its `workflowEndEmitter` and reads the record as soon as an outcome is
     * recorded. Results do not depend on it.
     */
    invoker?: Pick<WorkflowInvoker, "workflowEndEmitter">;
    /**
     * Milliseconds between reads of the execution record in `wait`.
     * @default 25
     */
    pollInterval?: number;
  }
) {
  const { executions } = runtime;
  const observer = createExecutionObserver(executions, {
    pollInterval: options?.pollInterval,
    notifier: options?.invoker?.workflowEndEmitter,
  });

  const sdk = {
    /**
     * Admits an execution and resolves once admission commits. A dispatcher
     * delivers it; this does not wait for a worker to start. With
     * `idempotent: true`, a repeat of the same request resolves with
     * `status: "existing"` and admits nothing.
     * @throws {ExecutionAdmissionError} when the ID is in use (in idempotent
     * mode, by a different request), an idempotent trigger has no ID, or the
     * event cannot be stored without loss
     */
    async trigger<K extends string & keyof W>(
      event: TriggerEvent<K, EventParamsOf<W[K]>>
    ): Promise<TriggerAck> {
      const admission = await executions.admit({
        executionId: event.executionId,
        workflowId: event.workflowId,
        params: event.params,
        context: new Map(Object.entries(event.context ?? {})),
        idempotent: event.idempotent,
      });
      if (admission.status === "rejected") {
        throw new ExecutionAdmissionError(admission.code, admission.message);
      }
      return { executionId: admission.executionId, status: admission.status };
    },

    /**
     * Admits an execution and waits, without a timeout, for its record to
     * reach a terminal status. Resolves with the result, or rejects with the
     * recorded error.
     */
    async triggerAndWait<K extends string & keyof W>(
      event: TriggerEvent<K, EventParamsOf<W[K]>>
    ): Promise<WorkflowGeneratorReturnType<W[K]>> {
      const { executionId } = await sdk.trigger(event);
      const result = await observer.wait(executionId);
      if (!result) {
        throw new Error(`Execution "${executionId}" has no record`);
      }
      if (result.status === "failed") throw result.error;
      if (result.status === "timed-out") {
        throw new Error(`Execution "${executionId}" is still ${result.execution.status}`);
      }
      return result.result as WorkflowGeneratorReturnType<W[K]>;
    },

    /** The current view of an execution, or `undefined` if none exists. */
    poll(executionId: string): Promise<ExecutionView | undefined> {
      return observer.poll(executionId);
    },

    /**
     * Reads the execution record, and then waits for a terminal outcome.
     * Resolves with `timed-out` and the current view when `timeout` passes
     * first, and with `undefined` at once for an unknown ID.
     * @throws {RangeError} for a negative timeout
     */
    wait(
      executionId: string,
      options?: ExecutionWaitOptions
    ): Promise<WaitResult | undefined> {
      return observer.wait(executionId, options);
    },

    /**
     * Pages through executions, newest first by admission.
     * @throws {ExecutionListRequestError} for an invalid limit, status or cursor
     */
    list(request?: ExecutionListRequest): Promise<ExecutionViewPage> {
      return observer.list(request);
    },
  };
  return sdk satisfies ExecutionObserver;
}
