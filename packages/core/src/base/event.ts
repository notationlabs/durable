import { FreezableMap } from "../utils/map";

export type EventParams = Record<string, any> | void;
export type EventContext = ReadonlyMap<any, any>;
export type MiddlewareEventContext = FreezableMap<any, any>;

export type TriggerEvent<
  WorkflowId extends string,
  Params extends EventParams = EventParams
> = Params extends void
  ? {
      workflowId: WorkflowId;
      executionId?: string;
      params?: undefined;
      context?: Record<string, any>;
      /**
       * Makes a repeat of the trigger safe. When `executionId` is in use,
       * the trigger returns `existing` if the workflow ID and params match,
       * and fails with `EXECUTION_REQUEST_MISMATCH` otherwise. Requires
       * `executionId`.
       */
      idempotent?: boolean;
    }
  : {
      workflowId: WorkflowId;
      executionId?: string;
      params: Params;
      context?: Record<string, any>;
      /**
       * Makes a repeat of the trigger safe. When `executionId` is in use,
       * the trigger returns `existing` if the workflow ID and params match,
       * and fails with `EXECUTION_REQUEST_MISMATCH` otherwise. Requires
       * `executionId`.
       */
      idempotent?: boolean;
    };

/**
 * The answer to a trigger once admission commits. `existing` means an
 * idempotent trigger matched an execution already admitted with the same
 * request, and admitted nothing.
 */
export type TriggerAck = {
  executionId: string;
  status: "accepted" | "existing";
};

export type ExecutionEvent<Params extends EventParams = EventParams> = {
  workflowId: string;
  executionId: string;
  params: Params;
  context?: Record<string, any>;
};

export type WorkflowEvent<
  Params extends EventParams = EventParams,
  Context extends ReadonlyMap<any, any> = ReadonlyMap<any, any>
> = {
  workflowId: string;
  executionId: string;
  params: Params;
  context: Context;
};

/**
 * The event a trigger route passes through middleware before admission.
 * `executionId` is absent when the caller lets the runtime choose one.
 */
export type MiddlewareEvent<
  Params extends EventParams = EventParams,
  Context extends MiddlewareEventContext = MiddlewareEventContext
> = Omit<ExecutionEvent<Params>, "executionId" | "context"> & {
  executionId?: string;
  context: Context;
};
