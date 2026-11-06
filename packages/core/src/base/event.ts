import { FreezableMap } from "../utils/map";

export type EventParams = Record<string, any> | void;
export type EventContext = ReadonlyMap<any, any>;
export type MiddlewareEventContext = FreezableMap<any, any>;

export type TriggerEvent<
  WorkflowId extends string,
  Params extends EventParams = EventParams
> = (Params extends void
  ? { workflowId: WorkflowId; params?: undefined; context?: Record<string, any> }
  : { workflowId: WorkflowId; params: Params; context?: Record<string, any> }) &
  TriggerIdempotency;

/**
 * `idempotent: true` requires `executionId`, so omitting it is a type error.
 * Admission still rejects it at runtime with `IDEMPOTENT_TRIGGER_REQUIRES_ID`
 * for callers without these types, such as HTTP clients.
 */
type TriggerIdempotency =
  | { executionId?: string; idempotent?: false }
  | {
      executionId: string;
      /**
       * Makes a repeat of the trigger safe. When `executionId` is in use,
       * the trigger returns `existing` if the workflow ID and params match,
       * and fails with `EXECUTION_REQUEST_MISMATCH` otherwise.
       */
      idempotent: true;
    };

/**
 * The response to a trigger after admission commits. `existing` means an
 * idempotent trigger matched an earlier execution with the same request, and
 * nothing was admitted.
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
  /**
   * The version recorded by the execution's first delivery, whatever version
   * the current code declares. 1 for a workflow that declares none.
   */
  workflowVersion: number;
};

/**
 * The event a delivery is dispatched with. The driver adds `workflowVersion`:
 * the recorded one, or the version the workflow declares.
 */
export type DeliveryEvent<
  Params extends EventParams = EventParams,
  Context extends ReadonlyMap<any, any> = ReadonlyMap<any, any>
> = Omit<WorkflowEvent<Params, Context>, "workflowVersion">;

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
