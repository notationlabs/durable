import { isTerminalExecutionStatus } from "./execution";
import type {
  ExecutionCancellation,
  ExecutionListRequest,
  ExecutionParent,
  ExecutionRecord,
} from "./execution";

type ExecutionViewBase = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
  /** Present on executions started by `step.start` or `step.call`. */
  parent?: ExecutionParent;
  /** Present once a cancellation request is recorded. */
  cancellation?: ExecutionCancellation;
};

export type ActiveExecutionView = ExecutionViewBase & {
  status: "admitted" | "running" | "suspended";
};

export type TerminalExecutionView =
  | (ExecutionViewBase & { status: "completed"; result: unknown })
  | (ExecutionViewBase & { status: "failed"; error: Error })
  | (ExecutionViewBase & { status: "cancelled"; reason?: string; source?: string });

/**
 * What an observer sees of an execution record. It leaves out `params`,
 * `context`, lease and wake state and `unacknowledgedDeliveryCount`;
 * operators read those through `ExecutionClient.get`.
 */
export type ExecutionView = ActiveExecutionView | TerminalExecutionView;

export type WaitResult =
  | TerminalExecutionView
  | { status: "timed-out"; execution: ActiveExecutionView };

export type ExecutionWaitOptions = {
  /**
   * Milliseconds to wait for a terminal outcome. Without one, `wait` waits
   * until the execution is terminal.
   */
  timeout?: number;
  /**
   * Ends the wait. `wait` stops reading the record and rejects with the
   * signal's reason.
   */
  signal?: AbortSignal;
};

export type ExecutionViewPage = {
  executions: ExecutionView[];
  /** Present when more executions may follow. */
  cursor?: string;
};

export type CancelOptions = {
  reason?: string;
  /** Records `cancelled` at once, without running workflow code. */
  force?: boolean;
};

export type CancelResult =
  | { status: "requested"; execution: ActiveExecutionView }
  | TerminalExecutionView;

export interface ExecutionControl {
  /**
   * Requests cancellation without waiting for the workflow to stop. Resolves
   * `undefined` for an unknown ID.
   */
  cancel(executionId: string, options?: CancelOptions): Promise<CancelResult | undefined>;
}

/** Reads executions from their durable record, from any process. */
export interface ExecutionObserver {
  /** The current view of the record, or `undefined` if none exists. */
  poll(executionId: string): Promise<ExecutionView | undefined>;

  /**
   * Reads the record, and then waits for a terminal outcome. Resolves with
   * the outcome, or with `timed-out` and the current view when `timeout`
   * passes first. Resolves with `undefined` at once for an unknown ID.
   * Rejects with the signal's reason once `signal` aborts.
   * @throws {RangeError} for a negative timeout
   */
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;

  /**
   * Pages through executions, newest first by admission sequence.
   * @throws {ExecutionListRequestError} for an invalid limit, status or cursor
   */
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
}

export function toExecutionView(record: ExecutionRecord): ExecutionView {
  const base: ExecutionViewBase = {
    executionId: record.executionId,
    workflowId: record.workflowId,
    admittedAt: record.admittedAt,
    updatedAt: record.updatedAt,
    ...(record.parent ? { parent: { ...record.parent } } : {}),
    ...(record.cancellation ? { cancellation: { ...record.cancellation } } : {}),
  };
  if (isTerminalExecutionStatus(record.status)) {
    const outcome = record.outcome;
    if (outcome?.status !== record.status) {
      throw new Error(
        `Terminal execution "${record.executionId}" has no ${record.status} outcome`
      );
    }
    switch (outcome.status) {
      case "completed":
        return { ...base, status: "completed", result: outcome.result };
      case "failed":
        return { ...base, status: "failed", error: outcome.error };
      case "cancelled":
        return {
          ...base,
          status: "cancelled",
          ...(outcome.reason !== undefined ? { reason: outcome.reason } : {}),
          ...(outcome.source !== undefined ? { source: outcome.source } : {}),
        };
    }
  }
  return { ...base, status: record.status };
}

export function isTerminalExecutionView(
  view: ExecutionView
): view is TerminalExecutionView {
  return isTerminalExecutionStatus(view.status);
}
