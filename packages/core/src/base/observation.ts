import type { ExecutionListRequest, ExecutionRecord } from "./execution";

type ExecutionViewBase = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
};

export type ActiveExecutionView = ExecutionViewBase & {
  status: "admitted" | "running" | "suspended";
};

export type TerminalExecutionView =
  | (ExecutionViewBase & { status: "completed"; result: unknown })
  | (ExecutionViewBase & { status: "failed"; error: Error });

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
  };
  if (record.status === "completed" || record.status === "failed") {
    const outcome = record.outcome;
    if (outcome?.status !== record.status) {
      throw new Error(
        `Terminal execution "${record.executionId}" has no ${record.status} outcome`
      );
    }
    return outcome.status === "completed"
      ? { ...base, status: "completed", result: outcome.result }
      : { ...base, status: "failed", error: outcome.error };
  }
  return { ...base, status: record.status };
}

export function isTerminalExecutionView(
  view: ExecutionView
): view is TerminalExecutionView {
  return view.status === "completed" || view.status === "failed";
}
