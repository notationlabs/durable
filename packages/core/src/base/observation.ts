import { isTerminalExecutionStatus } from "./execution";
import type {
  ExecutionCancellation,
  ExecutionDivergence,
  ExecutionListRequest,
  ExecutionParent,
  ExecutionRecord,
} from "./execution";
import { isPrunedExecution } from "./retention";
import type { ExecutionTombstone, PrunedExecutionRecord } from "./retention";

export type ExecutionViewBase = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
  /** Present on executions started by `step.start` or `step.call`. */
  parent?: ExecutionParent;
  /** Present once a cancellation request is recorded. */
  cancellation?: ExecutionCancellation;
  /** Present once a delivery has recorded the workflow version. */
  workflowVersion?: number;
  /** Present while the execution is parked after a divergent replay. */
  divergence?: ExecutionDivergence;
};

export type ActiveExecutionView = ExecutionViewBase & {
  status: "admitted" | "running" | "suspended";
};

export type TerminalExecutionView =
  | (ExecutionViewBase & { status: "completed"; result: unknown })
  | (ExecutionViewBase & { status: "failed"; error: Error })
  | (ExecutionViewBase & { status: "cancelled"; reason?: string; source?: string });

/** What an observer sees of a pruned execution: its tombstone. */
export type PrunedExecutionView = ExecutionTombstone & { status: "pruned" };

/**
 * What an observer sees of an execution record. It leaves out `params`,
 * `context`, lease and wake state and `unacknowledgedDeliveryCount`;
 * operators read those through `ExecutionClient.get`.
 */
export type ExecutionView = ActiveExecutionView | TerminalExecutionView | PrunedExecutionView;

export type WaitResult =
  | TerminalExecutionView
  | PrunedExecutionView
  | { status: "timed-out"; execution: ActiveExecutionView };

export type ExecutionWaitOptions = {
  /**
   * Milliseconds to wait for a terminal outcome. Without one, `wait` waits
   * until the execution is terminal.
   */
  timeout?: number;
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
  | TerminalExecutionView
  | PrunedExecutionView;

export type ClearDivergenceResult = { status: "cleared" } | { status: "not-diverged" };

export interface ExecutionControl {
  /**
   * Records a cancellation request and returns without waiting for the
   * workflow to stop. Resolves with the terminal view when the execution is
   * terminal, including after a forced cancel, with the pruned view of a
   * pruned execution, which is not changed, and with `undefined` for an
   * unknown ID.
   */
  cancel(executionId: string, options?: CancelOptions): Promise<CancelResult | undefined>;

  /**
   * Removes a diverged execution's `divergence` and makes it due now, so the
   * next delivery replays it on the code deployed by then. Resolves with
   * `not-diverged` when there is nothing to clear, including for a pruned
   * execution, and with `undefined` for an unknown ID.
   */
  clearDivergence(executionId: string): Promise<ClearDivergenceResult | undefined>;
}

/** Reads executions from their durable record, from any process. */
export interface ExecutionObserver {
  /**
   * The current view of the record, the pruned view of a pruned execution,
   * or `undefined` if none exists.
   */
  poll(executionId: string): Promise<ExecutionView | undefined>;

  /**
   * Reads the record, and then waits for a terminal outcome. Resolves with
   * the outcome, or with `timed-out` and the current view when `timeout`
   * passes first. Resolves at once with the pruned view of a pruned
   * execution, and with `undefined` for an unknown ID.
   */
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;

  /**
   * Pages through executions, newest first by admission sequence. Pruned
   * executions are left out unless `status` is `pruned`.
   */
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
}

export function toExecutionView(
  record: ExecutionRecord | PrunedExecutionRecord
): ExecutionView {
  if (isPrunedExecution(record)) {
    return {
      status: "pruned",
      executionId: record.executionId,
      workflowId: record.workflowId,
      terminalStatus: record.terminalStatus,
      prunedAt: record.prunedAt,
    };
  }
  const base: ExecutionViewBase = {
    executionId: record.executionId,
    workflowId: record.workflowId,
    admittedAt: record.admittedAt,
    updatedAt: record.updatedAt,
    ...(record.parent ? { parent: { ...record.parent } } : {}),
    ...(record.cancellation ? { cancellation: { ...record.cancellation } } : {}),
    ...(record.workflowVersion !== undefined ? { workflowVersion: record.workflowVersion } : {}),
    ...(record.divergence ? { divergence: { ...record.divergence } } : {}),
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

export function isPrunedExecutionView(view: ExecutionView): view is PrunedExecutionView {
  return view.status === "pruned";
}
