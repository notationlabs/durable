import type { DurableObject } from "./durable-value";
import type { EventContext, EventParams } from "./event";
import type { PrunedExecutionRecord, RetentionOptions } from "./retention";

export type ExecutionStatus =
  | "admitted"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export type ExecutionOutcome =
  | { status: "completed"; result: unknown }
  | { status: "failed"; error: Error }
  | {
      status: "cancelled";
      reason?: string;
      /** The execution where the cancellation began, when it was propagated. */
      source?: string;
    };

/**
 * The execution and step that started an execution with `step.start` or
 * `step.call`. A `detached` child receives no cancellation propagated from
 * its parent.
 */
export type ExecutionParent = {
  executionId: string;
  stepKey: string;
  detached: boolean;
};

/**
 * A cancellation request recorded on an execution. It stays on the record
 * whatever the outcome.
 */
export type ExecutionCancellation = {
  reason?: string;
  /** The execution where the cancellation began, when it was propagated. */
  source?: string;
  forced: boolean;
  requestedAt: number;
  /**
   * Set when the throw into workflow code is recorded. Until then the request
   * is pending; after it, the workflow is in cleanup.
   */
  deliveredAt?: number;
};

export type ExecutionDivergenceCode = "NONDETERMINISTIC_REPLAY" | "STEP_SCHEMA_CHANGED";

/**
 * Set on an execution when a delivery found that the workflow code no longer
 * matches the recorded history. While it is set, wakes stay pending but do
 * not deliver the execution. `clearDivergence` removes it, and so does a
 * terminal outcome.
 */
export type ExecutionDivergence = {
  code: ExecutionDivergenceCode;
  /** The step that would have run, when there was one. */
  stepKey?: string;
  /** The workflow version recorded on the execution. */
  workflowVersion: number;
  detectedAt: number;
};

/**
 * The durable record of one workflow execution. Admission creates it and
 * stores the event once; every delivery reads the event from here.
 *
 * - `admitted`: no delivery has been acknowledged yet.
 * - `running`: the execution has a live lease. When the lease ends without a
 *   terminal acknowledgement, the status returns to `suspended` if any
 *   earlier delivery was acknowledged as suspended, and to `admitted`
 *   otherwise.
 * - `suspended`: the latest acknowledged delivery suspended the workflow.
 * - `completed`, `failed` and `cancelled` are terminal. `outcome` is present
 *   only in these states.
 *
 * `parent` is present, and immutable, only on executions admitted by `start`.
 * `cancellation` is present once `cancel` has recorded a request.
 * `workflowVersion` is present once a delivery has made its first fenced
 * write. `divergence` is present while the execution is parked after a
 * divergent replay.
 */
export interface ExecutionRecord {
  executionId: string;
  workflowId: string;
  params: EventParams;
  context: EventContext;
  status: ExecutionStatus;
  outcome?: ExecutionOutcome;
  admittedAt: number;
  updatedAt: number;
  /** Consecutive deliveries that ended without an acknowledgement. */
  unacknowledgedDeliveryCount: number;
  parent?: ExecutionParent;
  cancellation?: ExecutionCancellation;
  workflowVersion?: number;
  divergence?: ExecutionDivergence;
}

export type ExecutionAdmissionErrorCode =
  | "EXECUTION_ID_CONFLICT"
  /** An idempotent admission found the ID pruned, so it cannot compare params. */
  | "EXECUTION_PRUNED"
  | "INVALID_EXECUTION_EVENT"
  | "EXECUTION_REQUEST_MISMATCH"
  | "IDEMPOTENT_TRIGGER_REQUIRES_ID";

/**
 * A lease on one execution, granted by `claimDue`. `token` is a fencing token:
 * every claim of the execution issues a token greater than every earlier one,
 * and only the current token's fenced writes are accepted. It also identifies
 * the wakes the delivery covers. `expiresAt` is read from the runtime clock;
 * clocks decide only when the execution becomes claimable again, never whose
 * writes count.
 */
export type ExecutionLease = {
  executionId: string;
  token: number;
  expiresAt: number;
};

/**
 * Limits a claim to the executions that match. The claim applies it in the same
 * atomic step, so an execution that does not match is not leased and gains no
 * redelivery delay.
 */
export type ExecutionClaimFilter = {
  /** Claims only executions whose ID is in this list. An empty list claims nothing. */
  executionIds?: readonly string[];
};

/**
 * Returned by a fenced write whose lease is no longer current: a higher token
 * has been issued, or the lease has ended. The write changed nothing, and its
 * holder must stop.
 */
export type LeaseLost = { status: "lost" };

export function isLeaseLost(value: unknown): value is LeaseLost {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { status?: unknown }).status === "lost"
  );
}

/**
 * What a delivery reports once its durable work is finished:
 * - `completed`: the workflow returned a result;
 * - `suspended`: the unfinished step is recorded, and `wakeAt`, when present,
 *   is the earliest time a paused step needs the execution to run again;
 * - `failed`: workflow code threw an uncaught error;
 * - `cancelled`: the delivery carried a cancellation request and workflow
 *   code threw `WorkflowCancelledError`. The acknowledgement records the
 *   request's `reason` and `source` as the outcome.
 *
 * - `diverged`: replay no longer matches the recorded history. Nothing more
 *   was written. The acknowledgement records `divergence` and parks the
 *   execution instead of recording an outcome.
 *
 * `workflowVersion` is the version the delivery exposed. The acknowledgement
 * records it when the execution has none.
 *
 * Infrastructure errors are never outcomes: the delivery rejects instead.
 */
export type WorkflowRunOutcome = (
  | { status: "completed"; result: unknown }
  | { status: "suspended"; wakeAt?: number }
  | { status: "failed"; error: Error }
  | { status: "cancelled" }
  | { status: "diverged"; code: ExecutionDivergenceCode; stepKey?: string }
) & { workflowVersion?: number };

export type ExecutionAdmissionRequest = {
  executionId?: string;
  workflowId: string;
  params: EventParams;
  context: EventContext;
  /**
   * Makes a repeat of the request safe. When the ID is already in use, the
   * admission returns `existing` if the recorded workflow ID and canonical
   * params match, and `EXECUTION_REQUEST_MISMATCH` otherwise. Context is not
   * compared. Requires `executionId`. A pruned ID returns `EXECUTION_PRUNED`.
   */
  idempotent?: boolean;
};

export type ExecutionAdmissionResult =
  | { status: "accepted"; executionId: string }
  /** An idempotent repeat: nothing was admitted. */
  | { status: "existing"; executionId: string }
  | {
      status: "rejected";
      code: ExecutionAdmissionErrorCode;
      message: string;
    };

export type ExecutionListRequest = {
  workflowId?: string;
  /**
   * Matched against the status at the runtime clock's current time. Without
   * it, pruned executions are left out; `pruned` returns only them.
   */
  status?: ExecutionStatus | "pruned";
  /** `true` returns only diverged executions, and `false` only the others. */
  diverged?: boolean;
  /** Returned by the previous page. */
  cursor?: string;
  /** @default 50, at most 1000 */
  limit?: number;
};

export type ExecutionListPage = {
  executions: (ExecutionRecord | PrunedExecutionRecord)[];
  /** Present when more executions may follow. */
  cursor?: string;
};

export type WorkflowStartErrorCode =
  | "INVALID_START_PARAMETERS"
  | "START_REQUEST_MISMATCH";

export type ExecutionStartRequest = {
  /** Identifies the request: the calling execution and its step key. */
  requestId: { executionId: string; stepKey: string };
  workflowId: string;
  params?: DurableObject;
  /**
   * Records the parent link as detached, so the target receives no
   * cancellation propagated from the caller. Compared on a repeat.
   * @default false
   */
  detached?: boolean;
};

export type ExecutionStartResult =
  | { status: "accepted"; executionId: string }
  | {
      status: "rejected";
      code: WorkflowStartErrorCode;
      message: string;
    };

export type ExecutionWakeResult =
  | { status: "requested" }
  | { status: "ignored" }
  | {
      status: "rejected";
      code: "EXECUTION_NOT_FOUND";
      message: string;
    };

export type ExecutionCancelRequest = {
  executionId: string;
  reason?: string;
  /** Records `cancelled` at once, without running workflow code. */
  force?: boolean;
};

export type ExecutionCancelResult =
  /** The request is recorded and the execution is not terminal. */
  | { status: "requested" }
  /** The execution is terminal: a forced cancel's outcome, or the outcome it already had. */
  | ExecutionOutcome
  /** The execution was pruned. Nothing changed. */
  | PrunedExecutionRecord
  | {
      status: "rejected";
      code: "EXECUTION_NOT_FOUND";
      message: string;
    };

export type ExecutionClearDivergenceResult =
  /** `divergence` was removed and the execution is due now. */
  | { status: "cleared" }
  /** The execution has no divergence; nothing changed. */
  | { status: "not-diverged" }
  | {
      status: "rejected";
      code: "EXECUTION_NOT_FOUND";
      message: string;
    };

export type ExecutionAcknowledgeResult =
  | { status: "suspended" }
  | { status: "diverged" }
  | ExecutionOutcome
  | LeaseLost;

export type ExecutionRenewResult =
  | { status: "renewed"; lease: ExecutionLease }
  | LeaseLost;

export type ExecutionReleaseResult = { status: "released" } | LeaseLost;

export interface ExecutionClient {
  /**
   * Creates the execution record with status `admitted` and makes it due
   * immediately, all-or-nothing. Execution IDs are never reused.
   */
  admit(request: ExecutionAdmissionRequest): Promise<ExecutionAdmissionResult>;

  /** The record, the tombstone of a pruned execution, or undefined for an unknown ID. */
  get(executionId: string): Promise<ExecutionRecord | PrunedExecutionRecord | undefined>;

  /**
   * Pages through execution records, newest first by admission sequence. The
   * backend assigns the sequence, so it increases across all clients whatever
   * their clocks. A cursor resumes after the last execution of its page, so no
   * execution is returned twice, and executions admitted after the first page
   * do not appear on later pages.
   * @throws {ExecutionListRequestError} for an invalid filter, limit or cursor
   */
  list(request?: ExecutionListRequest): Promise<ExecutionListPage>;

  /**
   * Starts a workflow on behalf of the execution and step named by
   * `requestId`, idempotently by that ID. A new start admits the target with
   * the caller's context and a parent link to the caller, and records the
   * start request, all-or-nothing. A repeated request returns the first
   * target's ID if its workflow ID and canonical params match, and
   * `START_REQUEST_MISMATCH` otherwise, whatever the target's status. Invalid
   * params return `INVALID_START_PARAMETERS` and admit nothing. Not fenced.
   */
  start(request: ExecutionStartRequest): Promise<ExecutionStartResult>;

  /**
   * Records a cancellation request and propagates it to every non-terminal,
   * non-detached descendant, all-or-nothing. Not fenced: it needs no lease.
   *
   * A graceful request makes the execution due; its next delivery throws
   * `WorkflowCancelledError` at the first unfinished step. A forced request
   * records `cancelled` at once, clears the pending wake, deletes the waiters
   * the execution holds, wakes its join waiters and ends any live lease by
   * issuing a higher token.
   *
   * A repeated request keeps the first `reason` and `source`; a forced
   * request after a graceful one makes it forced. A terminal execution is
   * not changed, and its outcome is returned; a pruned one returns its
   * tombstone. An invalid request throws a
   * `TypeError` and changes nothing.
   */
  cancel(request: ExecutionCancelRequest): Promise<ExecutionCancelResult>;

  /**
   * Removes the execution's `divergence` and makes it due now, so the next
   * delivery replays it again on the code deployed by then. Not fenced. An
   * execution without a divergence, including a terminal or pruned one, is
   * unchanged.
   */
  clearDivergence(executionId: string): Promise<ExecutionClearDivergenceResult>;

  /**
   * Requests a wake from outside a delivery, such as a store commit. Wakes not
   * covered by a claimed delivery merge into one due time: the earliest. A
   * terminal or pruned execution ignores it.
   */
  requestWake(request: {
    executionId: string;
    wakeAt: number;
  }): Promise<ExecutionWakeResult>;

  /**
   * Leases up to `limit` executions whose due time is at or before the runtime
   * clock and that have no live lease, earliest due first. Each lease lasts
   * `duration` milliseconds and carries a token greater than every earlier
   * token for its execution. An execution has at most one live lease at a
   * time, across every client sharing the backend.
   */
  claimDue(request: {
    limit: number;
    duration: number;
    /** When absent, any execution whose due time has passed may be claimed. */
    filter?: ExecutionClaimFilter;
  }): Promise<ExecutionLease[]>;

  /**
   * Extends a lease to `duration` milliseconds from now. Returns `lost` if a
   * higher token has been issued or the lease has ended. An expired lease
   * that has not been claimed again is still current.
   */
  renew(request: {
    lease: ExecutionLease;
    duration: number;
  }): Promise<ExecutionRenewResult>;

  /**
   * Ends a lease without acknowledging an outcome. The delivery counts as
   * unacknowledged, so the execution becomes claimable again only after the
   * redelivery delay. Returns `lost` for a stale lease.
   */
  release(request: { lease: ExecutionLease }): Promise<ExecutionReleaseResult>;

  /**
   * Records a delivery's outcome and ends its lease, all-or-nothing. A
   * `suspended` outcome clears the wakes the lease covered, leaving the
   * execution due at the earliest of `wakeAt` and any wake requested since the
   * claim. A `diverged` outcome records `divergence` and leaves those wakes
   * pending without delivering them; it does not count as an unacknowledged
   * delivery. A stale lease returns `lost` and changes nothing.
   */
  acknowledge(request: {
    lease: ExecutionLease;
    outcome: WorkflowRunOutcome;
  }): Promise<ExecutionAcknowledgeResult>;

  /**
   * Replaces up to `limit` expired terminal executions with their tombstones,
   * oldest outcome first, and returns the tombstones. An execution is expired
   * when the period of its workflow's policy for its status has passed since
   * the outcome was recorded, by the runtime clock. A target with a join
   * waiter whose joiner is not terminal is skipped.
   *
   * Each tombstone is one atomic write with the join-waiter check. It clears
   * the record's params, context, outcome, cancellation, parent link,
   * divergence, version and wake and lease state, deletes the join waiters
   * registered on the execution, and marks the tombstone as pending cleanup.
   * Safe to run concurrently: an execution is pruned once.
   */
  prune(request: {
    retain: RetentionOptions;
    limit: number;
  }): Promise<PrunedExecutionRecord[]>;

  /**
   * Up to `limit` tombstones still pending cleanup, with whether the
   * execution's stores are to be deleted.
   */
  listPendingPrunes(request: {
    limit: number;
  }): Promise<{ executionId: string; deleteExecutionStore: boolean }[]>;

  /**
   * Deletes the start-request records the pruned execution owns as caller and
   * clears its cleanup mark. Callers delete the execution's step records,
   * store data and waiters first. Safe to repeat; does nothing for an
   * execution that is not pruned.
   */
  finishPrune(executionId: string): Promise<void>;
}

/**
 * Thrown by the driver when a fenced write returns `lost`: another claim has
 * taken over the execution, so this delivery must stop without acknowledging.
 */
export class LeaseLostError extends Error {
  readonly executionId: string;

  constructor(executionId: string) {
    super(`Lease on execution "${executionId}" was lost`);
    this.name = "LeaseLostError";
    this.executionId = executionId;
  }
}

/**
 * Thrown by the driver when the current holder writes a step record that
 * already exists with a different value. The recorded value stands: the
 * delivery ends as a retry, and the next delivery replays the recorded value.
 */
export class StepConflictError extends Error {
  readonly executionId: string;
  readonly stepKey: string;

  constructor(executionId: string, stepKey: string) {
    super(
      `Step "${stepKey}" of execution "${executionId}" is already recorded with a different value`
    );
    this.name = "StepConflictError";
    this.executionId = executionId;
    this.stepKey = stepKey;
  }
}

export class ExecutionAdmissionError extends Error {
  readonly code: ExecutionAdmissionErrorCode;

  constructor(code: ExecutionAdmissionErrorCode, message: string) {
    super(message);
    this.name = "ExecutionAdmissionError";
    this.code = code;
  }
}

/** Thrown by `ExecutionClient.list` for an invalid filter, limit or cursor. */
export class ExecutionListRequestError extends Error {
  readonly code = "INVALID_LIST_REQUEST";

  constructor(message: string) {
    super(message);
    this.name = "ExecutionListRequestError";
  }
}

/**
 * Thrown to workflow code by `step.start` when the runtime rejects the start.
 * The rejection is recorded as the step's result, so replay throws it again.
 */
export class WorkflowStartError extends Error {
  readonly code: WorkflowStartErrorCode;

  constructor(code: WorkflowStartErrorCode, message: string) {
    super(message);
    this.name = "WorkflowStartError";
    this.code = code;
  }
}

const INFRASTRUCTURE_ERROR = Symbol.for("yieldstar.infrastructureError");

/**
 * Wraps a failure of the runtime rather than of workflow code, such as a heap
 * or store client rejecting. A delivery that hits one rejects without
 * resuming workflow code, and is not acknowledged.
 */
export class InfrastructureError extends Error {
  readonly [INFRASTRUCTURE_ERROR] = true;

  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = "InfrastructureError";
  }

  static is(value: unknown): value is InfrastructureError {
    return (
      typeof value === "object" &&
      value !== null &&
      (value as Record<symbol, unknown>)[INFRASTRUCTURE_ERROR] === true
    );
  }
}

const REPLAY_DIVERGENCE = Symbol.for("yieldstar.replayDivergence");

/**
 * Raised by the driver when replay no longer matches the recorded history.
 * It is not a workflow error: workflow code cannot catch it, and the runner
 * reports it as a `diverged` outcome instead of `failed`.
 */
export class ReplayDivergenceError extends Error {
  readonly [REPLAY_DIVERGENCE] = true;
  readonly code: ExecutionDivergenceCode;
  readonly stepKey?: string;

  constructor(code: ExecutionDivergenceCode, message: string, stepKey?: string) {
    super(message);
    this.name = "ReplayDivergenceError";
    this.code = code;
    if (stepKey !== undefined) this.stepKey = stepKey;
  }

  static is(value: unknown): value is ReplayDivergenceError {
    return (
      typeof value === "object" &&
      value !== null &&
      (value as Record<symbol, unknown>)[REPLAY_DIVERGENCE] === true
    );
  }
}

export function isTerminalExecutionStatus(
  status: ExecutionStatus
): status is "completed" | "failed" | "cancelled" {
  return status === "completed" || status === "failed" || status === "cancelled";
}

/**
 * Thrown into workflow code at the first unfinished step once a cancellation
 * request is delivered. The throw is recorded as that step's result, so
 * replay throws it again at the same step. A workflow that lets it escape
 * ends `cancelled`; one that catches it and returns ends `completed`.
 */
export class WorkflowCancelledError extends Error {
  readonly reason?: string;
  /** The execution whose cancellation caused this one, when propagated. */
  readonly source?: string;

  constructor(options?: { reason?: string; source?: string }) {
    super(
      options?.reason === undefined
        ? "Execution was cancelled"
        : `Execution was cancelled: ${options.reason}`
    );
    this.name = "WorkflowCancelledError";
    if (options?.reason !== undefined) this.reason = options.reason;
    if (options?.source !== undefined) this.source = options.source;
  }
}
