import type { ExecutionOutcome } from "./execution";

/** A step of one execution waiting for another execution to finish. */
export type JoinWaiter = {
  /** The execution being joined. */
  targetExecutionId: string;
  /** The joining execution. */
  executionId: string;
  stepKey: string;
};

export type JoinResult =
  /** The target was terminal when read. No waiter was registered. */
  | { status: "terminal"; outcome: ExecutionOutcome }
  /** The target is not terminal. The waiter is registered. */
  | { status: "waiting" }
  /** No execution has the target ID. No waiter was registered. */
  | { status: "not-found" }
  /** The target was pruned, so its outcome is gone. No waiter was registered. */
  | { status: "pruned" };

/**
 * Join waiters, keyed by target, joining execution and step key.
 *
 * A terminal `ExecutionClient.acknowledge` requests a wake for every join
 * waiter on its execution in the same atomic write as the outcome, and leaves
 * the waiters in place. A heap write that records a step's result deletes that
 * step's join waiters in the same write. A forced cancel deletes the waiters
 * its execution holds.
 *
 * So a waiter can stay registered for good when no step result follows it:
 * a stale holder registers after the step was recorded, or a deploy removes a
 * join step that was still waiting. The only effect is one extra delivery of
 * the joiner when the target finishes, and `getWaiters` may list the waiter.
 *
 * A target is not pruned while a waiter whose joiner is not terminal is
 * registered on it. Waiters whose joiner is terminal or missing do not hold
 * it back, since those joiners never read the target again. The tombstone
 * write deletes the waiters registered on the target, and the cleanup of a
 * pruned joiner deletes the waiters it holds.
 */
export interface JoinClient {
  /**
   * Reads the target's record and, if it is not terminal, registers the
   * waiter, in one atomic step. So every waiter on a terminal target has a
   * pending or delivered wake for its joiner. Repeating it keeps one
   * registration. Not fenced: a stale holder's registration causes at most
   * one extra delivery.
   */
  join(waiter: JoinWaiter): Promise<JoinResult>;

  /** The waiters registered on the target, for operators and tests. */
  getWaiters(targetExecutionId: string): Promise<JoinWaiter[]>;

  /**
   * Deletes every waiter the execution registered as a joiner, for a pruned
   * execution. Safe to repeat.
   */
  deleteWaitersForExecution(executionId: string): Promise<void>;
}

export type WorkflowJoinErrorCode =
  | "JOINED_EXECUTION_FAILED"
  | "JOINED_EXECUTION_CANCELLED"
  | "JOIN_TARGET_NOT_FOUND"
  | "JOIN_TARGET_PRUNED"
  | "JOIN_SELF";

/**
 * Thrown to workflow code by `step.join` and `step.call`. It is recorded as
 * the step's error, so replay throws it again with the same `code` and
 * `executionId`.
 * - `JOINED_EXECUTION_FAILED`: the target failed. `cause` is its recorded
 *   error, with at least its `name` and `message`.
 * - `JOINED_EXECUTION_CANCELLED`: the target was cancelled. `reason` and
 *   `source` are its outcome's.
 * - `JOIN_TARGET_NOT_FOUND`: no execution has the target ID.
 * - `JOIN_TARGET_PRUNED`: the target was pruned before the join read it.
 * - `JOIN_SELF`: the target is the calling execution.
 */
export class WorkflowJoinError extends Error {
  readonly code: WorkflowJoinErrorCode;
  /** The joined execution. */
  readonly executionId: string;
  /** The cancelled target's reason, for `JOINED_EXECUTION_CANCELLED`. */
  readonly reason?: string;
  /** The cancelled target's source, for `JOINED_EXECUTION_CANCELLED`. */
  readonly source?: string;
  declare readonly cause?: Error;

  constructor(
    code: WorkflowJoinErrorCode,
    executionId: string,
    message: string,
    options?: { cause?: Error; reason?: string; source?: string }
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "WorkflowJoinError";
    this.code = code;
    this.executionId = executionId;
    if (options?.reason !== undefined) this.reason = options.reason;
    if (options?.source !== undefined) this.source = options.source;
  }
}
