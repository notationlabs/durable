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
  | { status: "not-found" };

/**
 * Join waiters, keyed by target, joining execution and step key.
 *
 * A terminal `ExecutionClient.acknowledge` requests a wake for every join
 * waiter on its execution in the same atomic write as the outcome, and leaves
 * the waiters in place. A heap write that records a step's result deletes that
 * step's join waiters in the same write. No other method deletes a waiter.
 *
 * So a waiter stays registered indefinitely when no step result follows it:
 * a stale holder registers after the step was recorded, or a deploy removes a
 * join step that was still waiting. The only effect is one extra delivery of
 * the joiner when the target finishes, and `getWaiters` may list the waiter.
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
}

export type WorkflowJoinErrorCode =
  | "JOINED_EXECUTION_FAILED"
  | "JOIN_TARGET_NOT_FOUND"
  | "JOIN_SELF";

/**
 * Thrown to workflow code by `step.join` and `step.call`. It is recorded as
 * the step's error, so replay throws it again with the same `code` and
 * `executionId`.
 * - `JOINED_EXECUTION_FAILED`: the target failed. `cause` is its recorded
 *   error, with at least its `name` and `message`.
 * - `JOIN_TARGET_NOT_FOUND`: no execution has the target ID.
 * - `JOIN_SELF`: the target is the calling execution.
 */
export class WorkflowJoinError extends Error {
  readonly code: WorkflowJoinErrorCode;
  /** The joined execution. */
  readonly executionId: string;
  declare readonly cause?: Error;

  constructor(
    code: WorkflowJoinErrorCode,
    executionId: string,
    message: string,
    options?: { cause?: Error }
  ) {
    super(message, options);
    this.name = "WorkflowJoinError";
    this.code = code;
    this.executionId = executionId;
  }
}
