import { isTerminalExecutionStatus } from "../base/execution";
import type { JoinResult, JoinWaiter } from "../base/join";
import { decodeExecutionOutcome, type ExecutionState } from "./execution-state";

/**
 * Validates a join waiter and copies it. Adapters call this before their
 * atomic read-and-register.
 */
export function prepareJoinWaiter(waiter: JoinWaiter): JoinWaiter {
  for (const field of ["targetExecutionId", "executionId", "stepKey"] as const) {
    if (typeof waiter?.[field] !== "string" || waiter[field] === "") {
      throw new Error(`Invalid join waiter: ${field} must be a non-empty string`);
    }
  }
  return {
    targetExecutionId: waiter.targetExecutionId,
    executionId: waiter.executionId,
    stepKey: waiter.stepKey,
  };
}

/**
 * Answers a join from the target's stored state. Adapters call this in the
 * same atomic step as the registration, and register the waiter only when it
 * returns `waiting`.
 */
export function readJoinTarget(target: ExecutionState | undefined): JoinResult {
  if (!target) return { status: "not-found" };
  if (isTerminalExecutionStatus(target.status)) {
    return { status: "terminal", outcome: decodeExecutionOutcome(target.outcome!) };
  }
  return { status: "waiting" };
}
