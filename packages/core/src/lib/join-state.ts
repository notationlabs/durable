import { isTerminalExecutionStatus } from "../base/execution";
import type { JoinResult } from "../base/join";
import { decodeExecutionOutcome, type ExecutionState } from "./execution-state";

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
