import {
  prepareJoinWaiter,
  readJoinTarget,
  type JoinClient,
  type JoinResult,
  type JoinWaiter,
} from "@yieldstar/core";
import type { MemoryExecutionClient } from "./memory-execution";

/**
 * In-memory join waiters. `join` reads the target and registers in one
 * synchronous step, and the execution client calls `wakeWaiters` synchronously
 * inside a terminal acknowledgement, so neither can interleave with the other.
 */
export class MemoryJoinClient implements JoinClient {
  /** Keyed by target execution ID, joining execution ID and step key. */
  private waiters = new Map<string, JoinWaiter>();
  private executions: Pick<MemoryExecutionClient, "getState" | "wakeNow">;

  constructor(params: {
    executions: Pick<MemoryExecutionClient, "getState" | "wakeNow">;
  }) {
    this.executions = params.executions;
  }

  async join(request: JoinWaiter): Promise<JoinResult> {
    const waiter = prepareJoinWaiter(request);
    const result = readJoinTarget(this.executions.getState(waiter.targetExecutionId));
    if (result.status === "waiting") this.waiters.set(waiterKey(waiter), waiter);
    return result;
  }

  /**
   * The waiters registered on the target, ordered by joining execution and
   * then step key. Not part of `JoinClient`; for tests.
   */
  getWaiters(targetExecutionId: string): JoinWaiter[] {
    return this.waitersOn(targetExecutionId)
      .map((waiter) => ({ ...waiter }))
      .sort((a, b) => compare(a.executionId, b.executionId) || compare(a.stepKey, b.stepKey));
  }

  /**
   * Requests a wake for every waiter on the target and keeps the waiters.
   * Called synchronously by a terminal acknowledgement of the target.
   */
  wakeWaiters(targetExecutionId: string) {
    for (const waiter of this.waitersOn(targetExecutionId)) {
      this.executions.wakeNow(waiter.executionId);
    }
  }

  /**
   * Deletes the waiters a step registered. Called synchronously when the heap
   * records the step's result, so the waiter goes with that write.
   */
  deleteWaitersForStep(step: { executionId: string; stepKey: string }) {
    for (const [key, waiter] of this.waiters) {
      if (waiter.executionId === step.executionId && waiter.stepKey === step.stepKey) {
        this.waiters.delete(key);
      }
    }
  }

  private waitersOn(targetExecutionId: string) {
    return [...this.waiters.values()].filter(
      (waiter) => waiter.targetExecutionId === targetExecutionId
    );
  }
}

function waiterKey(waiter: JoinWaiter) {
  return JSON.stringify([waiter.targetExecutionId, waiter.executionId, waiter.stepKey]);
}

// Code unit order, as SQLite's binary collation.
function compare(a: string, b: string) {
  return a < b ? -1 : a > b ? 1 : 0;
}
