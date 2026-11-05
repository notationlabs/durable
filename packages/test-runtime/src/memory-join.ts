import {
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

  async join(waiter: JoinWaiter): Promise<JoinResult> {
    const result = readJoinTarget(this.executions.getState(waiter.targetExecutionId));
    if (result.status === "waiting") this.waiters.set(waiterKey(waiter), waiter);
    return result;
  }

  /**
   * Requests a wake for every waiter on the target and keeps the waiters.
   * Called synchronously by a terminal acknowledgement of the target.
   */
  wakeWaiters(targetExecutionId: string) {
    for (const waiter of this.waiters.values()) {
      if (waiter.targetExecutionId === targetExecutionId) {
        this.executions.wakeNow(waiter.executionId);
      }
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
}

function waiterKey(waiter: JoinWaiter) {
  return JSON.stringify([waiter.targetExecutionId, waiter.executionId, waiter.stepKey]);
}

