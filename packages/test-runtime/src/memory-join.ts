import {
  compareJoinWaiters,
  isTerminalExecutionStatus,
  prepareJoinWaiter,
  readJoinTarget,
  type JoinClient,
  type JoinResult,
  type JoinWaiter,
} from "@yieldstar/core";
import type { MemoryExecutionClient } from "./memory-execution";
import { deleteExecutionWaiters, waiterKey } from "./waiters";

/**
 * In-memory join waiters. `join` reads the target and registers in one
 * synchronous step, and the execution client calls `wakeWaiters` synchronously
 * inside a terminal acknowledgement, so neither can interleave with the other.
 */
export class MemoryJoinClient implements JoinClient {
  /** Keyed by target execution ID, then by joining execution ID and step key. */
  private waiters = new Map<string, Map<string, JoinWaiter>>();
  private executions: Pick<MemoryExecutionClient, "getState" | "wakeNow">;

  constructor(params: {
    executions: Pick<MemoryExecutionClient, "getState" | "wakeNow">;
  }) {
    this.executions = params.executions;
  }

  async join(request: JoinWaiter): Promise<JoinResult> {
    const waiter = prepareJoinWaiter(request);
    const result = readJoinTarget(this.executions.getState(waiter.targetExecutionId));
    if (result.status !== "waiting") return result;
    let waiters = this.waiters.get(waiter.targetExecutionId);
    if (!waiters) this.waiters.set(waiter.targetExecutionId, (waiters = new Map()));
    waiters.set(waiterKey(waiter), waiter);
    return result;
  }

  async getWaiters(targetExecutionId: string): Promise<JoinWaiter[]> {
    return [...(this.waiters.get(targetExecutionId)?.values() ?? [])]
      .map((waiter) => ({ ...waiter }))
      .sort(compareJoinWaiters);
  }

  /**
   * Requests a wake for every waiter on the target and keeps the waiters.
   * Called synchronously by a terminal acknowledgement of the target.
   */
  wakeWaiters(targetExecutionId: string) {
    for (const waiter of this.waiters.get(targetExecutionId)?.values() ?? []) {
      this.executions.wakeNow(waiter.executionId);
    }
  }

  /**
   * Deletes the waiters a step registered. Called synchronously when the heap
   * records the step's result, so the waiter goes with that write.
   */
  deleteWaitersForStep(params: { executionId: string; stepKey: string }) {
    const key = waiterKey(params);
    for (const [targetExecutionId, waiters] of this.waiters) {
      waiters.delete(key);
      if (waiters.size === 0) this.waiters.delete(targetExecutionId);
    }
  }

  /**
   * Deletes every waiter an execution registered. Called synchronously by a
   * forced cancel, so the waiters go with that write, and by the cleanup of
   * a pruned execution. The deletion runs before the promise is returned.
   */
  async deleteWaitersForExecution(executionId: string): Promise<void> {
    deleteExecutionWaiters(this.waiters, executionId);
  }

  /**
   * Whether a waiter whose joiner exists and is not terminal is registered on
   * the target. Read synchronously by the tombstone write.
   */
  isTargetHeld(targetExecutionId: string): boolean {
    for (const waiter of this.waiters.get(targetExecutionId)?.values() ?? []) {
      const joiner = this.executions.getState(waiter.executionId);
      if (joiner && !isTerminalExecutionStatus(joiner.status)) return true;
    }
    return false;
  }

  /** Deletes the waiters registered on a target. Called synchronously by its tombstone write. */
  deleteTargetWaiters(targetExecutionId: string) {
    this.waiters.delete(targetExecutionId);
  }
}
