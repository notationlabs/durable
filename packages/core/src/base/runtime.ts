import type { EffectClient } from "./effect";
import type { ExecutionClient } from "./execution";
import type { HeapClient } from "./heap";
import type { JoinClient } from "./join";
import { DEFAULT_PRUNE_BATCH_SIZE, type RetentionOptions } from "./retention";
import type { StoreClient } from "./store";

export interface RuntimeOptions {
  /** Every client of the runtime reads time from this clock. Defaults to `Date.now`. */
  clock?: () => number;
  /**
   * Rejects a keyless step that has no record with `WorkflowReplayError`
   * (`STEP_KEY_REQUIRED`). A keyless step that already has a record still
   * replays. Off by default; it will default to on in the next major version.
   */
  requireStepKeys?: boolean;
  /**
   * How long to keep terminal executions before pruning them. Without it,
   * nothing is pruned. Invalid periods throw a `TypeError` from the
   * constructor.
   */
  retain?: RetentionOptions;
}

/**
 * A runtime is defined by the guarantees its clients keep, not by how many
 * databases it uses:
 *
 * - Where one write must accompany another, such as the wakes for a store
 *   commit, the second is durable once the first commits and is applied
 *   eventually (in one transaction or through an outbox).
 * - A store or effect waiter is deleted atomically with its step's result.
 * - An effect's settlement and the wakes of its waiters are one atomic write.
 * - A terminal acknowledgement and the wakes of its join waiters are one
 *   atomic write. A join waiter is deleted atomically with its step's result.
 * - Every client reads time from the runtime's clock.
 * - Replacing a terminal record with its tombstone is one atomic write with
 *   the check that no non-terminal joiner waits on it.
 */
export abstract class Runtime {
  /** The clock every client, and every step's wake time, reads. */
  abstract readonly clock: () => number;
  /** As in `RuntimeOptions.requireStepKeys`. */
  readonly requireStepKeys: boolean = false;
  abstract readonly executions: ExecutionClient;
  abstract readonly effects: EffectClient;
  abstract readonly heap: HeapClient;
  abstract readonly joins: JoinClient;
  abstract readonly stores: StoreClient;
  /** As in `RuntimeOptions.retain`, validated. */
  readonly retain?: RetentionOptions;

  /**
   * Runs one pruning pass: replaces up to `limit` expired terminal executions
   * with tombstones under `retain`, and then cleans up to `limit` tombstones
   * still pending cleanup, including ones a pass that stopped part way left
   * behind. Cleanup deletes each execution's step records, store receipts,
   * waiters and start requests, and its stores when its policy sets
   * `pruneExecutionStores`. Every step is safe to repeat and to run in
   * several processes at once. Without `retain`, it only cleans.
   *
   * A cleanup that throws does not stop the pass: its tombstone stays pending
   * for a later pass, and the pass goes on to the next one. The result counts
   * the executions tombstoned, cleaned and failed, and lists each failed
   * cleanup's error in `errors`. A failure to tombstone or to list pending
   * cleanups rejects the pass.
   */
  async prune(options?: { limit?: number }): Promise<PruneResult> {
    const limit = options?.limit ?? DEFAULT_PRUNE_BATCH_SIZE;
    if (!Number.isInteger(limit) || limit < 1) {
      throw new RangeError("limit must be a positive integer");
    }
    const pruned = this.retain
      ? (await this.executions.prune({ retain: this.retain, limit })).length
      : 0;
    let cleaned = 0;
    const errors: PruneFailure[] = [];
    for (const pending of await this.executions.listPendingPrunes({ limit })) {
      const { executionId } = pending;
      try {
        // 1. Delete everything the tombstone no longer refers to.
        await this.heap.deleteSteps({ executionId });
        await this.stores.deleteExecutionData({
          executionId,
          deleteExecutionStore: pending.deleteExecutionStore,
        });
        await this.effects.deleteWaitersForExecution(executionId);
        await this.joins.deleteWaitersForExecution(executionId);
        // 2. Clear the mark last, so a pass that stops before here is finished
        // by the next one.
        await this.executions.finishPrune(executionId);
        cleaned++;
      } catch (error) {
        errors.push({ executionId, error });
      }
    }
    return { pruned, cleaned, failed: errors.length, errors };
  }
}

/** What one `Runtime.prune` pass did. */
export type PruneResult = {
  /** Executions replaced with tombstones. */
  pruned: number;
  /** Tombstones whose cleanup finished. */
  cleaned: number;
  /** Tombstones whose cleanup threw. They stay pending. */
  failed: number;
  errors: PruneFailure[];
};

export type PruneFailure = { executionId: string; error: unknown };
