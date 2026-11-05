import {
  isLeaseCurrent,
  markCancellationDelivered,
  type ExecutionLease,
  type HeapClient,
  type HeapRecord,
  type LeaseLost,
} from "@yieldstar/core";
import { EffectWaitersDao } from "./dao/effect-waiters-dao";
import { ExecutionsDao } from "./dao/executions-dao";
import { JoinWaitersDao } from "./dao/join-waiters-dao";
import { StepResponsesDao } from "./dao/step-response-dao";
import { StoreWaitersDao } from "./dao/store-waiters-dao";
import type { SqliteDriver } from "./sqlite-driver";
import { runTransaction } from "./sqlite-transaction";

export class SqliteHeapClient implements HeapClient {
  private db: SqliteDriver;
  private executionsDao: ExecutionsDao;
  private stepResponsesDao: StepResponsesDao;
  private storeWaitersDao: StoreWaitersDao;
  private effectWaitersDao: EffectWaitersDao;
  private joinWaitersDao: JoinWaitersDao;
  private clock: () => number;

  constructor(db: SqliteDriver, options: { clock?: () => number } = {}) {
    this.db = db;
    // Read Date.now lazily so fake timers installed later still apply.
    this.clock = options.clock ?? (() => Date.now());
    this.executionsDao = new ExecutionsDao(db);
    this.stepResponsesDao = new StepResponsesDao(db);
    this.storeWaitersDao = new StoreWaitersDao(db);
    this.effectWaitersDao = new EffectWaitersDao(db);
    this.joinWaitersDao = new JoinWaitersDao(db);
  }

  async getAllSteps() {
    return this.stepResponsesDao.getAllSteps();
  }

  async deleteAll() {
    this.stepResponsesDao.deleteAll();
  }

  async readStep(params: { executionId: string; stepKey: string }) {
    const result = this.stepResponsesDao.getLatestStepResponse(
      params.executionId,
      params.stepKey
    );

    if (!result) {
      return null;
    }

    return {
      stepResponseJson: result.step_response,
      meta: {
        attempt: result.step_attempt,
        done: Boolean(result.step_done),
      },
    };
  }

  /**
   * Checks the lease against the `executions` row and writes the step record
   * in one IMMEDIATE transaction, so no claim can interleave between them.
   */
  async writeStep(params: {
    lease: ExecutionLease;
    stepKey: string;
    stepAttempt: number;
    done: boolean;
    stepResponseJson: string;
  }): Promise<HeapRecord | LeaseLost> {
    const { lease, stepKey, stepAttempt, done, stepResponseJson } = params;
    const { executionId } = lease;
    return runTransaction(this.db, () => {
      const state = this.executionsDao.getState(executionId);
      if (!state || !isLeaseCurrent(state, lease)) {
        return { status: "lost" as const };
      }

      const { record, inserted } = this.insert(
        executionId,
        stepKey,
        stepAttempt,
        done,
        stepResponseJson
      );
      // A waiter is deleted with the write that records its step's result.
      // Unfinished attempts (e.g. a store wait, a suspended effect or a join)
      // keep the waiter they registered.
      if (inserted && done) this.deleteStepWaiters(executionId, stepKey);
      return record;
    });
  }

  /**
   * Checks the lease, writes the step records, deletes those steps' waiters
   * and sets `deliveredAt` in one IMMEDIATE transaction.
   */
  async recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly { stepKey: string; stepAttempt: number; stepResponseJson: string }[];
  }): Promise<HeapRecord[] | LeaseLost> {
    const { lease, steps } = params;
    const { executionId } = lease;
    return runTransaction(this.db, () => {
      const state = this.executionsDao.getState(executionId);
      if (!state || !isLeaseCurrent(state, lease)) {
        return { status: "lost" as const };
      }
      // Throws, and so rolls back, when no request is recorded.
      this.executionsDao.updateState(markCancellationDelivered(state, this.clock()));
      return steps.map(({ stepKey, stepAttempt, stepResponseJson }) => {
        const { record } = this.insert(
          executionId,
          stepKey,
          stepAttempt,
          true,
          stepResponseJson
        );
        this.deleteStepWaiters(executionId, stepKey);
        return record;
      });
    });
  }

  /** A record never changes once written: an existing one is returned as stored. */
  private insert(
    executionId: string,
    stepKey: string,
    stepAttempt: number,
    done: boolean,
    stepResponseJson: string
  ): { record: HeapRecord; inserted: boolean } {
    const existing = this.stepResponsesDao.getStepResponse(
      executionId,
      stepKey,
      stepAttempt
    );
    if (existing) {
      return {
        record: {
          stepResponseJson: existing.step_response,
          meta: { attempt: existing.step_attempt, done: Boolean(existing.step_done) },
        },
        inserted: false,
      };
    }
    this.stepResponsesDao.insertStepResponse(
      executionId,
      stepKey,
      stepAttempt,
      done,
      stepResponseJson
    );
    return { record: { stepResponseJson, meta: { attempt: stepAttempt, done } }, inserted: true };
  }

  private deleteStepWaiters(executionId: string, stepKey: string) {
    this.storeWaitersDao.deleteStepWaiters(executionId, stepKey);
    this.effectWaitersDao.deleteStepWaiters(executionId, stepKey);
    this.joinWaitersDao.deleteStepWaiters(executionId, stepKey);
  }
}
