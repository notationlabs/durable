import {
  isLeaseCurrent,
  markCancellationDelivered,
  recordWorkflowVersion,
  type ExecutionLease,
  type ExecutionState,
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

    return toHeapRecord(result);
  }

  async listStepKeys(params: { executionId: string }): Promise<string[]> {
    return this.stepResponsesDao.listStepKeys(params.executionId);
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
    stepKind?: string;
    workflowVersion?: number;
    deleteWaitersOf?: readonly string[];
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
        stepResponseJson,
        params.stepKind
      );
      this.recordWorkflowVersion(state, params.workflowVersion);
      // A waiter is deleted with the write that records its step's result.
      // Unfinished attempts (e.g. a store wait, a suspended effect or a join)
      // keep the waiter they registered.
      if (inserted && done) {
        this.deleteStepWaiters(executionId, stepKey);
        for (const other of params.deleteWaitersOf ?? []) {
          this.deleteStepWaiters(executionId, other);
        }
      }
      return record;
    });
  }

  /**
   * Checks the lease, writes the step records, deletes those steps' waiters
   * and sets `deliveredAt` in one IMMEDIATE transaction. `deliveredAt` is set
   * last, so a failure there rolls back the records and the deletions.
   */
  async recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly {
      stepKey: string;
      stepAttempt: number;
      stepResponseJson: string;
      stepKind?: string;
    }[];
    workflowVersion?: number;
  }): Promise<(HeapRecord | null)[] | LeaseLost> {
    const { lease, steps } = params;
    const { executionId } = lease;
    return runTransaction(this.db, () => {
      const state = this.executionsDao.getState(executionId);
      if (!state || !isLeaseCurrent(state, lease)) {
        return { status: "lost" as const };
      }
      const stored = steps.map(({ stepKey, stepAttempt }) =>
        this.read(executionId, stepKey, stepAttempt)
      );
      const conflicting = stored.some(
        (record, i) =>
          record !== null &&
          (record.stepResponseJson !== steps[i].stepResponseJson || !record.meta.done)
      );
      if (conflicting) return stored;
      const records = steps.map(({ stepKey, stepAttempt, stepResponseJson, stepKind }) => {
        const { record } = this.insert(
          executionId,
          stepKey,
          stepAttempt,
          true,
          stepResponseJson,
          stepKind
        );
        this.deleteStepWaiters(executionId, stepKey);
        return record;
      });
      // Throws, and so rolls back, when no request is recorded.
      this.executionsDao.updateState(
        recordWorkflowVersion(markCancellationDelivered(state, this.clock()), params.workflowVersion)
      );
      return records;
    });
  }

  private read(executionId: string, stepKey: string, stepAttempt: number): HeapRecord | null {
    const existing = this.stepResponsesDao.getStepResponse(executionId, stepKey, stepAttempt);
    return existing ? toHeapRecord(existing) : null;
  }

  private recordWorkflowVersion(state: ExecutionState, workflowVersion: number | undefined) {
    const next = recordWorkflowVersion(state, workflowVersion);
    if (next !== state) this.executionsDao.updateState(next);
  }

  /** A record never changes once written: an existing one is returned as stored. */
  private insert(
    executionId: string,
    stepKey: string,
    stepAttempt: number,
    done: boolean,
    stepResponseJson: string,
    stepKind: string | undefined
  ): { record: HeapRecord; inserted: boolean } {
    const existing = this.read(executionId, stepKey, stepAttempt);
    if (existing) return { record: existing, inserted: false };
    this.stepResponsesDao.insertStepResponse(
      executionId,
      stepKey,
      stepAttempt,
      done,
      stepResponseJson,
      stepKind ?? null
    );
    return {
      record: {
        stepResponseJson,
        meta: { attempt: stepAttempt, done, ...(stepKind !== undefined ? { kind: stepKind } : {}) },
      },
      inserted: true,
    };
  }

  private deleteStepWaiters(executionId: string, stepKey: string) {
    this.storeWaitersDao.deleteStepWaiters(executionId, stepKey);
    this.effectWaitersDao.deleteStepWaiters(executionId, stepKey);
    this.joinWaitersDao.deleteStepWaiters(executionId, stepKey);
  }
}

function toHeapRecord(row: {
  step_response: string;
  step_attempt: number;
  step_done: 0 | 1;
  step_kind: string | null;
}): HeapRecord {
  return {
    stepResponseJson: row.step_response,
    meta: {
      attempt: Number(row.step_attempt),
      done: Boolean(row.step_done),
      ...(row.step_kind !== null && row.step_kind !== undefined ? { kind: row.step_kind } : {}),
    },
  };
}
