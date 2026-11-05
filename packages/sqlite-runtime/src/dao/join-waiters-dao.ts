import type { JoinWaiter } from "@yieldstar/core";
import type { SqliteDriver } from "../sqlite-driver";

class JoinWaiterRow {
  target_execution_id!: string;
  execution_id!: string;
  step_key!: string;
}

/**
 * Join waiters live in the same database as execution and step records, so a
 * terminal acknowledgement wakes them, and the heap deletes one, in the
 * transaction of its own write. Callers own the surrounding transaction.
 */
export class JoinWaitersDao {
  private db: SqliteDriver;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.setupDb();
  }

  setupDb() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS join_waiters (
        target_execution_id TEXT NOT NULL,
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        PRIMARY KEY (target_execution_id, execution_id, step_key)
      );
      CREATE INDEX IF NOT EXISTS idx_join_waiters_step
      ON join_waiters(execution_id, step_key);
    `);
  }

  upsertWaiter(waiter: JoinWaiter) {
    this.db
      .query(
        `INSERT INTO join_waiters (target_execution_id, execution_id, step_key)
         VALUES ($targetExecutionId, $executionId, $stepKey)
         ON CONFLICT(target_execution_id, execution_id, step_key) DO NOTHING`
      )
      .run({
        $targetExecutionId: waiter.targetExecutionId,
        $executionId: waiter.executionId,
        $stepKey: waiter.stepKey,
      });
  }

  getWaiters(targetExecutionId: string): JoinWaiter[] {
    return this.db
      .query<JoinWaiterRow>(
        `SELECT target_execution_id, execution_id, step_key FROM join_waiters
         WHERE target_execution_id = $targetExecutionId
         ORDER BY execution_id, step_key`
      )
      .all({ $targetExecutionId: targetExecutionId })
      .map((row) => ({
        targetExecutionId: row.target_execution_id,
        executionId: row.execution_id,
        stepKey: row.step_key,
      }));
  }

  getWaitingExecutionIds(targetExecutionId: string): string[] {
    return this.db
      .query<JoinWaiterRow>(
        `SELECT DISTINCT execution_id FROM join_waiters
         WHERE target_execution_id = $targetExecutionId`
      )
      .all({ $targetExecutionId: targetExecutionId })
      .map((row) => row.execution_id);
  }

  deleteStepWaiters(executionId: string, stepKey: string) {
    this.db
      .query(
        `DELETE FROM join_waiters WHERE execution_id = $executionId AND step_key = $stepKey`
      )
      .run({ $executionId: executionId, $stepKey: stepKey });
  }
}
