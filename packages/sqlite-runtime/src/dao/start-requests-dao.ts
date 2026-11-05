import type { StartRequestState } from "@yieldstar/core";
import type { SqliteDriver } from "../sqlite-driver";
import { runTransaction } from "../sqlite-transaction";

class StartRequestRow {
  caller_execution_id!: string;
  step_key!: string;
  workflow_id!: string;
  params!: string | null;
  detached!: number;
  execution_id!: string;
}

/**
 * Start requests recorded by `ExecutionClient.start`, keyed by the caller's
 * execution ID and step key. Callers own the surrounding transaction, which
 * must also insert the target execution.
 */
export class StartRequestsDao {
  private db: SqliteDriver;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.setupDb();
  }

  setupDb() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS start_requests (
        caller_execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        params TEXT,
        execution_id TEXT NOT NULL,
        PRIMARY KEY (caller_execution_id, step_key)
      );
    `);
    this.setupDetached();
  }

  /**
   * Adds `detached` to tables from before cancellation, so their requests
   * read as not detached. The check repeats under the write lock, so
   * processes that open the file together add the column once.
   */
  private setupDetached() {
    const hasColumn = () =>
      this.db
        .query<{ name: string }>(`PRAGMA table_info(start_requests)`)
        .all()
        .some((column) => column.name === "detached");
    if (hasColumn()) return;
    runTransaction(this.db, () => {
      if (hasColumn()) return;
      this.db.run(
        `ALTER TABLE start_requests ADD COLUMN detached INTEGER NOT NULL DEFAULT 0`
      );
    });
  }

  get(callerExecutionId: string, stepKey: string): StartRequestState | undefined {
    const row = this.db
      .query<StartRequestRow>(
        `SELECT * FROM start_requests
         WHERE caller_execution_id = $callerExecutionId AND step_key = $stepKey`
      )
      .get({ $callerExecutionId: callerExecutionId, $stepKey: stepKey });
    return row
      ? {
          callerExecutionId: row.caller_execution_id,
          stepKey: row.step_key,
          workflowId: row.workflow_id,
          params: row.params,
          detached: Boolean(row.detached),
          executionId: row.execution_id,
        }
      : undefined;
  }

  /** Deletes the start requests a pruned caller recorded. */
  deleteForCaller(callerExecutionId: string) {
    this.db
      .query(`DELETE FROM start_requests WHERE caller_execution_id = $callerExecutionId`)
      .run({ $callerExecutionId: callerExecutionId });
  }

  insert(request: StartRequestState) {
    this.db
      .query(
        `INSERT INTO start_requests
           (caller_execution_id, step_key, workflow_id, params, detached, execution_id)
         VALUES
           ($callerExecutionId, $stepKey, $workflowId, $params, $detached, $executionId)`
      )
      .run({
        $callerExecutionId: request.callerExecutionId,
        $stepKey: request.stepKey,
        $workflowId: request.workflowId,
        $params: request.params,
        $detached: request.detached ? 1 : 0,
        $executionId: request.executionId,
      });
  }
}
