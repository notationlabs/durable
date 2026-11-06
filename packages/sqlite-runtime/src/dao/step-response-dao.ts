import type { SqliteDriver } from "../sqlite-driver";
import { runTransaction } from "../sqlite-transaction";

class StepReponseRow {
  execution_id!: string;
  step_key!: string;
  step_attempt!: number;
  step_done!: 0 | 1;
  step_response!: string;
  step_kind!: string | null;
}

export class StepResponsesDao {
  private db: SqliteDriver;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.setupDb();
  }

  setupDb() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS step_responses (
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_attempt INTEGER NOT NULL,
        step_done BOOL NOT NULL,
        step_response TEXT NOT NULL,
        PRIMARY KEY (execution_id, step_key, step_attempt)
      );
      CREATE INDEX IF NOT EXISTS idx_execution_step_attempt 
      ON step_responses(execution_id, step_key, step_attempt DESC);
    `);
    this.setupStepKind();
  }

  /**
   * Adds `step_kind`, under the write lock and only once. Rows from before it
   * have no kind, so a delivery does not compare their kind on replay.
   */
  private setupStepKind() {
    const hasColumn = () =>
      this.db
        .query<{ name: string }>(`PRAGMA table_info(step_responses)`)
        .all()
        .some((column) => column.name === "step_kind");
    if (hasColumn()) return;
    runTransaction(this.db, () => {
      if (!hasColumn()) this.db.run(`ALTER TABLE step_responses ADD COLUMN step_kind TEXT`);
    });
  }

  listStepKeys(executionId: string) {
    return this.db
      .query<{ step_key: string }>(
        `SELECT DISTINCT step_key FROM step_responses WHERE execution_id = $executionId`
      )
      .all({ $executionId: executionId })
      .map((row) => row.step_key);
  }

  getAllSteps() {
    const query = this.db.query(`SELECT * FROM step_responses`);
    return query.all();
  }

  deleteAll() {
    const query = this.db.query(`DELETE FROM step_responses`);
    query.run();
  }

  getLatestStepResponse(executionId: string, stepKey: string) {
    const query = this.db.query<StepReponseRow>(
      `SELECT * FROM step_responses
       WHERE execution_id = $executionId
       AND step_key = $stepKey
       ORDER BY step_attempt DESC LIMIT 1`
    );

    const result = query.get({
      $executionId: executionId,
      $stepKey: stepKey,
    });

    return result;
  }

  getStepResponse(executionId: string, stepKey: string, stepAttempt: number) {
    return this.db
      .query<StepReponseRow>(
        `SELECT * FROM step_responses
         WHERE execution_id = $executionId
         AND step_key = $stepKey
         AND step_attempt = $stepAttempt`
      )
      .get({
        $executionId: executionId,
        $stepKey: stepKey,
        $stepAttempt: stepAttempt,
      });
  }

  insertStepResponse(
    executionId: string,
    stepKey: string,
    stepAttempt: number,
    stepDone: boolean,
    stepResponseJson: string,
    stepKind: string | null
  ) {
    const query = this.db.query(
      `INSERT INTO step_responses (execution_id, step_key, step_attempt, step_done, step_response, step_kind)
        VALUES ($executionId, $stepKey, $stepAttempt, $stepDone, $stepResponse, $stepKind)`
    );

    query.run({
      $executionId: executionId,
      $stepKey: stepKey,
      $stepAttempt: stepAttempt,
      $stepDone: stepDone,
      $stepResponse: stepResponseJson,
      $stepKind: stepKind,
    });
  }
}
