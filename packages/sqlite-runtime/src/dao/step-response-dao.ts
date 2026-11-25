import type { SqliteDriver } from "../sqlite-driver";

class StepReponseRow {
  execution_id!: string;
  step_key!: string;
  step_attempt!: number;
  step_done!: 0 | 1;
  step_response!: string;
}

export class StepResponsesDao {
  private db: SqliteDriver;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.setupDb();
  }

  // WITHOUT ROWID stores each record in the primary key's index, so a record
  // is one row to write. A table that an earlier version created keeps its
  // rowid and its second index, which repeats the primary key.
  setupDb() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS step_responses (
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_attempt INTEGER NOT NULL,
        step_done BOOL NOT NULL,
        step_response TEXT NOT NULL,
        PRIMARY KEY (execution_id, step_key, step_attempt)
      ) WITHOUT ROWID;
    `);
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

  /** The latest attempt of every step of an execution. */
  getLatestStepResponses(executionId: string) {
    return this.db
      .query<StepReponseRow>(
        `SELECT * FROM step_responses AS latest
         WHERE execution_id = $executionId
         AND step_attempt = (
           SELECT MAX(step_attempt) FROM step_responses
           WHERE execution_id = latest.execution_id AND step_key = latest.step_key
         )`
      )
      .all({ $executionId: executionId });
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
    stepResponseJson: string
  ) {
    const query = this.db.query(
      `INSERT INTO step_responses (execution_id, step_key, step_attempt, step_done, step_response) 
        VALUES ($executionId, $stepKey, $stepAttempt, $stepDone, $stepResponse)`
    );

    query.run({
      $executionId: executionId,
      $stepKey: stepKey,
      $stepAttempt: stepAttempt,
      $stepDone: stepDone,
      $stepResponse: stepResponseJson,
    });
  }
}
