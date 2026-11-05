import type {
  ExecutionState,
  PreparedExecutionListRequest,
  SequencedExecutionState,
} from "@yieldstar/core";
import { nextClaimAt } from "@yieldstar/core";
import type { SqliteDriver, SqliteParameters } from "../sqlite-driver";

class ExecutionRow {
  execution_id!: string;
  workflow_id!: string;
  params!: string;
  context!: string;
  status!: ExecutionState["status"];
  outcome!: string | null;
  admitted_at!: number;
  updated_at!: number;
  due_at!: number | null;
  claimed_due_at!: number | null;
  lease_token!: number;
  lease_expires_at!: number | null;
  unacknowledged_delivery_count!: number;
  redeliver_at!: number | null;
  next_claim_at!: number | null;
  parent_execution_id!: string | null;
  parent_step_key!: string | null;
}

class SequencedExecutionRow extends ExecutionRow {
  sequence!: number;
}

class ExecutionIdRow {
  execution_id!: string;
}

/**
 * Persists every ExecutionState field, plus `next_claim_at` (derived with
 * nextClaimAt) so claimDue can find due executions through an index. The
 * table's `rowid` is the admission sequence that orders `list`: SQLite
 * assigns it under the write lock, one more than the largest rowid in the
 * table. Callers own the surrounding transaction.
 */
export class ExecutionsDao {
  private db: SqliteDriver;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.setupDb();
  }

  setupDb() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS executions (
        execution_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        params TEXT NOT NULL,
        context TEXT NOT NULL,
        status TEXT NOT NULL,
        outcome TEXT,
        admitted_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        due_at INTEGER,
        claimed_due_at INTEGER,
        lease_token INTEGER NOT NULL DEFAULT 0,
        lease_expires_at INTEGER,
        unacknowledged_delivery_count INTEGER NOT NULL DEFAULT 0,
        redeliver_at INTEGER,
        next_claim_at INTEGER,
        parent_execution_id TEXT,
        parent_step_key TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_executions_next_claim_at
      ON executions(next_claim_at) WHERE next_claim_at IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_executions_workflow_id
      ON executions(workflow_id);
      CREATE INDEX IF NOT EXISTS idx_executions_status
      ON executions(status);
    `);
  }

  getState(executionId: string): ExecutionState | undefined {
    const row = this.db
      .query<ExecutionRow>(
        `SELECT * FROM executions WHERE execution_id = $executionId`
      )
      .get({ $executionId: executionId });
    return row ? toState(row) : undefined;
  }

  /**
   * Up to `limit` claimable execution IDs, earliest due first. `now` is a
   * millisecond Unix timestamp from the runtime clock. When `executionIds` is
   * given, only those executions are considered.
   */
  getClaimableIds(
    now: number,
    limit: number,
    executionIds?: readonly string[]
  ) {
    return this.db
      .query<ExecutionIdRow>(
        `SELECT execution_id FROM executions
         WHERE next_claim_at IS NOT NULL AND next_claim_at <= $now
           AND ($executionIds IS NULL
             OR execution_id IN (SELECT value FROM json_each($executionIds)))
         ORDER BY next_claim_at, execution_id
         LIMIT $limit`
      )
      .all({
        $now: now,
        $limit: limit,
        $executionIds: executionIds ? JSON.stringify(executionIds) : null,
      })
      .map((row) => row.execution_id);
  }

  /**
   * Up to `limit + 1` executions that match the request, newest first. `now`
   * is used to decide which executions are `running`. Only the conditions the
   * request uses are added, so SQLite can read a filter through the
   * `workflow_id` or `status` index, whose entries are in rowid order.
   */
  listStates(
    request: PreparedExecutionListRequest,
    now: number
  ): SequencedExecutionState[] {
    const conditions: string[] = [];
    const bindings: SqliteParameters = { $limit: request.limit + 1 };
    if (request.workflowId !== undefined) {
      conditions.push("workflow_id = $workflowId");
      bindings.$workflowId = request.workflowId;
    }
    if (request.beforeSequence !== undefined) {
      conditions.push("rowid < $before");
      bindings.$before = request.beforeSequence;
    }
    // `running` is derived from a live lease, so the other statuses exclude
    // executions whose lease has not expired.
    if (request.status === "running") {
      conditions.push("lease_expires_at IS NOT NULL AND lease_expires_at > $now");
      bindings.$now = now;
    } else if (request.status !== undefined) {
      conditions.push(
        "status = $status AND (lease_expires_at IS NULL OR lease_expires_at <= $now)"
      );
      bindings.$status = request.status;
      bindings.$now = now;
    }
    return this.db
      .query<SequencedExecutionRow>(
        `SELECT *, rowid AS sequence FROM executions
         ${conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : ""}
         ORDER BY rowid DESC
         LIMIT $limit`
      )
      .all(bindings)
      .map((row) => ({
        state: toState(row),
        admissionSequence: Number(row.sequence),
      }));
  }

  /** Returns false when the execution ID is already in use. */
  insertState(state: ExecutionState): boolean {
    const result = this.db
      .query(
        `INSERT INTO executions (
           execution_id, workflow_id, params, context, status, outcome,
           admitted_at, updated_at, due_at, claimed_due_at, lease_token,
           lease_expires_at, unacknowledged_delivery_count, redeliver_at,
           next_claim_at, parent_execution_id, parent_step_key
         ) VALUES (
           $executionId, $workflowId, $params, $context, $status, $outcome,
           $admittedAt, $updatedAt, $dueAt, $claimedDueAt, $leaseToken,
           $leaseExpiresAt, $unacknowledgedDeliveryCount, $redeliverAt,
           $nextClaimAt, $parentExecutionId, $parentStepKey
         )
         ON CONFLICT(execution_id) DO NOTHING`
      )
      .run({
        ...toBindings(state),
        $parentExecutionId: state.parent?.executionId ?? null,
        $parentStepKey: state.parent?.stepKey ?? null,
      });
    return Number(result.changes) === 1;
  }

  /** Leaves the parent link as inserted: it is immutable. */
  updateState(state: ExecutionState) {
    this.db
      .query(
        `UPDATE executions SET
           workflow_id = $workflowId,
           params = $params,
           context = $context,
           status = $status,
           outcome = $outcome,
           admitted_at = $admittedAt,
           updated_at = $updatedAt,
           due_at = $dueAt,
           claimed_due_at = $claimedDueAt,
           lease_token = $leaseToken,
           lease_expires_at = $leaseExpiresAt,
           unacknowledged_delivery_count = $unacknowledgedDeliveryCount,
           redeliver_at = $redeliverAt,
           next_claim_at = $nextClaimAt
         WHERE execution_id = $executionId`
      )
      .run(toBindings(state));
  }
}

function toState(row: ExecutionRow): ExecutionState {
  return {
    executionId: row.execution_id,
    workflowId: row.workflow_id,
    params: row.params,
    context: row.context,
    status: row.status,
    outcome: row.outcome,
    admittedAt: Number(row.admitted_at),
    updatedAt: Number(row.updated_at),
    dueAt: toNumberOrNull(row.due_at),
    claimedDueAt: toNumberOrNull(row.claimed_due_at),
    leaseToken: Number(row.lease_token),
    leaseExpiresAt: toNumberOrNull(row.lease_expires_at),
    unacknowledgedDeliveryCount: Number(row.unacknowledged_delivery_count),
    redeliverAt: toNumberOrNull(row.redeliver_at),
    parent:
      row.parent_execution_id !== null && row.parent_step_key !== null
        ? { executionId: row.parent_execution_id, stepKey: row.parent_step_key }
        : null,
  };
}

function toBindings(state: ExecutionState) {
  return {
    $executionId: state.executionId,
    $workflowId: state.workflowId,
    $params: state.params,
    $context: state.context,
    $status: state.status,
    $outcome: state.outcome,
    $admittedAt: state.admittedAt,
    $updatedAt: state.updatedAt,
    $dueAt: state.dueAt,
    $claimedDueAt: state.claimedDueAt,
    $leaseToken: state.leaseToken,
    $leaseExpiresAt: state.leaseExpiresAt,
    $unacknowledgedDeliveryCount: state.unacknowledgedDeliveryCount,
    $redeliverAt: state.redeliverAt,
    $nextClaimAt: nextClaimAt(state),
  };
}

function toNumberOrNull(value: number | null) {
  return value === null ? null : Number(value);
}
