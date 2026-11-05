import type {
  ExecutionDivergenceState,
  ExecutionState,
  PreparedExecutionListRequest,
  RetentionOptions,
  SequencedExecutionState,
} from "@yieldstar/core";
import { nextClaimAt } from "@yieldstar/core";
import type { SqliteDriver } from "../sqlite-driver";
import { runTransaction } from "../sqlite-transaction";

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
  parent_detached!: number;
  admission_sequence!: number;
  cancellation_reason!: string | null;
  cancellation_source!: string | null;
  cancellation_forced!: number | null;
  cancellation_requested_at!: number | null;
  cancellation_delivered_at!: number | null;
  workflow_version!: number | null;
  divergence_code!: string | null;
  divergence_step_key!: string | null;
  divergence_workflow_version!: number | null;
  divergence_detected_at!: number | null;
  pruned_at!: number | null;
  prune_cleanup_pending!: number;
  prune_execution_stores!: number;
}

class PendingPruneRow {
  execution_id!: string;
  prune_execution_stores!: number;
}

class ExecutionIdRow {
  execution_id!: string;
}

/**
 * Persists every ExecutionState field, plus `next_claim_at` (derived with
 * nextClaimAt) so claimDue can find due executions through an index, and
 * `admission_sequence`, which orders `list`. A cancellation request is stored
 * in the `cancellation_*` columns; `cancellation_requested_at` is NULL when
 * there is none. A divergence is stored in the `divergence_*` columns;
 * `divergence_code` is NULL when there is none. A tombstone has `pruned_at`
 * set, and `prune_cleanup_pending` is 1 until its cleanup finishes. Callers
 * own the surrounding transaction.
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
        parent_step_key TEXT,
        admission_sequence INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_executions_next_claim_at
      ON executions(next_claim_at) WHERE next_claim_at IS NOT NULL;
    `);
    this.setupAdmissionSequence();
    this.setupCancellation();
    this.setupReplayStability();
    this.setupRetention();
  }

  /**
   * Adds the tombstone columns, and the indexes that find expired executions
   * and tombstones pending cleanup. Rows from before them read as executions
   * that are not pruned.
   *
   * Every process that shares the file must run a version with retention
   * support before anything prunes. A process on an earlier version reads a
   * tombstone as a terminal execution with an empty event and no outcome,
   * and fails to decode it.
   */
  private setupRetention() {
    this.addMissingColumns([
      "pruned_at INTEGER",
      "prune_cleanup_pending INTEGER NOT NULL DEFAULT 0",
      "prune_execution_stores INTEGER NOT NULL DEFAULT 0",
    ]);
    this.db.run(`
      CREATE INDEX IF NOT EXISTS idx_executions_unpruned_terminal
      ON executions(updated_at)
      WHERE pruned_at IS NULL AND status IN ('completed', 'failed', 'cancelled');
      CREATE INDEX IF NOT EXISTS idx_executions_prune_cleanup_pending
      ON executions(admission_sequence) WHERE prune_cleanup_pending = 1;
    `);
  }

  /**
   * Adds the workflow version and divergence columns. Rows from before them
   * read as executions with no recorded version and no divergence.
   *
   * Every process that shares the file must run a version with replay
   * stability support before a diverged execution is parked. A process on an
   * earlier version does not read `divergence_*`, and would deliver a parked
   * execution again when it next computes `next_claim_at`.
   */
  private setupReplayStability() {
    this.addMissingColumns([
      "workflow_version INTEGER",
      "divergence_code TEXT",
      "divergence_step_key TEXT",
      "divergence_workflow_version INTEGER",
      "divergence_detected_at INTEGER",
    ]);
  }

  /**
   * Adds each column that does not exist yet. It runs under the write lock
   * and checks again there, so processes that open the file together add
   * each column once.
   */
  private addMissingColumns(columns: readonly string[]) {
    const missing = () => {
      const existing = new Set(
        this.db
          .query<{ name: string }>(`PRAGMA table_info(executions)`)
          .all()
          .map((column) => column.name)
      );
      return columns.filter((column) => !existing.has(column.split(" ")[0]));
    };
    if (missing().length === 0) return;
    runTransaction(this.db, () => {
      for (const column of missing()) {
        this.db.run(`ALTER TABLE executions ADD COLUMN ${column}`);
      }
    });
  }

  /**
   * Adds the cancellation columns and the index that finds an execution's
   * children. The defaults make rows from before cancellation read as
   * attached children with no request.
   *
   * Every process that shares the file must run a version with cancellation
   * support before anything calls `cancel`. A process on an earlier version
   * does not treat `cancelled` as terminal, cannot decode `step-cancelled`
   * records or cancelled outcomes, and ignores cancellation requests.
   */
  private setupCancellation() {
    this.addMissingColumns([
      "parent_detached INTEGER NOT NULL DEFAULT 0",
      "cancellation_reason TEXT",
      "cancellation_source TEXT",
      "cancellation_forced INTEGER",
      "cancellation_requested_at INTEGER",
      "cancellation_delivered_at INTEGER",
    ]);
    this.db.run(`
      CREATE INDEX IF NOT EXISTS idx_executions_parent
      ON executions(parent_execution_id) WHERE parent_execution_id IS NOT NULL;
    `);
  }

  /**
   * Sets up the admission sequence that orders `list`. Inserts leave
   * `admission_sequence` NULL, and an AFTER INSERT trigger sets it from a
   * single-row counter in the same statement. The column has no default and
   * allows NULL, so inserts from processes on older versions still succeed
   * during a rolling upgrade. The counter only increases, so a sequence is
   * never reused after a delete.
   *
   * Existing rows take their rowid, which follows insertion order if no row
   * has been deleted, and the counter starts above them. The setup runs under
   * the write lock and skips what already exists, so processes that open the
   * file together set it up once. The trigger is created last, so its
   * presence means the setup is complete.
   */
  private setupAdmissionSequence() {
    const isSetUp = () =>
      this.db
        .query<{ name: string }>(
          `SELECT name FROM sqlite_master
           WHERE type = 'trigger' AND name = 'trg_executions_admission_sequence'`
        )
        .all().length > 0;
    const hasColumn = () =>
      this.db
        .query<{ name: string }>(`PRAGMA table_info(executions)`)
        .all()
        .some((column) => column.name === "admission_sequence");
    if (isSetUp()) return;
    runTransaction(this.db, () => {
      if (isSetUp()) return;
      if (!hasColumn()) {
        this.db.run(`
          ALTER TABLE executions ADD COLUMN admission_sequence INTEGER;
          UPDATE executions SET admission_sequence = rowid;
        `);
      }
      this.db.run(`
        CREATE TABLE IF NOT EXISTS execution_admission_counter (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          last_sequence INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO execution_admission_counter (id, last_sequence)
        SELECT 1, COALESCE(MAX(admission_sequence), 0) FROM executions;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_admission_sequence
        ON executions(admission_sequence);
        CREATE INDEX IF NOT EXISTS idx_executions_workflow_admission_sequence
        ON executions(workflow_id, admission_sequence);
        CREATE TRIGGER trg_executions_admission_sequence
        AFTER INSERT ON executions
        WHEN NEW.admission_sequence IS NULL
        BEGIN
          UPDATE execution_admission_counter
          SET last_sequence = last_sequence + 1
          WHERE id = 1;
          UPDATE executions
          SET admission_sequence = (
            SELECT last_sequence FROM execution_admission_counter WHERE id = 1
          )
          WHERE execution_id = NEW.execution_id;
        END;
      `);
    });
  }

  getState(executionId: string): ExecutionState | undefined {
    const row = this.db
      .query<ExecutionRow>(
        `SELECT * FROM executions WHERE execution_id = $executionId`
      )
      .get({ $executionId: executionId });
    return row ? toState(row) : undefined;
  }

  /** The children of an execution that are not detached, in admission order. */
  getAttachedChildStates(parentExecutionId: string): ExecutionState[] {
    return this.db
      .query<ExecutionRow>(
        `SELECT * FROM executions
         WHERE parent_execution_id = $parentExecutionId AND parent_detached = 0
         ORDER BY admission_sequence`
      )
      .all({ $parentExecutionId: parentExecutionId })
      .map(toState);
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
   * Up to `limit` IDs of unpruned terminal executions whose retention period
   * has passed at `now`, oldest outcome first. Executions with a join waiter
   * whose joiner exists and is not terminal are left out. Callers check each
   * again with `isExecutionExpired` before writing its tombstone.
   */
  getExpiredIds(retain: RetentionOptions, now: number, limit: number): string[] {
    const cutoffs = retentionCutoffs(retain, now);
    if (cutoffs.latest === null) return [];
    return this.db
      .query<ExecutionIdRow>(
        `SELECT execution_id FROM executions AS e
         WHERE pruned_at IS NULL
           AND status IN ('completed', 'failed', 'cancelled')
           AND updated_at <= $latest
           AND updated_at <= COALESCE(
             (SELECT json_extract(c.value, '$[2]') FROM json_each($workflowCutoffs) AS c
              WHERE json_extract(c.value, '$[0]') = e.workflow_id
                AND json_extract(c.value, '$[1]') = e.status),
             CASE
               WHEN e.workflow_id IN (SELECT value FROM json_each($policyWorkflowIds)) THEN NULL
               ELSE json_extract($defaultCutoffs, '$.' || e.status)
             END
           )
           AND NOT EXISTS (
             SELECT 1 FROM join_waiters AS w
             JOIN executions AS j ON j.execution_id = w.execution_id
             WHERE w.target_execution_id = e.execution_id
               AND j.status NOT IN ('completed', 'failed', 'cancelled')
           )
         ORDER BY updated_at, execution_id
         LIMIT $limit`
      )
      .all({
        $latest: cutoffs.latest,
        $workflowCutoffs: JSON.stringify(cutoffs.workflows),
        $policyWorkflowIds: JSON.stringify(cutoffs.policyWorkflowIds),
        $defaultCutoffs: JSON.stringify(cutoffs.default),
        $limit: limit,
      })
      .map((row) => row.execution_id);
  }

  /** Up to `limit` tombstones pending cleanup, in admission order. */
  getPendingPrunes(limit: number): { executionId: string; deleteExecutionStore: boolean }[] {
    return this.db
      .query<PendingPruneRow>(
        `SELECT execution_id, prune_execution_stores FROM executions
         WHERE prune_cleanup_pending = 1
         ORDER BY admission_sequence
         LIMIT $limit`
      )
      .all({ $limit: limit })
      .map((row) => ({
        executionId: row.execution_id,
        deleteExecutionStore: Boolean(row.prune_execution_stores),
      }));
  }

  /**
   * Up to `limit + 1` executions that match the request, newest first. `now`
   * is used to decide which executions are `running`. Tombstones match only
   * `status: "pruned"`.
   */
  listStates(
    request: PreparedExecutionListRequest,
    now: number
  ): SequencedExecutionState[] {
    // `running` is derived from a live lease, so the other statuses exclude
    // executions whose lease has not expired.
    const statusCondition =
      request.status === undefined
        ? "AND pruned_at IS NULL"
        : request.status === "pruned"
          ? "AND pruned_at IS NOT NULL"
          : request.status === "running"
            ? "AND pruned_at IS NULL AND lease_expires_at IS NOT NULL AND lease_expires_at > $now"
            : "AND pruned_at IS NULL AND status = $status AND (lease_expires_at IS NULL OR lease_expires_at <= $now)";
    const divergedCondition =
      request.diverged === undefined
        ? ""
        : request.diverged
          ? "AND divergence_code IS NOT NULL"
          : "AND divergence_code IS NULL";
    return this.db
      .query<ExecutionRow>(
        `SELECT * FROM executions
         WHERE ($workflowId IS NULL OR workflow_id = $workflowId)
           AND ($before IS NULL OR admission_sequence < $before)
           ${statusCondition}
           ${divergedCondition}
         ORDER BY admission_sequence DESC
         LIMIT $limit`
      )
      .all({
        $workflowId: request.workflowId ?? null,
        $before: request.beforeSequence ?? null,
        $limit: request.limit + 1,
        ...(request.status === undefined || request.status === "pruned"
          ? {}
          : request.status === "running"
            ? { $now: now }
            : { $now: now, $status: request.status }),
      })
      .map((row) => ({
        state: toState(row),
        admissionSequence: Number(row.admission_sequence),
      }));
  }

  /**
   * Returns false when the execution ID is already in use. The insert is
   * detected from its RETURNING row, not from `changes`, because Bun before
   * 1.4 counts the admission sequence trigger's updates in `changes`.
   */
  insertState(state: ExecutionState): boolean {
    const inserted = this.db
      .query<{ execution_id: string }>(
        `INSERT INTO executions (
           execution_id, workflow_id, params, context, status, outcome,
           admitted_at, updated_at, due_at, claimed_due_at, lease_token,
           lease_expires_at, unacknowledged_delivery_count, redeliver_at,
           next_claim_at, parent_execution_id, parent_step_key,
           parent_detached, cancellation_reason, cancellation_source,
           cancellation_forced, cancellation_requested_at,
           cancellation_delivered_at, workflow_version, divergence_code,
           divergence_step_key, divergence_workflow_version,
           divergence_detected_at, pruned_at, prune_cleanup_pending,
           prune_execution_stores
         ) VALUES (
           $executionId, $workflowId, $params, $context, $status, $outcome,
           $admittedAt, $updatedAt, $dueAt, $claimedDueAt, $leaseToken,
           $leaseExpiresAt, $unacknowledgedDeliveryCount, $redeliverAt,
           $nextClaimAt, $parentExecutionId, $parentStepKey,
           $parentDetached, $cancellationReason, $cancellationSource,
           $cancellationForced, $cancellationRequestedAt,
           $cancellationDeliveredAt, $workflowVersion, $divergenceCode,
           $divergenceStepKey, $divergenceWorkflowVersion,
           $divergenceDetectedAt, $prunedAt, $pruneCleanupPending,
           $pruneExecutionStores
         )
         ON CONFLICT(execution_id) DO NOTHING
         RETURNING execution_id`
      )
      .get({
        ...toBindings(state),
        $parentExecutionId: state.parent?.executionId ?? null,
        $parentStepKey: state.parent?.stepKey ?? null,
        $parentDetached: state.parent?.detached ? 1 : 0,
      });
    return inserted !== undefined;
  }

  /**
   * Writes a tombstone state over the row, and clears the parent link, which
   * `updateState` leaves alone.
   */
  replaceWithTombstone(state: ExecutionState) {
    if (state.pruned === null) {
      throw new Error(`Execution "${state.executionId}" is not a tombstone`);
    }
    this.updateState(state);
    this.db
      .query(
        `UPDATE executions SET
           parent_execution_id = NULL,
           parent_step_key = NULL,
           parent_detached = 0
         WHERE execution_id = $executionId`
      )
      .run({ $executionId: state.executionId });
  }

  /** Leaves the parent link as inserted: only a tombstone clears it. */
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
           next_claim_at = $nextClaimAt,
           cancellation_reason = $cancellationReason,
           cancellation_source = $cancellationSource,
           cancellation_forced = $cancellationForced,
           cancellation_requested_at = $cancellationRequestedAt,
           cancellation_delivered_at = $cancellationDeliveredAt,
           workflow_version = $workflowVersion,
           divergence_code = $divergenceCode,
           divergence_step_key = $divergenceStepKey,
           divergence_workflow_version = $divergenceWorkflowVersion,
           divergence_detected_at = $divergenceDetectedAt,
           pruned_at = $prunedAt,
           prune_cleanup_pending = $pruneCleanupPending,
           prune_execution_stores = $pruneExecutionStores
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
        ? {
            executionId: row.parent_execution_id,
            stepKey: row.parent_step_key,
            detached: Boolean(row.parent_detached),
          }
        : null,
    cancellation:
      row.cancellation_requested_at !== null
        ? {
            reason: row.cancellation_reason,
            source: row.cancellation_source,
            forced: Boolean(row.cancellation_forced),
            requestedAt: Number(row.cancellation_requested_at),
            deliveredAt: toNumberOrNull(row.cancellation_delivered_at),
          }
        : null,
    workflowVersion: toNumberOrNull(row.workflow_version),
    divergence:
      row.divergence_code !== null
        ? {
            code: row.divergence_code as ExecutionDivergenceState["code"],
            stepKey: row.divergence_step_key,
            workflowVersion: Number(row.divergence_workflow_version ?? 1),
            detectedAt: Number(row.divergence_detected_at ?? 0),
          }
        : null,
    pruned:
      row.pruned_at !== null
        ? {
            prunedAt: Number(row.pruned_at),
            cleanupPending: Boolean(row.prune_cleanup_pending),
            deleteExecutionStore: Boolean(row.prune_execution_stores),
          }
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
    $cancellationReason: state.cancellation?.reason ?? null,
    $cancellationSource: state.cancellation?.source ?? null,
    $cancellationForced:
      state.cancellation === null ? null : state.cancellation.forced ? 1 : 0,
    $cancellationRequestedAt: state.cancellation?.requestedAt ?? null,
    $cancellationDeliveredAt: state.cancellation?.deliveredAt ?? null,
    $workflowVersion: state.workflowVersion,
    $divergenceCode: state.divergence?.code ?? null,
    $divergenceStepKey: state.divergence?.stepKey ?? null,
    $divergenceWorkflowVersion: state.divergence?.workflowVersion ?? null,
    $divergenceDetectedAt: state.divergence?.detectedAt ?? null,
    $prunedAt: state.pruned?.prunedAt ?? null,
    $pruneCleanupPending: state.pruned?.cleanupPending ? 1 : 0,
    $pruneExecutionStores: state.pruned?.deleteExecutionStore ? 1 : 0,
  };
}

const TERMINAL_STATUSES = ["completed", "failed", "cancelled"] as const;

/**
 * The latest `updated_at` at which each policy's executions have expired at
 * `now`. `latest` is the latest of them, or null when no policy has a period.
 */
function retentionCutoffs(retain: RetentionOptions, now: number) {
  let latest: number | null = null;
  const cutoffsOf = (policy: RetentionOptions["default"]) => {
    const cutoffs: Partial<Record<(typeof TERMINAL_STATUSES)[number], number>> = {};
    for (const status of TERMINAL_STATUSES) {
      const period = policy?.[status];
      if (period === undefined) continue;
      cutoffs[status] = now - period;
      latest = latest === null ? now - period : Math.max(latest, now - period);
    }
    return cutoffs;
  };
  const workflows: [string, string, number][] = [];
  const policyWorkflowIds = Object.keys(retain.workflows ?? {});
  for (const workflowId of policyWorkflowIds) {
    for (const [status, cutoff] of Object.entries(cutoffsOf(retain.workflows![workflowId]))) {
      workflows.push([workflowId, status, cutoff]);
    }
  }
  return {
    default: cutoffsOf(retain.default),
    workflows,
    policyWorkflowIds,
    latest: latest as number | null,
  };
}

function toNumberOrNull(value: number | null) {
  return value === null ? null : Number(value);
}
