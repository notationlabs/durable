/**
 * How long a workflow's terminal executions are kept before they are pruned.
 * Each period is in milliseconds after the terminal outcome was recorded. An
 * omitted period keeps executions with that outcome forever.
 */
export type RetentionPolicy = {
  completed?: number;
  failed?: number;
  cancelled?: number;
  /**
   * Also deletes every store whose `id` equals the execution ID when the
   * execution is pruned. Other executions and external code may read such a
   * store by its ID, so it is kept by default.
   * @default false
   */
  pruneExecutionStores?: boolean;
};

/**
 * Retention policies, read by the runtime when pruning runs. A workflow's
 * entry replaces `default` as a whole. With no policy for a workflow, its
 * executions are never pruned.
 */
export type RetentionOptions = {
  default?: RetentionPolicy;
  /** Keyed by workflow ID. */
  workflows?: Record<string, RetentionPolicy>;
};

/**
 * What a pruned execution leaves in place of its record. Tombstones are kept
 * forever, so a pruned execution ID is never admitted again.
 */
export type ExecutionTombstone = {
  executionId: string;
  workflowId: string;
  terminalStatus: "completed" | "failed" | "cancelled";
  prunedAt: number;
};

/** What `ExecutionClient.get` and `list` return for a pruned execution. */
export type PrunedExecutionRecord = ExecutionTombstone & { status: "pruned" };

export function isPrunedExecution(value: unknown): value is PrunedExecutionRecord {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { status?: unknown }).status === "pruned"
  );
}

/**
 * Thrown by `triggerAndWait` and `waitForResult` when the execution was
 * pruned: its outcome no longer exists.
 */
export class ExecutionPrunedError extends Error {
  readonly code = "EXECUTION_PRUNED";
  readonly executionId: string;
  readonly workflowId: string;
  readonly terminalStatus: ExecutionTombstone["terminalStatus"];
  readonly prunedAt: number;

  constructor(tombstone: ExecutionTombstone) {
    super(
      `Execution "${tombstone.executionId}" ended ${tombstone.terminalStatus} and was pruned; its outcome is no longer kept`
    );
    this.name = "ExecutionPrunedError";
    this.executionId = tombstone.executionId;
    this.workflowId = tombstone.workflowId;
    this.terminalStatus = tombstone.terminalStatus;
    this.prunedAt = tombstone.prunedAt;
  }
}
