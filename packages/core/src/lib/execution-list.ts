import type {
  ExecutionListPage,
  ExecutionListRequest,
  ExecutionStatus,
} from "../base/execution";
import { ExecutionListRequestError } from "../base/execution";
import { isPlainObject } from "../base/durable-value";
import { executionStatusAt, toExecutionRecord, type ExecutionState } from "./execution-state";

export const DEFAULT_EXECUTION_LIST_LIMIT = 50;
export const MAX_EXECUTION_LIST_LIMIT = 1_000;

const EXECUTION_STATUSES: readonly ExecutionStatus[] = [
  "admitted",
  "running",
  "suspended",
  "completed",
  "failed",
];

export type PreparedExecutionListRequest = {
  workflowId?: string;
  status?: ExecutionStatus;
  /** Only executions whose admission sequence is below this one. */
  beforeSequence?: number;
  limit: number;
};

/**
 * An execution with the admission sequence its adapter assigned. Adapters
 * assign each new execution a sequence greater than that of every execution
 * in the backend, in the same atomic write as its insert.
 */
export type SequencedExecutionState = {
  state: ExecutionState;
  admissionSequence: number;
};

/**
 * Validates a list request and decodes its cursor. A limit above
 * `MAX_EXECUTION_LIST_LIMIT` is lowered to it.
 * @throws {ExecutionListRequestError} for an invalid filter, limit or cursor
 */
export function prepareExecutionListRequest(
  request: ExecutionListRequest | undefined
): PreparedExecutionListRequest {
  if (request === undefined) return { limit: DEFAULT_EXECUTION_LIST_LIMIT };
  if (!isPlainObject(request)) {
    throw new ExecutionListRequestError("A list request must be an object");
  }
  const { workflowId, status, cursor, limit } = request as Record<string, unknown>;
  if (workflowId !== undefined && typeof workflowId !== "string") {
    throw new ExecutionListRequestError("workflowId must be a string");
  }
  if (status !== undefined && !EXECUTION_STATUSES.includes(status as ExecutionStatus)) {
    throw new ExecutionListRequestError(
      `status must be one of ${EXECUTION_STATUSES.join(", ")}`
    );
  }
  if (
    limit !== undefined &&
    (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1)
  ) {
    throw new ExecutionListRequestError("limit must be a positive integer");
  }
  return {
    workflowId,
    status: status as ExecutionStatus | undefined,
    beforeSequence: cursor === undefined ? undefined : decodeExecutionListCursor(cursor),
    limit: Math.min(
      (limit as number | undefined) ?? DEFAULT_EXECUTION_LIST_LIMIT,
      MAX_EXECUTION_LIST_LIMIT
    ),
  };
}

export function matchesExecutionListRequest(
  state: ExecutionState,
  request: PreparedExecutionListRequest,
  now: number
): boolean {
  return (
    (request.workflowId === undefined || state.workflowId === request.workflowId) &&
    (request.status === undefined || executionStatusAt(state, now) === request.status)
  );
}

/**
 * Builds a page from up to `limit + 1` matching executions, newest first.
 * Adapters fetch one more than the limit, so the last page carries no cursor.
 */
export function toExecutionListPage(
  matches: readonly SequencedExecutionState[],
  request: PreparedExecutionListRequest,
  now: number
): ExecutionListPage {
  const page = matches.slice(0, request.limit);
  const result: ExecutionListPage = {
    executions: page.map(({ state }) => toExecutionRecord(state, now)),
  };
  if (matches.length > request.limit) {
    result.cursor = String(page[page.length - 1].admissionSequence);
  }
  return result;
}

function decodeExecutionListCursor(cursor: unknown): number {
  if (typeof cursor !== "string") {
    throw new ExecutionListRequestError("cursor must be a string");
  }
  const sequence = /^\d+$/.test(cursor) ? Number(cursor) : NaN;
  if (!Number.isSafeInteger(sequence)) {
    throw new ExecutionListRequestError("cursor is not a list cursor");
  }
  return sequence;
}
