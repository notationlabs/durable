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

export const EXECUTION_STATUSES: readonly ExecutionStatus[] = [
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
 * assign each new execution a sequence greater than every earlier one,
 * including executions since deleted, in the same atomic write as its insert.
 */
export type SequencedExecutionState = {
  state: ExecutionState;
  admissionSequence: number;
};

/**
 * Validates a list request and decodes its cursor. A limit above
 * `MAX_EXECUTION_LIST_LIMIT` is lowered to it. A `null` field is treated as
 * absent, since JSON clients send `null` for a field they leave out.
 * @throws {ExecutionListRequestError} for an invalid filter, limit or cursor
 */
export function prepareExecutionListRequest(
  request: ExecutionListRequest | undefined
): PreparedExecutionListRequest {
  if (request === undefined) return { limit: DEFAULT_EXECUTION_LIST_LIMIT };
  if (!isPlainObject(request)) {
    throw new ExecutionListRequestError("A list request must be an object");
  }
  const fields = request as Record<string, unknown>;
  const workflowId = fields.workflowId ?? undefined;
  const status = fields.status ?? undefined;
  const cursor = fields.cursor ?? undefined;
  const limit = fields.limit ?? undefined;
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
  const prepared: PreparedExecutionListRequest = {
    limit: Math.min(
      (limit as number | undefined) ?? DEFAULT_EXECUTION_LIST_LIMIT,
      MAX_EXECUTION_LIST_LIMIT
    ),
  };
  if (workflowId !== undefined) prepared.workflowId = workflowId;
  if (status !== undefined) prepared.status = status as ExecutionStatus;
  if (cursor !== undefined) prepared.beforeSequence = decodeExecutionListCursor(cursor);
  return prepared;
}

/** Whether a state matches the request's filters at `now`. */
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
    result.cursor = encodeExecutionListCursor(page[page.length - 1].admissionSequence);
  }
  return result;
}

const CURSOR_PREFIX = "v1:";

export function encodeExecutionListCursor(admissionSequence: number): string {
  return toBase64Url(`${CURSOR_PREFIX}${admissionSequence}`);
}

function decodeExecutionListCursor(cursor: unknown): number {
  if (typeof cursor !== "string") {
    throw new ExecutionListRequestError("cursor must be a string");
  }
  const decoded = fromBase64Url(cursor);
  const match = decoded?.startsWith(CURSOR_PREFIX)
    ? /^\d+$/.exec(decoded.slice(CURSOR_PREFIX.length))
    : null;
  const sequence = match ? Number(match[0]) : NaN;
  if (!Number.isSafeInteger(sequence)) {
    throw new ExecutionListRequestError("cursor is not a list cursor");
  }
  return sequence;
}

function toBase64Url(text: string): string {
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromBase64Url(value: string): string | undefined {
  if (!/^[A-Za-z0-9_-]*$/.test(value)) return undefined;
  try {
    return atob(value.replace(/-/g, "+").replace(/_/g, "/"));
  } catch {
    return undefined;
  }
}
