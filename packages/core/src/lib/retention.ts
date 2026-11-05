import { isTerminalExecutionStatus } from "../base/execution";
import { isPlainObject } from "../base/durable-value";
import type { RetentionOptions, RetentionPolicy } from "../base/retention";
import type { ExecutionState } from "./execution-state";

/** The number of executions one pruning pass tombstones and cleans at most. */
export const DEFAULT_PRUNE_BATCH_SIZE = 100;

/** Milliseconds between the dispatcher's pruning passes. */
export const DEFAULT_PRUNE_INTERVAL = 60_000;

const PERIODS = ["completed", "failed", "cancelled"] as const;

/**
 * Validates retention options and returns a copy, so later changes to the
 * caller's object do not apply. Returns undefined for undefined.
 * @throws {TypeError} for a period that is not a finite number of
 * milliseconds at or above zero, or a field of the wrong type
 */
export function validateRetentionOptions(
  retain: RetentionOptions | undefined
): RetentionOptions | undefined {
  if (retain === undefined) return undefined;
  if (!isPlainObject(retain)) throw new TypeError("retain must be an object");
  const copy: RetentionOptions = {};
  if (retain.default !== undefined) {
    copy.default = validateRetentionPolicy(retain.default, "retain.default");
  }
  if (retain.workflows !== undefined) {
    if (!isPlainObject(retain.workflows)) {
      throw new TypeError("retain.workflows must be an object");
    }
    copy.workflows = {};
    for (const [workflowId, policy] of Object.entries(retain.workflows)) {
      copy.workflows[workflowId] = validateRetentionPolicy(
        policy,
        `retain.workflows[${JSON.stringify(workflowId)}]`
      );
    }
  }
  return copy;
}

function validateRetentionPolicy(policy: unknown, label: string): RetentionPolicy {
  if (!isPlainObject(policy)) throw new TypeError(`${label} must be an object`);
  const fields = policy as Record<string, unknown>;
  const copy: RetentionPolicy = {};
  for (const status of PERIODS) {
    const period = fields[status];
    if (period === undefined) continue;
    if (typeof period !== "number" || !Number.isFinite(period) || period < 0) {
      throw new TypeError(
        `${label}.${status} must be a finite number of milliseconds at or above 0`
      );
    }
    copy[status] = period;
  }
  const pruneExecutionStores = fields.pruneExecutionStores;
  if (pruneExecutionStores !== undefined) {
    if (typeof pruneExecutionStores !== "boolean") {
      throw new TypeError(`${label}.pruneExecutionStores must be a boolean`);
    }
    copy.pruneExecutionStores = pruneExecutionStores;
  }
  return copy;
}

/** The workflow's own policy when it has one, and the default otherwise. */
export function retentionPolicyFor(
  retain: RetentionOptions,
  workflowId: string
): RetentionPolicy | undefined {
  const workflows = retain.workflows;
  if (workflows !== undefined && Object.hasOwn(workflows, workflowId)) {
    return workflows[workflowId];
  }
  return retain.default;
}

/**
 * Whether the execution has passed its retention period at `now`: it is
 * terminal, not pruned, and its policy's period for its status has passed
 * since `updatedAt`, when the outcome was recorded. Adapters also check that
 * no join waiter with a non-terminal joiner is registered on it, in the same
 * atomic write as the tombstone.
 */
export function isExecutionExpired(
  state: ExecutionState,
  retain: RetentionOptions,
  now: number
): boolean {
  if (state.pruned !== null || !isTerminalExecutionStatus(state.status)) return false;
  const period = retentionPolicyFor(retain, state.workflowId)?.[state.status];
  return period !== undefined && state.updatedAt + period <= now;
}

/** Whether the execution's policy deletes its stores when it is pruned. */
export function prunesExecutionStores(
  retain: RetentionOptions,
  workflowId: string
): boolean {
  return retentionPolicyFor(retain, workflowId)?.pruneExecutionStores === true;
}
