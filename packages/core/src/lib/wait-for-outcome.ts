import type { EventEmitter } from "node:events";
import type { ExecutionClient, ExecutionOutcome } from "../base/execution";
import {
  DEFAULT_OBSERVER_POLL_INTERVAL,
  waitForExecutionRecord,
} from "./execution-observer";

/**
 * Resolves with an execution's outcome once its record is terminal, or with
 * undefined if no record exists. The record is the source of truth, so this
 * settles even if the process that ran the execution restarted. An optional
 * `emitter` (the invoker's deprecated `workflowEndEmitter`) only nudges an
 * early re-read.
 */
export async function waitForExecutionOutcome(
  executions: Pick<ExecutionClient, "get">,
  executionId: string,
  options?: { emitter?: EventEmitter; pollInterval?: number; signal?: AbortSignal }
): Promise<ExecutionOutcome | undefined> {
  const record = await waitForExecutionRecord(executions, executionId, {
    pollInterval: options?.pollInterval ?? DEFAULT_OBSERVER_POLL_INTERVAL,
    deadline: Infinity,
    notifier: options?.emitter,
    signal: options?.signal,
  });
  return record?.outcome;
}
