import { isPrunedExecution, type ExecutionClient, type ExecutionRecord } from "@yieldstar/core";

/**
 * Reads an execution's live record. Throws when the execution was never
 * admitted or was pruned.
 */
export async function liveRecord(
  executions: Pick<ExecutionClient, "get">,
  executionId: string
): Promise<ExecutionRecord> {
  const found = await executions.get(executionId);
  if (found === undefined || isPrunedExecution(found)) {
    throw new Error(
      `expected "${executionId}" to have a live record, found ${JSON.stringify(found)}`
    );
  }
  return found;
}

/**
 * Reads an execution's live record, or undefined when it was never admitted.
 * Throws when the execution was pruned.
 */
export async function findLiveRecord(
  executions: Pick<ExecutionClient, "get">,
  executionId: string
): Promise<ExecutionRecord | undefined> {
  const found = await executions.get(executionId);
  if (found !== undefined && isPrunedExecution(found)) {
    throw new Error(`expected "${executionId}" not to be pruned, found ${JSON.stringify(found)}`);
  }
  return found;
}
