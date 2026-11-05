import type { WorkflowRunOutcome } from "@yieldstar/core";

/**
 * Drops `workflowVersion` from a delivery's outcome, for tests that compare
 * outcomes and are not about versions.
 */
export function withoutVersion(outcome: WorkflowRunOutcome): WorkflowRunOutcome {
  const { workflowVersion: _, ...rest } = outcome;
  return rest as WorkflowRunOutcome;
}
