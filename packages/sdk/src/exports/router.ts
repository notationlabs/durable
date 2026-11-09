import type { WorkflowRouter } from "@durable/core";

export function createWorkflowRouter<W extends WorkflowRouter>(
  workflows: W
): W {
  return workflows;
}
