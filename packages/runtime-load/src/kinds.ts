/** How long the `delay` kind waits between its two steps, in milliseconds. */
export const LOAD_DELAY_MS = 1000;

/** The IDs the load router mounts its workflows under. */
export type LoadWorkflowId =
  | "load-single"
  | "load-ten"
  | "load-delay"
  | "load-parent"
  | "load-child"
  | "load-fail";

export type LoadKind = "single" | "ten" | "delay" | "parent" | "fail" | "child";

/**
 * What the driver expects of each kind: the workflow it triggers, the status
 * it must end in, and how long its delays take by design. The driver does not
 * trigger `child` itself; `parent` starts two of them.
 */
export const loadKinds: Record<
  LoadKind,
  { workflowId: LoadWorkflowId; expected: "completed" | "failed"; delayMs: number }
> = {
  single: { workflowId: "load-single", expected: "completed", delayMs: 0 },
  ten: { workflowId: "load-ten", expected: "completed", delayMs: 0 },
  delay: { workflowId: "load-delay", expected: "completed", delayMs: LOAD_DELAY_MS },
  parent: { workflowId: "load-parent", expected: "completed", delayMs: 0 },
  fail: { workflowId: "load-fail", expected: "failed", delayMs: 0 },
  child: { workflowId: "load-child", expected: "completed", delayMs: 0 },
};
