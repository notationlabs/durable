import type { ExecutionClient } from "../base/execution";
import {
  isTerminalExecutionView,
  toExecutionView,
  type ExecutionControl,
} from "../base/observation";
import { prepareExecutionCancel } from "./execution-state";

/**
 * Thrown by `cancel` for an empty execution ID, a `reason` that is not a
 * string, or a `force` that is not a boolean.
 */
export class ExecutionCancelRequestError extends TypeError {
  constructor(message: string) {
    super(message);
    this.name = "ExecutionCancelRequestError";
  }
}

/**
 * Cancels executions through `executions`. After the request is recorded,
 * `cancel` reads the record once to build its result, so a `requested`
 * result carries the view at that read. An execution that reached a
 * terminal status between the two calls resolves with its terminal view.
 */
export function createExecutionControl(
  executions: Pick<ExecutionClient, "cancel" | "get">
): ExecutionControl {
  return {
    async cancel(executionId, options) {
      const prepared = prepareExecutionCancel({ ...options, executionId });
      if (!prepared.ok) throw new ExecutionCancelRequestError(prepared.message);
      const result = await executions.cancel({
        executionId: prepared.executionId,
        ...prepared.request,
      });
      if (result.status === "rejected") return undefined;
      const record = await executions.get(prepared.executionId);
      if (!record) return undefined;
      const view = toExecutionView(record);
      if (isTerminalExecutionView(view)) return view;
      return { status: "requested", execution: view };
    },
  };
}
