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
 * Cancels executions through `executions`. If an execution ends between the
 * cancel and the read that follows, the result is its terminal view.
 */
export function createExecutionControl(
  executions: Pick<ExecutionClient, "cancel" | "clearDivergence" | "get">
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

    async clearDivergence(executionId) {
      if (typeof executionId !== "string" || executionId === "") {
        throw new TypeError("executionId must be a non-empty string");
      }
      const result = await executions.clearDivergence(executionId);
      if (result.status === "rejected") return undefined;
      return result;
    },
  };
}
