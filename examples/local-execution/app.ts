import pino from "pino";
import { ExecutionDispatcher, createLocalSdk } from "@notation/durable";
import { createWorkflowInvoker } from "@notation/durable/worker";
import { runtime } from "./shared";
import type { WorkflowRouter } from "./shared";

const logger = pino();
const workerPath = new URL("worker.ts", import.meta.url).href;

export const invoker = createWorkflowInvoker({ workerPath, logger });
export const sdk = createLocalSdk<WorkflowRouter>(runtime, { invoker });

const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger });
dispatcher.start();

try {
  const result = await sdk.triggerAndWait({
    workflowId: "dynamic-workflow",
    params: { msg: "hello" },
  });
  console.log(result);
} finally {
  await dispatcher.stop();
}
