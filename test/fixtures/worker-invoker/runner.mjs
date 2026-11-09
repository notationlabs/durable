import { WorkflowRunner } from "@durable/core";
import { MemoryRuntime } from "@durable/test-runtime";
import { createWorkflowWorker } from "@durable/worker-invoker";
import { leaseLocally } from "./lease-locally.mjs";
import { parityRouter } from "./parity-workflows.mjs";

const logger = { info() {}, error() {} };
const runtime = new MemoryRuntime();
const runner = new WorkflowRunner(runtime, {
  router: parityRouter,
  logger,
});

createWorkflowWorker(leaseLocally(runtime, runner), logger).listen();
