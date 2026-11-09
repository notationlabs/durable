import pino from "pino";
import { WorkflowRunner } from "@notation/durable";
import { createWorkflowWorker } from "@notation/durable/worker";
import { runtime, workflowRouter } from "./shared";

const logger = pino();

const workflowRunner = new WorkflowRunner(runtime, {
  router: workflowRouter,
  logger,
});

const worker = createWorkflowWorker(workflowRunner, logger);

worker.listen();
