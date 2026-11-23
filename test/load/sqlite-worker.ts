import pino from "pino";
import { WorkflowRunner } from "@durable/core";
import { loadRouter } from "@durable/runtime-load";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createWorkflowWorker } from "@durable/worker-invoker";
import { openDb } from "./sqlite-db";

const logger = pino({ level: process.env.LOAD_LOG_LEVEL ?? "error" });
const runtime = new SqliteRuntime(await openDb());
createWorkflowWorker(new WorkflowRunner(runtime, { router: loadRouter, logger }), logger).listen();
