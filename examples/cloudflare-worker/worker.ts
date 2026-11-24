import { createWorkflowRouter } from "@notation/durable";
import { WorkflowObject } from "@notation/durable/cloudflare";
import { simpleWorkflow } from "../workflows/simple";
import { dynamicWorkflow } from "../workflows/dynamic";

const router = createWorkflowRouter({
  "simple-workflow": simpleWorkflow,
  "dynamic-workflow": dynamicWorkflow,
});

export type WorkflowRouter = typeof router;

export class Workflows extends WorkflowObject({ router, basePath: "/durable" }) {}

export default {
  // Serves /durable/trigger, /durable/wait and the other routes. Add
  // authentication here before exposing them.
  fetch: (request, env) => env.WORKFLOWS.getByName("default").fetch(request),
} satisfies ExportedHandler<Env>;
