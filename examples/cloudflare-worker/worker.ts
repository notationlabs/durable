import { createWorkflowRouter } from "@notation/durable";
import {
  createWorkflowObject,
  type DurableObjectStateLike,
} from "@notation/durable/cloudflare";
import { simpleWorkflow } from "../workflows/simple";
import { dynamicWorkflow } from "../workflows/dynamic";

const router = createWorkflowRouter({
  "simple-workflow": simpleWorkflow,
  "dynamic-workflow": dynamicWorkflow,
});

export type WorkflowRouter = typeof router;

type Env = {
  WORKFLOWS: {
    idFromName(name: string): unknown;
    get(id: unknown): { fetch(request: Request): Promise<Response> };
  };
};

// Objects hold executions. Its SQLite storage is the runtime.
export class Workflows {
  private durable;

  constructor(ctx: DurableObjectStateLike) {
    this.durable = createWorkflowObject(ctx, { router, basePath: "/durable" });
  }

  fetch(request: Request) {
    return this.durable.fetch(request);
  }

  alarm() {
    return this.durable.alarm();
  }
}

export default {
  // Serves /durable/trigger, /durable/wait and the other routes. Add
  // authentication here before exposing them.
  fetch(request: Request, env: Env) {
    const object = env.WORKFLOWS.get(env.WORKFLOWS.idFromName("default"));
    return object.fetch(request);
  },
};
