// The parts of `cloudflare:workers` that the bench Worker uses and the
// Cloudflare package's own declaration leaves out.
declare module "cloudflare:workers" {
  export const env: any;

  export type WorkflowEvent<Params> = { payload: Readonly<Params>; instanceId: string };

  export type WorkflowStep = {
    do<T>(name: string, callback: () => Promise<T>): Promise<T>;
    sleep(name: string, duration: number | string): Promise<void>;
    waitForEvent<T>(
      name: string,
      options: { type: string; timeout?: number | string }
    ): Promise<{ payload: T; type: string }>;
  };

  export abstract class WorkflowEntrypoint<Env = unknown, Params = unknown> {
    protected env: Env;
    abstract run(event: WorkflowEvent<Params>, step: WorkflowStep): Promise<unknown>;
  }
}
