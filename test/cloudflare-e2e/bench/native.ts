import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import { mark as markOn, range, sizes, sleep, type Clock, type Namespace, type Run, type Scenario } from "./shared";

export type NativeInstance = {
  id: string;
  status(): Promise<{ status: string; error?: { name: string; message: string } }>;
  sendEvent(event: { type: string; payload: unknown }): Promise<void>;
};

export type NativeBinding = {
  create(options: { id?: string; params: unknown }): Promise<NativeInstance>;
  createBatch(options: {
    instances: { id?: string; params: unknown }[];
  }): Promise<{ created: NativeInstance[]; errors: { index: number; message: string }[] }>;
  get(id: string): Promise<NativeInstance>;
};

type Env = { BENCH: Namespace<Clock>; NATIVE: NativeBinding };

export type NativeParams =
  | (Run & { scenario: Exclude<Scenario, "burst"> })
  | (Run & { scenario: "burst"; index: number })
  | { scenario: "child"; parent: string; index: number };

// The same workflows as `durable.ts`, written for Cloudflare Workflows. One
// class holds them all, so the Worker binds one workflow.
export class NativeWorkflow extends WorkflowEntrypoint<Env, NativeParams> {
  async run(event: WorkflowEvent<NativeParams>, step: WorkflowStep) {
    const params = event.payload;
    if (params.scenario === "child") return this.child(params, step);
    const mark = (label: string) => markOn(this.env, params, label);

    switch (params.scenario) {
      case "start":
        await step.do("start", () => mark("start"));
        return;

      case "chain":
        await step.do("a", () => mark("a"));
        for (let i = 0; i < sizes.chainSteps; i++) await step.do(`s${i}`, async () => i);
        await step.do("b", () => mark("b"));
        return;

      case "payload":
        await step.do("a", () => mark("a"));
        for (let i = 0; i < sizes.payloadSteps; i++) {
          await step.do(`s${i}`, async () => "x".repeat(sizes.payloadBytes));
        }
        await step.do("b", () => mark("b"));
        return;

      case "sleep":
        for (let i = 0; i < sizes.sleeps; i++) {
          await step.do(`t${i}`, () => mark(`t${i}`));
          await step.sleep(`d${i}`, sizes.sleepMs);
        }
        await step.do("end", () => mark(`t${sizes.sleeps}`));
        return;

      case "fanout":
        await step.do("a", () => mark("a"));
        await Promise.all(
          range(sizes.fanOut).map((i) =>
            step.do(`f${i}`, async () => {
              await sleep(sizes.fanOutMs);
              return i;
            })
          )
        );
        await step.do("b", () => mark("b"));
        return;

      case "signal":
        await step.waitForEvent("go", { type: "go", timeout: "5 minutes" });
        await step.do("woke", () => mark("woke"));
        return;

      // Cloudflare Workflows has no step that waits for another instance. The
      // parent creates each child in a step and waits for an event, and the
      // child's last step sends that event to the parent.
      case "children":
        await step.do("a", () => mark("a"));
        await Promise.all(
          range(sizes.children).map(async (index) => {
            await step.do(`start-${index}`, async () => {
              try {
                await this.env.NATIVE.create({
                  id: `${event.instanceId}-c${index}`,
                  params: { scenario: "child", parent: event.instanceId, index },
                });
              } catch (err) {
                // A second attempt of this step finds the child it created.
                if (!String(err).includes("already_exists")) throw err;
              }
            });
            await step.waitForEvent(`join-${index}`, {
              type: `child-${index}`,
              timeout: "5 minutes",
            });
          })
        );
        await step.do("b", () => mark("b"));
        return;

      case "burst":
        await step.do("one", async () => 1);
        await step.do("two", async () => 2);
        await step.do("done", () => mark(`done${params.index}`));
        return;

    }
  }

  private async child(params: { parent: string; index: number }, step: WorkflowStep) {
    const value = await step.do("double", async () => params.index * 2);
    await step.do("report", async () => {
      const parent = await this.env.NATIVE.get(params.parent);
      await parent.sendEvent({ type: `child-${params.index}`, payload: { value } });
    });
    return value;
  }
}
