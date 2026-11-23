import { createWorkflowRouter, workflow } from "@durable/sdk";
import { LOAD_DELAY_MS, type LoadWorkflowId } from "./kinds";

// Every step has an explicit key, so the workflows stay safe to replay after
// a deploy changes the code around them.

const single = workflow(async function* (step) {
  return yield* step.run("only", () => 1);
});

const ten = workflow(async function* (step) {
  let total = 0;
  for (let i = 0; i < 10; i++) {
    total += yield* step.run(`step-${i}`, () => i);
  }
  return total;
});

const delay = workflow(async function* (step) {
  const before = yield* step.run("before", () => Date.now());
  yield* step.delay("wait", LOAD_DELAY_MS);
  const after = yield* step.run("after", () => Date.now());
  return after - before;
});

const child = workflow<{ n: number }>(async function* (step, event) {
  return yield* step.run("double", () => event.params.n * 2);
});

const parent = workflow(async function* (step) {
  const first = yield* step.start("start-1", { workflowId: "load-child", params: { n: 1 } });
  const second = yield* step.start("start-2", { workflowId: "load-child", params: { n: 2 } });
  const one = yield* step.join<number>("join-1", first.executionId);
  const two = yield* step.join<number>("join-2", second.executionId);
  return one + two;
});

class LoadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LoadError";
  }
}

const fail = workflow(async function* (step) {
  yield* step.run("before", () => 1);
  yield* step.run("fail", (): number => {
    throw new LoadError("deliberate failure");
  });
  return "unreachable";
});

/** The workflows the load driver triggers. Every host under load mounts all of them. */
export const loadRouter = createWorkflowRouter({
  "load-single": single,
  "load-ten": ten,
  "load-delay": delay,
  "load-parent": parent,
  "load-child": child,
  "load-fail": fail,
} satisfies Record<LoadWorkflowId, unknown>);

export type LoadRouter = typeof loadRouter;
