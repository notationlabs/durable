import { env } from "cloudflare:workers";
import {
  createWorkflowRouter,
  defineStore,
  workflow,
  type StandardSchemaV1,
} from "../../../packages/durable/dist/index.js";
import { mark as markOn, range, sizes, sleep, type Run } from "./shared";

const mark = (run: Run, label: string) => markOn(env, run, label);

function schema<T>(): StandardSchemaV1<unknown, T> {
  return {
    "~standard": { version: 1, vendor: "bench", validate: (value) => ({ value: value as T }) },
  };
}

export const Gate = defineStore("gate", schema<{ open: boolean }>(), {
  initial: () => ({ open: false }),
});

const start = workflow<Run>(async function* (step, event) {
  yield* step.run("start", () => mark(event.params, "start"));
});

const chain = workflow<Run>(async function* (step, event) {
  yield* step.run("a", () => mark(event.params, "a"));
  for (let i = 0; i < sizes.chainSteps; i++) yield* step.run(`s${i}`, () => i);
  yield* step.run("b", () => mark(event.params, "b"));
});

const payload = workflow<Run>(async function* (step, event) {
  yield* step.run("a", () => mark(event.params, "a"));
  for (let i = 0; i < sizes.payloadSteps; i++) {
    yield* step.run(`s${i}`, () => "x".repeat(sizes.payloadBytes));
  }
  yield* step.run("b", () => mark(event.params, "b"));
});

const sleeper = workflow<Run>(async function* (step, event) {
  for (let i = 0; i < sizes.sleeps; i++) {
    yield* step.run(`t${i}`, () => mark(event.params, `t${i}`));
    yield* step.delay(`d${i}`, sizes.sleepMs);
  }
  yield* step.run("end", () => mark(event.params, `t${sizes.sleeps}`));
});

const fanout = workflow<Run>(async function* (step, event) {
  yield* step.run("a", () => mark(event.params, "a"));
  yield* step.all(
    "fan",
    range(sizes.fanOut).map((i) =>
      step.run(`f${i}`, async () => {
        await sleep(sizes.fanOutMs);
        return i;
      })
    )
  );
  yield* step.run("b", () => mark(event.params, "b"));
});

const signal = workflow<Run>(async function* (step, event) {
  const gate = step.store(Gate, { id: event.params.run });
  yield* gate.when("go", (state) => (state.open ? true : undefined));
  yield* step.run("woke", () => mark(event.params, "woke"));
});

const child = workflow<{ index: number }>(async function* (step, event) {
  return yield* step.run("double", () => event.params.index * 2);
});

const children = workflow<Run>(async function* (step, event) {
  yield* step.run("a", () => mark(event.params, "a"));
  yield* step.all(
    "join",
    range(sizes.children).map((index) =>
      step.call<typeof child>(`child-${index}`, { workflowId: "child", params: { index } })
    )
  );
  yield* step.run("b", () => mark(event.params, "b"));
});

const burst = workflow<Run & { index: number }>(async function* (step, event) {
  yield* step.run("one", () => 1);
  yield* step.run("two", () => 2);
  yield* step.run("done", () => mark(event.params, `done${event.params.index}`));
});

export const router = createWorkflowRouter({
  start,
  chain,
  payload,
  sleep: sleeper,
  fanout,
  signal,
  child,
  children,
  burst,
});

export type Router = typeof router;
