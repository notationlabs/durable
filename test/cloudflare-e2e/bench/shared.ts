export const sizes = {
  chainSteps: 100,
  payloadSteps: 20,
  payloadBytes: 100_000,
  sleeps: 5,
  sleepMs: 1_000,
  fanOut: 20,
  fanOutMs: 100,
  children: 5,
  burst: 100,
};

export const SCENARIOS = [
  "start",
  "chain",
  "payload",
  "sleep",
  "fanout",
  "signal",
  "children",
  "burst",
] as const;
export type Scenario = (typeof SCENARIOS)[number];

export const SYSTEMS = ["native", "durable", "inline"] as const;
export type SystemName = (typeof SYSTEMS)[number];

/** The `Bench` object that drives a run, and the name of one measurement in it. */
export type Run = { bench: string; run: string };

export type Namespace<Stub = unknown> = {
  idFromName(name: string): unknown;
  get(id: unknown): Stub;
};

export type Clock = {
  mark(run: string, label: string, call: 0 | 1): Promise<void>;
};

// Reports that a step function has started, by calling the `Bench` object
// that is the clock of the run. The second call leaves when the first has
// returned, so the time between the two arrivals is one round trip to the
// clock. The driver subtracts half of it from the first arrival to find when
// the step function started, and adds half to the second to find when it
// carried on.
export async function mark(env: { BENCH: Namespace<Clock> }, run: Run, label: string) {
  const stub = env.BENCH.get(env.BENCH.idFromName(run.bench));
  await stub.mark(run.run, label, 0);
  await stub.mark(run.run, label, 1);
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const range = (n: number) => Array.from({ length: n }, (_, i) => i);
