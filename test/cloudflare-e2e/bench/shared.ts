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

export const systemTitles: Record<SystemName, string> = {
  native: "Cloudflare Workflows",
  durable: "Durable",
  inline: "Durable, deliveries in the object",
};

export const workflowTitles: Record<Scenario, string> = {
  start: "One step",
  chain: `${sizes.chainSteps} steps in sequence`,
  payload: `${sizes.payloadSteps} steps that each return ${sizes.payloadBytes / 1000} KB`,
  sleep: `${sizes.sleeps} sleeps of ${sizes.sleepMs / 1000} s`,
  fanout: `${sizes.fanOut} concurrent steps that each wait ${sizes.fanOutMs} ms`,
  signal: "A wait for a signal from outside",
  children: `A parent that runs ${sizes.children} one-step children`,
  burst: `${sizes.burst} three-step executions triggered at once`,
};

/** The `Bench` object that drives a run, and the name of one measurement in it. */
export type Run = {
  bench: string;
  run: string;
  /** Set by the cost pass. The step functions then make no call to the clock. */
  quiet?: boolean;
};

/** What one Durable object has been billed for since it was constructed. */
export type Usage = {
  /** Changes when Cloudflare starts the object again, which resets the counts. */
  instance: string;
  /** Requests to the trigger route. */
  trigger: number;
  /** Requests to the poll and wait routes, which a caller chooses to send. */
  observe: number;
  alarm: number;
  /** Calls to the object's own RPC methods, such as the one that sends a signal. */
  rpc: number;
  /** With isolated deliveries, each is also one request to the instance that runs it. */
  deliveries: number;
  /** The wall time of the deliveries, which is billed to the instances that run them. */
  deliveryMs: number;
  /** The time with a trigger, an alarm, an RPC call or a delivery in progress. */
  activeMs: number;
  rowsRead: number;
  rowsWritten: number;
  /** Calls to `setAlarm` and `deleteAlarm`. */
  alarmWrites: number;
  storedBytes: number;
};

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
  if (run.quiet) return;
  const stub = env.BENCH.get(env.BENCH.idFromName(run.bench));
  await stub.mark(run.run, label, 0);
  await stub.mark(run.run, label, 1);
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const range = (n: number) => Array.from({ length: n }, (_, i) => i);
