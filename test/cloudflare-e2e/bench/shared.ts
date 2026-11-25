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

// Workflows whose step functions wait, as a call to another service does.
// Only the cost pass runs them: how long a waiting step takes says nothing
// about either system, and what it costs does. `burst` executions start at once.
export const WAITS = {
  wait: { steps: 1, ms: 1_000, burst: 1 },
  longWait: { steps: 1, ms: 30_000, burst: 1 },
  waits: { steps: 10, ms: 1_000, burst: 1 },
  waitBurst: { steps: 1, ms: 1_000, burst: 100 },
} as const;
export type WaitScenario = keyof typeof WAITS;
export const WAIT_SCENARIOS = Object.keys(WAITS) as WaitScenario[];
export const COST_SCENARIOS = [...SCENARIOS, ...WAIT_SCENARIOS];
export type CostScenario = Scenario | WaitScenario;

export const SYSTEMS = ["native", "durable", "inline"] as const;
export type SystemName = (typeof SYSTEMS)[number];

export const systemTitles: Record<SystemName, string> = {
  native: "Cloudflare Workflows",
  durable: "Durable",
  inline: "Durable, deliveries in the object",
};

export const workflowTitles: Record<CostScenario, string> = {
  start: "One step",
  chain: `${sizes.chainSteps} steps in sequence`,
  payload: `${sizes.payloadSteps} steps that each return ${sizes.payloadBytes / 1000} KB`,
  sleep: `${sizes.sleeps} sleeps of ${sizes.sleepMs / 1000} s`,
  fanout: `${sizes.fanOut} concurrent steps that each wait ${sizes.fanOutMs} ms`,
  signal: "A wait for a signal from outside",
  children: `A parent that runs ${sizes.children} one-step children`,
  burst: `${sizes.burst} three-step executions triggered at once`,
  wait: `One step that waits ${WAITS.wait.ms / 1000} s`,
  longWait: `One step that waits ${WAITS.longWait.ms / 1000} s`,
  waits: `${WAITS.waits.steps} steps that each wait ${WAITS.waits.ms / 1000} s`,
  waitBurst: `${WAITS.waitBurst.burst} executions at once of one step that waits ${WAITS.waitBurst.ms / 1000} s`,
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
  warm(): void;
  mark(run: string, label: string, call: 0 | 1, warmMs?: number): Promise<void>;
};

// Reports that a step function has started, by calling the `Bench` object
// that is the clock of the run.
// 1. A call on a new stub takes longer than the next call on it, so the first
//    call only warms the stub. The step function times it with its own clock,
//    which is current after an awaited call.
// 2. The second call leaves when the first has returned, and the third when
//    the second has returned. The time between their arrivals is one round
//    trip to the clock.
// The driver takes the warm-up and half a round trip off the second call's
// arrival to find when the step function started, and adds half a round trip
// to the third call's arrival to find when it carried on.
export async function mark(env: { BENCH: Namespace<Clock> }, run: Run, label: string) {
  if (run.quiet) return;
  const stub = env.BENCH.get(env.BENCH.idFromName(run.bench));
  const before = Date.now();
  await stub.warm();
  await stub.mark(run.run, label, 0, Date.now() - before);
  await stub.mark(run.run, label, 1);
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const range = (n: number) => Array.from({ length: n }, (_, i) => i);
