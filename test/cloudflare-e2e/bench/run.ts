import { SCENARIOS, SYSTEMS, sizes, type Scenario, type SystemName } from "./shared";

// Usage: BENCH_URL=https://durable-bench.<subdomain>.workers.dev BENCH_SECRET=... tsx run.ts [scenario ...]
// Prints the results as Markdown on stdout and progress on stderr.
// `BENCH_REPEAT` multiplies the number of measurements of every scenario.
const base = process.env.BENCH_URL ?? "http://localhost:8798";
const secret = process.env.BENCH_SECRET ?? "local";
const repeat = Number(process.env.BENCH_REPEAT ?? 1);
// A `Bench` object drives the measurements, and each Durable class holds the
// executions in one object of the same name. Cloudflare decides where each
// object runs, and the time of a call between two objects depends on it, so
// the measurements are spread over `BENCH_OBJECTS` sets of objects.
const id = `bench-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
const benches = Array.from({ length: Number(process.env.BENCH_OBJECTS ?? 3) }, (_, i) => `${id}-${i}`);

const iterations: Record<Scenario, number> = {
  start: 21,
  chain: 12,
  payload: 12,
  sleep: 6,
  fanout: 15,
  signal: 15,
  children: 15,
  burst: 9,
};

const columns: Record<SystemName, string> = {
  native: "Cloudflare Workflows",
  durable: "Durable",
  inline: "Durable, deliveries in the object",
};

const rows: { scenario: Scenario; metric: string; workflow: string; measured: string }[] = [
  { scenario: "start", metric: "accepted", workflow: "One step", measured: "Trigger call returns" },
  { scenario: "start", metric: "firstStep", workflow: "", measured: "Trigger to the step function starting" },
  { scenario: "start", metric: "result", workflow: "", measured: "Trigger to the caller holding the result" },
  {
    scenario: "chain",
    metric: "perStep",
    workflow: `${sizes.chainSteps} steps in sequence`,
    measured: "Time per step",
  },
  {
    scenario: "payload",
    metric: "perStep",
    workflow: `${sizes.payloadSteps} steps that each return ${sizes.payloadBytes / 1000} KB`,
    measured: "Time per step",
  },
  {
    scenario: "sleep",
    metric: "late",
    workflow: `${sizes.sleeps} sleeps of ${sizes.sleepMs / 1000} s`,
    measured: `Time beyond ${sizes.sleepMs / 1000} s until the next step starts`,
  },
  {
    scenario: "fanout",
    metric: "over",
    workflow: `${sizes.fanOut} concurrent steps that each wait ${sizes.fanOutMs} ms`,
    measured: `Time beyond ${sizes.fanOutMs} ms until the next step starts`,
  },
  {
    scenario: "signal",
    metric: "wake",
    workflow: "A wait for a signal from outside",
    measured: "Signal sent to the next step starting",
  },
  {
    scenario: "children",
    metric: "total",
    workflow: `A parent that runs ${sizes.children} one-step children`,
    measured: "Children started to all results joined",
  },
  {
    scenario: "burst",
    metric: "accepted",
    workflow: `${sizes.burst} three-step executions triggered at once`,
    measured: "Trigger calls return",
  },
  { scenario: "burst", metric: "median", workflow: "", measured: "Trigger to the median execution's last step" },
  { scenario: "burst", metric: "last", workflow: "", measured: "Trigger to the last execution's last step" },
];

type Samples = Record<string, number[]>;
const samples = new Map<string, Samples>();
// The same measurements for each set of objects, to show how far the sets differ.
const samplesByBench = new Map<string, Samples>();
// The round trips of the calls from step functions to the clock, by system.
const roundTrips: Record<string, number[]> = {};
const failures: string[] = [];
const cell = (scenario: Scenario, system: SystemName) => `${scenario}/${system}`;

async function measure(scenario: Scenario, system: SystemName, bench: string, keep: boolean) {
  const url = new URL(`${base}/run`);
  url.searchParams.set("bench", bench);
  url.searchParams.set("scenario", scenario);
  url.searchParams.set("system", system);
  let body: { metrics?: Record<string, number | number[]>; roundTrips?: number[]; error?: string };
  try {
    const res = await fetch(url, { method: "POST", headers: { "x-bench-secret": secret } });
    const text = await res.text();
    try {
      body = JSON.parse(text);
    } catch {
      body = { error: `${res.status}: ${text.slice(0, 200)}` };
    }
  } catch (err) {
    body = { error: String(err) };
  }
  if (!body.metrics) {
    failures.push(`${scenario} on ${system}: ${body.error}`);
    console.error(`  failed: ${scenario} on ${system}: ${body.error}`);
    return;
  }
  if (!keep) return;
  (roundTrips[system] ??= []).push(...(body.roundTrips ?? []));
  let into = samples.get(cell(scenario, system));
  if (!into) samples.set(cell(scenario, system), (into = {}));
  let intoBench = samplesByBench.get(`${cell(scenario, system)}/${bench}`);
  if (!intoBench) samplesByBench.set(`${cell(scenario, system)}/${bench}`, (intoBench = {}));
  for (const [metric, value] of Object.entries(body.metrics)) {
    const values = Array.isArray(value) ? value : [value];
    (into[metric] ??= []).push(...values);
    (intoBench[metric] ??= []).push(...values);
  }
}

function percentile(values: number[], p: number) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

function format(ms: number | undefined) {
  if (ms === undefined) return "–";
  if (Math.abs(ms) >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (Math.abs(ms) >= 10) return `${Math.round(ms)} ms`;
  if (Math.abs(ms) >= 1) return `${ms.toFixed(1)} ms`;
  return `${ms.toFixed(2)} ms`;
}

function table(p: number, shown: typeof rows) {
  const lines = [
    `| Workflow | Measured | ${SYSTEMS.map((s) => columns[s]).join(" | ")} |`,
    `| :--- | :--- | ${SYSTEMS.map(() => "---:").join(" | ")} |`,
  ];
  for (const row of shown) {
    const cells = SYSTEMS.map((system) => {
      const values = samples.get(cell(row.scenario, system))?.[row.metric];
      return format(values?.length ? percentile(values, p) : undefined);
    });
    lines.push(`| ${row.workflow} | ${row.measured} | ${cells.join(" | ")} |`);
  }
  return lines.join("\n");
}

const requested = process.argv.slice(2) as Scenario[];
for (const name of requested) {
  if (!SCENARIOS.includes(name)) throw new Error(`unknown scenario ${name}`);
}
const chosen = requested.length ? requested : [...SCENARIOS];

for (const scenario of chosen) {
  const n = Math.ceil(iterations[scenario] * repeat);
  console.error(`${scenario}: ${n} measured on each system`);
  // The first execution of a workflow in a new isolate also loads its code.
  for (const bench of benches) {
    for (const system of SYSTEMS) await measure(scenario, system, bench, false);
  }
  for (let i = 0; i < n; i++) {
    const bench = benches[i % benches.length];
    // The systems take turns, and the order rotates, so that a slow minute
    // on Cloudflare's side does not fall on one system.
    const first = Math.floor(i / benches.length);
    for (let j = 0; j < SYSTEMS.length; j++) {
      await measure(scenario, SYSTEMS[(first + j) % SYSTEMS.length], bench, true);
    }
  }
}

const shown = rows.filter((row) => chosen.includes(row.scenario));
console.error("Medians in each set of objects:");
for (const row of shown) {
  const sets = SYSTEMS.map((system) => {
    const medians = benches.map((bench) => {
      const values = samplesByBench.get(`${cell(row.scenario, system)}/${bench}`)?.[row.metric];
      return format(values?.length ? percentile(values, 50) : undefined);
    });
    return `${system} ${medians.join(", ")}`;
  });
  console.error(`  ${row.scenario} ${row.metric}: ${sets.join("; ")}`);
}
const counts = shown
  .filter((row) => row.workflow)
  .map((row) => {
    const n = SYSTEMS.map((s) => samples.get(cell(row.scenario, s))?.[row.metric]?.length ?? 0);
    return `${row.workflow}: ${Math.min(...n)}`;
  });
const roundTripMedians = SYSTEMS.map((system) => {
  const values = roundTrips[system];
  return `${columns[system]} ${format(values?.length ? percentile(values, 50) : undefined)}`;
});

console.log(`# Cloudflare Workflows and Notation Durable on a deployed Worker

Measured on ${new Date().toISOString().slice(0, 10)}. Every figure is a time, and a lower one is better.

## Median

${table(50, shown)}

## 90th percentile

${table(90, shown)}

## How to read it

- **The systems** – All three run in one Worker. "Durable" is \`WorkflowObject\` with its defaults, which runs each delivery in another instance of the class. "Durable, deliveries in the object" sets \`isolateDeliveries: false\`.
- **The objects** – One Durable object holds every execution it is given. The measurements are spread evenly over ${benches.length} such objects for each Durable column, because the time of a call between two objects depends on where Cloudflare runs them.
- **The workflows** – Each workflow is written once for Cloudflare Workflows (\`native.ts\`) and once for Durable (\`durable.ts\`), with the same steps. Cloudflare Workflows has no step that waits for another instance, so its parent waits for an event that each child sends from a second step.
- **The clock** – Every time is read in one Durable Object. The driver runs in it, and a step function reports that it started by calling it twice in a row. The time between the two arrivals is one round trip of that call, and half of it is taken off each figure that starts or ends at a step. The median round trip was: ${roundTripMedians.join(", ")}.
- **The caller holding the result** – The Durable caller uses \`sdk.wait\`. The Cloudflare Workflows caller reads \`instance.status()\` every 10 ms.
- **The burst** – Cloudflare Workflows runs each instance on its own. One Durable object runs 10 deliveries at once by default (\`concurrency\`).
- **Measurements for each figure** – ${counts.join("; ")}. One execution of each workflow ran unmeasured first in each object.${
  failures.length ? `\n- **Failed measurements** – ${failures.length}: ${[...new Set(failures)].slice(0, 5).join("; ")}` : ""
}`);
process.exitCode = failures.length ? 1 : 0;
