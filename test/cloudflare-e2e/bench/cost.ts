import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import {
  COST_SCENARIOS,
  SYSTEMS,
  WAITS,
  systemTitles,
  workflowTitles,
  type CostScenario as Scenario,
  type SystemName,
  type Usage,
} from "./shared";

// Usage: BENCH_URL=https://durable-bench.<subdomain>.workers.dev BENCH_SECRET=... tsx cost.ts [scenario ...]
// Runs each workflow on each system, and prints what Cloudflare bills for one
// execution as Markdown on stdout, with progress on stderr.
//
// A Durable object counts its own usage. The usage of Cloudflare Workflows is
// read from Cloudflare's API and analytics with the `cf` CLI, which must be
// signed in to the account of the Worker (`cf auth login`).
// `CLOUDFLARE_ACCOUNT_ID` picks the account when the login has several.
const base = process.env.BENCH_URL ?? "http://localhost:8798";
const secret = process.env.BENCH_SECRET ?? "local";
const bench = `cost-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
const WORKFLOW_NAME = "durable-bench-native";

// US dollars for one unit beyond the monthly allowances of the Workers Paid
// plan, read on 2026-11-25 from developers.cloudflare.com/workflows/reference/pricing
// and developers.cloudflare.com/durable-objects/platform/pricing.
const price = {
  workflowRequest: 0.3 / 1e6,
  workflowStep: 0.8 / 1e5,
  workflowCpuMs: 0.02 / 1e6,
  objectRequest: 0.15 / 1e6,
  objectGbSecond: 12.5 / 1e6,
  rowWritten: 1 / 1e6,
  rowRead: 0.001 / 1e6,
  storedGbMonth: 0.2,
};
// Cloudflare bills an object's duration for 128 MB, whatever it uses.
const OBJECT_GB = 0.128;

const repeats: Record<Scenario, number> = {
  start: 5,
  chain: 5,
  payload: 5,
  sleep: 5,
  fanout: 5,
  signal: 5,
  children: 5,
  burst: 2,
  wait: 5,
  longWait: 3,
  waits: 3,
  waitBurst: 2,
};

type NativeUsage = { instances: number; steps: number; cpuMs: number };
type ObjectUsage = {
  requests: number;
  rowsWritten: number;
  rowsRead: number;
  seconds: number;
  storedBytes: number;
};

const failures: string[] = [];

// A failed run is run again from the start, with new executions, so nothing
// of the failed one is in the counts.
async function run(scenario: Scenario, system: SystemName, n: number) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await runOnce(scenario, system, n);
    } catch (err) {
      failures.push(String(err));
      console.error(`  failed: ${err}`);
      if (attempt === 3) throw err;
    }
  }
}

async function runOnce(scenario: Scenario, system: SystemName, n: number) {
  const url = new URL(`${base}/cost`);
  url.searchParams.set("bench", bench);
  url.searchParams.set("scenario", scenario);
  url.searchParams.set("system", system);
  url.searchParams.set("repeats", String(n));
  const res = await fetch(url, { method: "POST", headers: { "x-bench-secret": secret } });
  const text = await res.text();
  let body: { executions?: number; usage?: Usage; instances?: string[]; error?: string };
  try {
    body = JSON.parse(text);
  } catch {
    body = { error: `${res.status}: ${text.slice(0, 200)}` };
  }
  if (!body.executions) throw new Error(`${scenario} on ${system}: ${body.error}`);
  return body as { executions: number; usage?: Usage; instances?: string[] };
}

// What Cloudflare bills a Durable object for, for one execution. With
// isolated deliveries, each delivery is also a request to the instance that
// runs it, and that instance is billed for the delivery's wall time.
function objectUsage(usage: Usage, executions: number, isolated: boolean): ObjectUsage {
  const per = (value: number) => value / executions;
  return {
    requests: per(usage.trigger + usage.alarm + usage.rpc + (isolated ? usage.deliveries : 0)),
    rowsWritten: per(usage.rowsWritten + usage.alarmWrites),
    rowsRead: per(usage.rowsRead),
    seconds: per(usage.activeMs + (isolated ? usage.deliveryMs : 0)) / 1000,
    storedBytes: per(usage.storedBytes),
  };
}

const objectCost = (usage: ObjectUsage) =>
  usage.requests * price.objectRequest +
  usage.seconds * OBJECT_GB * price.objectGbSecond +
  usage.rowsWritten * price.rowWritten +
  usage.rowsRead * price.rowRead;

const nativeCost = (usage: NativeUsage) =>
  usage.instances * price.workflowRequest +
  usage.steps * price.workflowStep +
  usage.cpuMs * price.workflowCpuMs;

let account: string | undefined;
function analytics(query: string): Record<string, any>[] {
  account ??=
    process.env.CLOUDFLARE_ACCOUNT_ID ??
    JSON.parse(execFileSync("cf", ["auth", "whoami"], { encoding: "utf8" })).accounts[0].id;
  const out = execFileSync(
    "cf",
    ["analytics", "sql", "post", "--query", query, "--scope-account-tag", account!],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }
  );
  return JSON.parse(out).data;
}

// The CPU time that Cloudflare recorded for the instances, in milliseconds.
// The dataset is sampled when an account sends many events, and each row
// counts for `sampleInterval` events, so the sum is an estimate.
function cpuTime(ids: string[]) {
  let total = 0;
  for (let i = 0; i < ids.length; i += 40) {
    const list = ids
      .slice(i, i + 40)
      .map((id) => `'${id}'`)
      .join(", ");
    const [row] = analytics(
      `SELECT sum(cpuTimeDiffMs * sampleInterval) AS cpuMs FROM events.workflows WHERE workflowName = '${WORKFLOW_NAME}' AND timestamp > now() - INTERVAL '6' HOUR AND instanceId IN (${list}) LIMIT 1`
    );
    total += Number(row?.cpuMs ?? 0);
  }
  return total;
}

const exec = promisify(execFile);

// The number of steps that Cloudflare counted for an instance. The API gives
// the exact count, and the analytics do not, because they are sampled.
async function stepCount(id: string) {
  for (let attempt = 1; ; attempt++) {
    try {
      const { stdout } = await exec("cf", [
        "workflows", "instances", "get", id, "--workflow-name", WORKFLOW_NAME, "--simple", "true",
      ]);
      const instance = JSON.parse(stdout);
      if (instance.status !== "complete") throw new Error(`instance ${id} is ${instance.status}`);
      return instance.step_count as number;
    } catch (err) {
      if (attempt === 4) throw err;
      await sleep(2_000 * attempt);
    }
  }
}

async function pooled<T, R>(items: T[], size: number, fn: (item: T) => Promise<R>) {
  const results: R[] = new Array(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i]);
    }
  };
  await Promise.all(Array.from({ length: size }, worker));
  return results;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const requested = process.argv.slice(2) as Scenario[];
for (const name of requested) {
  if (!COST_SCENARIOS.includes(name)) throw new Error(`unknown scenario ${name}`);
}
const chosen = requested.length ? requested : [...COST_SCENARIOS];

// The first request to a Durable object creates the runtime's tables.
for (const system of SYSTEMS) await run("start", system, 1);

const objects: Record<string, ObjectUsage> = {};
const natives: Record<string, { executions: number; instances: string[] }> = {};
for (const scenario of chosen) {
  console.error(`${scenario}: ${repeats[scenario]} on each system`);
  for (const system of SYSTEMS) {
    const { executions, usage, instances } = await run(scenario, system, repeats[scenario]);
    if (usage) objects[`${scenario}/${system}`] = objectUsage(usage, executions, system === "durable");
    else natives[scenario] = { executions, instances: instances! };
  }
}

const allInstances = Object.values(natives).flatMap((n) => n.instances);
console.error(`Reading ${allInstances.length} Cloudflare Workflows instances from Cloudflare`);
const steps = new Map<string, number>();
const counted = await pooled(allInstances, 8, stepCount);
allInstances.forEach((id, i) => steps.set(id, counted[i]));

// An instance's events reach the analytics over a few minutes, so the CPU
// time is read until two reads in a row agree.
const cpuMs: Record<string, number> = {};
let previous = "";
for (let attempt = 0; ; attempt++) {
  await sleep(attempt ? 30_000 : 60_000);
  for (const [scenario, { instances }] of Object.entries(natives)) cpuMs[scenario] = cpuTime(instances);
  const read = JSON.stringify(cpuMs);
  if (read === previous || attempt === 20) break;
  previous = read;
}
const native: Record<string, NativeUsage> = {};
for (const [scenario, { executions, instances }] of Object.entries(natives)) {
  native[scenario] = {
    instances: instances.length / executions,
    steps: instances.reduce((total, id) => total + steps.get(id)!, 0) / executions,
    cpuMs: cpuMs[scenario] / executions,
  };
}

const dollars = (value: number) =>
  value < 0.01
    ? `$${Number(value.toPrecision(2))}`
    : `$${value >= 100 ? Math.round(value).toLocaleString("en-US") : value >= 10 ? value.toFixed(1) : value.toFixed(2)}`;
const number = (value: number) => String(Number(value.toFixed(value >= 10 ? 0 : value >= 1 ? 1 : 2)));
const size = (bytes: number) => (bytes >= 1e6 ? `${(bytes / 1e6).toFixed(1)} MB` : `${(bytes / 1e3).toFixed(1)} KB`);
const duration = (seconds: number) => (seconds >= 1 ? `${seconds.toFixed(2)} s` : `${Math.round(seconds * 1000)} ms`);

function grid(header: string[], rows: string[][]) {
  return [
    `| ${header.join(" | ")} |`,
    `| :--- | ${header.slice(1).map(() => "---:").join(" | ")} |`,
    ...rows.map((row) => `| ${row.join(" | ")} |`),
  ].join("\n");
}

const million = 1e6;
const costs = grid(
  ["Workflow", ...SYSTEMS.map((system) => systemTitles[system])],
  chosen.map((scenario) => [
    workflowTitles[scenario],
    dollars(nativeCost(native[scenario]) * million),
    dollars(objectCost(objects[`${scenario}/durable`]) * million),
    dollars(objectCost(objects[`${scenario}/inline`]) * million),
  ])
);
const nativeUnits = grid(
  ["Workflow", "Instances", "Steps", "CPU time"],
  chosen.map((scenario) => {
    const usage = native[scenario];
    return [
      workflowTitles[scenario],
      number(usage.instances),
      number(usage.steps),
      `${number(usage.cpuMs)} ms`,
    ];
  })
);
const objectUnits = (system: SystemName) =>
  grid(
    ["Workflow", "Requests", "Rows written", "Rows read", "Duration"],
    chosen.map((scenario) => {
      const usage = objects[`${scenario}/${system}`];
      return [
        workflowTitles[scenario],
        number(usage.requests),
        number(usage.rowsWritten),
        number(usage.rowsRead),
        duration(usage.seconds),
      ];
    })
  );
const secondCost = dollars(OBJECT_GB * price.objectGbSecond * million);

// The range, over the workflows whose steps wait or over the others, of the
// share that one unit has in a Durable figure.
const share = (waiting: boolean, part: (usage: ObjectUsage) => number) => {
  const shares = chosen
    .filter((scenario) => scenario in WAITS === waiting)
    .flatMap((s) => [objects[`${s}/durable`], objects[`${s}/inline`]])
    .map((usage) => (100 * part(usage)) / objectCost(usage));
  if (!shares.length) return "–";
  return `${Math.round(Math.min(...shares))}% to ${Math.round(Math.max(...shares))}%`;
};
const rowsShare = (usage: ObjectUsage) => usage.rowsWritten * price.rowWritten;
const durationShare = (usage: ObjectUsage) => usage.seconds * OBJECT_GB * price.objectGbSecond;
// The size of an object's database grows a page at a time, so only the
// workflows that store many pages over the pass give a figure.
const stored = chosen
  .filter((scenario) => ["chain", "payload", "burst"].includes(scenario))
  .map((scenario) => {
    const bytes = objects[`${scenario}/durable`].storedBytes;
    const monthly = dollars((bytes / 1e9) * price.storedGbMonth * million);
    return `${size(bytes)} for "${workflowTitles[scenario]}" (${monthly} a month for a million of them)`;
  });
const failureNote = failures.length
  ? ` ${failures.length} run${failures.length > 1 ? "s" : ""} failed and ran again: ${[...new Set(failures)].join("; ")}.`
  : "";
const storedNote = stored.length ? `, and one execution added ${stored.join(", ")}` : "";

const notes = [
  `**The executions** – Each figure is the mean of 2 to ${repeats.start} runs of the workflow. A step function does nothing unless its row says that it waits, and a step that waits does so on a timer. A burst's figures are for one of its executions, and a parent's figures include its children.${failureNote}`,
  `**Cloudflare Workflows** – An instance costs ${dollars(price.workflowRequest * million)} per million, a step ${dollars(price.workflowStep * million)} per million, and a millisecond of CPU time ${dollars(price.workflowCpuMs * million)} per million. A sleep and a wait for an event each count as a step. The step count of each instance comes from Cloudflare's API. The CPU time comes from Cloudflare's analytics, which sample their events, so it is an estimate. It is under 1% of every figure.`,
  `**Durable** – A request to an object costs ${dollars(price.objectRequest * million)} per million, a row written ${dollars(price.rowWritten * million)} per million, a row read ${dollars(price.rowRead * million)} per million, and a second of an object's duration ${secondCost} per million. The object counts them itself: its trigger requests, alarms, RPC calls, the rows that each SQL cursor reports, and the time it has a request, an alarm or a delivery in progress. A call to \`setAlarm\` or \`deleteAlarm\` counts as a row written.`,
  `**Where Durable's cost comes from** – Where no step waits, rows written are ${share(false, rowsShare)} of a Durable figure and duration is ${share(false, durationShare)}. Where a step waits, duration is ${share(true, durationShare)}. Cloudflare counts a row for each index that a write changes, as well as the row itself. Duration is an estimate: it leaves out the time a response waits for storage to confirm a write.`,
  `**Deliveries in other instances** – By default each delivery is one more request, to the instance that runs it, and that instance is billed for the delivery's duration too. The instance sends each step result to the object over a stub it was given. These calls are not counted as requests, which is how Cloudflare's pricing page describes calls on a stub that a call returns.`,
  `**Not counted** – A request that reads an execution's status or waits for its result is one more request to the object, and the object is billed for the time a wait stays open. Reading the status of a Cloudflare Workflows instance is not billed.`,
  `**Steps that take time** – Durable Objects bill duration while a step function runs, and Cloudflare Workflows bills only CPU time. So each second that a step waits adds ${secondCost} per million executions with deliveries in the object, and ${dollars(2 * OBJECT_GB * price.objectGbSecond * million)} by default, where both the object and the instance that runs the delivery are billed. On Cloudflare Workflows it adds nothing. An object is billed once for any number of executions that wait at the same time, so the executions that wait at once share the object's second, and each instance that runs a delivery is still billed for its own.`,
  `**Storage** – Both cost ${dollars(price.storedGbMonth)} per GB-month, and it is not in the figures above. Cloudflare Workflows deletes an instance 7 days after it ends by default. Durable keeps an execution until it is deleted${storedNote}. Cloudflare's analytics do not give a usable size for a Workflows instance.`,
];

console.log(`## Cost

US dollars for one million executions, measured on ${new Date().toISOString().slice(0, 10)}. The prices are those beyond the monthly allowances of the Workers Paid plan. Storage is not in these figures.

${costs}

### What Cloudflare bills for one execution on Cloudflare Workflows

${nativeUnits}

### What Cloudflare bills for one execution on Durable

${objectUnits("durable")}

### What Cloudflare bills for one execution on Durable, with deliveries in the object

${objectUnits("inline")}

### How to read it

${notes.map((note) => `- ${note}`).join("\n")}`);
