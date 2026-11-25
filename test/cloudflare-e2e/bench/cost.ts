import { execFileSync } from "node:child_process";
import {
  SCENARIOS,
  SYSTEMS,
  systemTitles,
  workflowTitles,
  type Scenario,
  type SystemName,
  type Usage,
} from "./shared";

// Usage: BENCH_URL=https://durable-bench.<subdomain>.workers.dev BENCH_SECRET=... tsx cost.ts [scenario ...]
// Runs each workflow on each system, and prints what Cloudflare bills for one
// execution as Markdown on stdout, with progress on stderr.
//
// A Durable object counts its own usage. The usage of Cloudflare Workflows is
// read from Cloudflare's analytics with the `cf` CLI, which must be signed in
// to the account of the Worker (`cf auth login`). `CLOUDFLARE_ACCOUNT_ID`
// picks the account when the login has several.
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
};

type NativeUsage = { instances: number; steps: number; cpuMs: number };
type ObjectUsage = {
  requests: number;
  rowsWritten: number;
  rowsRead: number;
  seconds: number;
  storedBytes: number;
};

async function run(scenario: Scenario, system: SystemName, n: number) {
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

// The events that Cloudflare recorded for the instances, or undefined while
// an instance has no event for its end yet. The dataset is sampled when an
// account sends many events, so each row counts for `sampleInterval` events.
function instanceEvents(ids: string[]) {
  const rows: Record<string, any>[] = [];
  for (let i = 0; i < ids.length; i += 40) {
    const list = ids
      .slice(i, i + 40)
      .map((id) => `'${id}'`)
      .join(", ");
    rows.push(
      ...analytics(
        `SELECT instanceId, eventType, sum(allStepCountDiff * sampleInterval) AS steps, sum(cpuTimeDiffMs * sampleInterval) AS cpuMs FROM events.workflows WHERE workflowName = '${WORKFLOW_NAME}' AND timestamp > now() - INTERVAL '6' HOUR AND instanceId IN (${list}) GROUP BY instanceId, eventType LIMIT 1000`
      )
    );
  }
  const ended = new Set(rows.filter((r) => r.eventType === "WORKFLOW_SUCCESS").map((r) => r.instanceId));
  return ids.every((id) => ended.has(id)) ? rows : undefined;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const requested = process.argv.slice(2) as Scenario[];
for (const name of requested) {
  if (!SCENARIOS.includes(name)) throw new Error(`unknown scenario ${name}`);
}
const chosen = requested.length ? requested : [...SCENARIOS];

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
console.error(`Reading ${allInstances.length} Cloudflare Workflows instances from the analytics`);
let events: Record<string, any>[] | undefined;
for (let attempt = 0; !events; attempt++) {
  if (attempt === 40) throw new Error("the analytics do not have every instance after 20 minutes");
  if (attempt) await sleep(30_000);
  events = instanceEvents(allInstances);
}
const native: Record<string, NativeUsage> = {};
for (const [scenario, { executions, instances }] of Object.entries(natives)) {
  const mine = events.filter((row) => instances.includes(row.instanceId));
  const sum = (column: string) => mine.reduce((total, row) => total + Number(row[column]), 0);
  native[scenario] = {
    instances: instances.length / executions,
    steps: sum("steps") / executions,
    cpuMs: sum("cpuMs") / executions,
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

const durableUsages = chosen.flatMap((s) => [objects[`${s}/durable`], objects[`${s}/inline`]]);
const share = (part: (usage: ObjectUsage) => number) => {
  const shares = durableUsages.map((usage) => (100 * part(usage)) / objectCost(usage));
  return `${Math.round(Math.min(...shares))}% to ${Math.round(Math.max(...shares))}%`;
};
// The size of an object's database grows a page at a time, so only the
// workflows that store many pages over the pass give a figure.
const stored = chosen
  .filter((scenario) => ["chain", "payload", "burst"].includes(scenario))
  .map((scenario) => {
    const bytes = objects[`${scenario}/durable`].storedBytes;
    const monthly = dollars((bytes / 1e9) * price.storedGbMonth * million);
    return `${size(bytes)} for "${workflowTitles[scenario]}" (${monthly} a month for a million of them)`;
  });
const storedNote = stored.length ? `, and one execution added ${stored.join(", ")}` : "";

const notes = [
  `**The executions** – Each figure is the mean of ${repeats.start} executions, or of ${repeats.burst} bursts. The step functions do nothing here and make no call to the clock. A burst's figures are for one of its executions, and a parent's figures include its children.`,
  `**Cloudflare Workflows** – An instance costs ${dollars(price.workflowRequest * million)} per million, a step ${dollars(price.workflowStep * million)} per million, and a millisecond of CPU time ${dollars(price.workflowCpuMs * million)} per million. A sleep and a wait for an event each count as a step. The counts come from Cloudflare's analytics for each instance.`,
  `**Durable** – A request to an object costs ${dollars(price.objectRequest * million)} per million, a row written ${dollars(price.rowWritten * million)} per million, a row read ${dollars(price.rowRead * million)} per million, and a second of an object's duration ${secondCost} per million. The object counts them itself: its trigger requests, alarms, RPC calls, the rows that each SQL cursor reports, and the time it has a request, an alarm or a delivery in progress. A call to \`setAlarm\` or \`deleteAlarm\` counts as a row written.`,
  `**Where Durable's cost comes from** – Rows written are ${share((u) => u.rowsWritten * price.rowWritten)} of each Durable figure. Cloudflare counts a row for each index that a write changes, as well as the row itself. Duration is ${share((u) => u.seconds * OBJECT_GB * price.objectGbSecond)} of each figure, and it is an estimate: it leaves out the time a response waits for storage to confirm a write.`,
  `**Deliveries in other instances** – By default each delivery is one more request, to the instance that runs it, and that instance is billed for the delivery's duration too. The instance sends each step result to the object over a stub it was given. These calls are not counted as requests, which is how Cloudflare's pricing page describes calls on a stub that a call returns.`,
  `**Not counted** – A request that reads an execution's status or waits for its result is one more request to the object, and the object is billed for the time a wait stays open. Reading the status of a Cloudflare Workflows instance is not billed.`,
  `**Steps that take time** – Durable Objects bill duration while a step function runs, and Cloudflare Workflows bills only CPU time. A step that waits one second for another service adds ${secondCost} per million executions with deliveries in the object and ${dollars(2 * OBJECT_GB * price.objectGbSecond * million)} by default. On Cloudflare Workflows it adds nothing.`,
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
