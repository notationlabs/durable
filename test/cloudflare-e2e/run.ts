import { createHttpSdkFactory } from "../../packages/durable/dist/client.js";
import type { Router } from "./workflows";

// Usage: E2E_URL=http://localhost:8799 [E2E_SECRET=...] tsx run.ts [scenario ...]
// With no scenario names, runs the quick set. `E2E_OBJECT` picks the Durable
// Object name, so runs against a deployed Worker can start from a clean object.
const base = process.env.E2E_URL ?? "http://localhost:8799";
const object = process.env.E2E_OBJECT ?? `run-${Date.now()}`;
const headers: Record<string, string> = {};
if (process.env.E2E_SECRET) headers["x-e2e-secret"] = process.env.E2E_SECRET;

const sdk = createHttpSdkFactory<Router>()({
  url: `${base}/durable`,
  fetchOptions: { headers },
});

// The SDK appends route names to `url`, so the object name travels as a header
// would not reach the routing Worker; it goes in the query string instead.
const originalFetch = globalThis.fetch;
globalThis.fetch = ((input: string | URL | Request, init?: RequestInit) => {
  if (typeof input === "string" && input.startsWith(base)) {
    const url = new URL(input);
    url.searchParams.set("object", object);
    input = url.toString();
  }
  return originalFetch(input, init);
}) as typeof fetch;

async function debug(path: string, body?: unknown): Promise<any> {
  const url = new URL(base + path);
  url.searchParams.set("object", object);
  const res = await originalFetch(url, {
    method: body === undefined ? "GET" : "POST",
    headers: { ...headers, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { status: res.status, text: text.slice(0, 200) };
  }
}

const sql = (query: string, ...bindings: unknown[]) => debug("/debug/sql", { query, bindings });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function check(condition: unknown, message: string) {
  if (!condition) throw new Error(message);
}

async function waitDone(executionId: string, timeout = 60_000) {
  const started = Date.now();
  const result = await sdk.wait(executionId, { timeout });
  return { result, waitedMs: Date.now() - started };
}

const scenarios: Record<string, () => Promise<unknown>> = {
  async steps() {
    const tag = `steps-${Date.now()}`;
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "steps", params: { tag, delayMs: 1000 } });
    const { result, waitedMs } = await waitDone(ack.executionId);
    check(result?.status === "completed", `status ${result?.status}: ${JSON.stringify(result)}`);
    const value = (result as any).result;
    const keys = await sql(
      "SELECT step_key, step_attempt, step_done FROM step_responses WHERE execution_id = ? ORDER BY rowid",
      ack.executionId
    );
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
    check(counts.every((c: any) => c.count === 1), `a step ran twice: ${JSON.stringify(counts)}`);
    check(value.a.n === 1 && value.b.n === 1 && value.c.n === 1, "keyless steps share a counter");
    const delayMs = value.c.at - value.b.at;
    check(delayMs >= 1000, `delay too short: ${delayMs}`);
    return { totalMs: Date.now() - t0, firstStepAfterTriggerMs: value.a.at - t0, waitedMs, delayToNextStepMs: delayMs, keys, counts };
  },

  async throws() {
    const ack = await sdk.trigger({ workflowId: "throws" });
    const { result } = await waitDone(ack.executionId);
    check(result?.status === "failed", `status ${result?.status}`);
    const error = (result as any).error as Error & { code?: string };
    check(error.message === "deliberate failure", `message ${error.message}`);
    let thrown: unknown;
    try {
      await sdk.waitForResult({ executionId: ack.executionId });
    } catch (err) {
      thrown = err;
    }
    return { name: error.name, code: error.code, waitForResultThrew: String(thrown) };
  },

  async children() {
    const ack = await sdk.trigger({ workflowId: "parent" });
    const { result, waitedMs } = await waitDone(ack.executionId);
    check(result?.status === "completed", `status ${result?.status}: ${JSON.stringify(result)}`);
    check(JSON.stringify((result as any).result) === "[2,4,6]", `result ${JSON.stringify(result)}`);
    const page = await sdk.list({ workflowId: "child" } as never);
    return { result: (result as any).result, waitedMs, childCount: page.executions.length };
  },

  async concurrency() {
    await debug("/debug/reset-live");
    const n = 25;
    const t0 = Date.now();
    const acks = await Promise.all(
      Array.from({ length: n }, () => sdk.trigger({ workflowId: "slow", params: { ms: 1000 } }))
    );
    const results = await Promise.all(acks.map((a) => waitDone(a.executionId, 120_000)));
    const statuses = results.map((r) => r.result?.status);
    check(statuses.every((s) => s === "completed"), `statuses ${statuses}`);
    const state = await debug("/debug/state");
    const finishes = results.map((r) => (r.result as any).result as number).sort();
    check(state.live.maxActive <= 10, `maxActive ${state.live.maxActive} > concurrency`);
    return {
      n,
      totalMs: Date.now() - t0,
      maxActive: state.live.maxActive,
      spreadMs: finishes[finishes.length - 1] - finishes[0],
      alarmAfter: state.alarm,
    };
  },

  async cancellation() {
    const tag = `cancel-${Date.now()}`;
    const ack = await sdk.trigger({ workflowId: "cancellable", params: { tag } });
    for (let i = 0; i < 50; i++) {
      const view = await sdk.poll(ack.executionId);
      if (view?.status === "suspended") break;
      await sleep(100);
    }
    const alarmBefore = (await debug("/debug/state")).alarm;
    const cancel = await sdk.cancel(ack.executionId, { reason: "e2e" });
    const { result, waitedMs } = await waitDone(ack.executionId);
    check(result?.status === "cancelled", `status ${result?.status}`);
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
    check(counts.find((c: any) => c.name.endsWith(":cleanup"))?.count === 1, "cleanup did not run once");
    const forced = await sdk.trigger({ workflowId: "cancellable", params: { tag: `${tag}-f` } });
    const forcedResult = await sdk.cancel(forced.executionId, { force: true });
    return {
      cancelStatus: cancel?.status,
      waitedMs,
      reason: (result as any).reason,
      counts,
      alarmBefore,
      alarmAfter: (await debug("/debug/state")).alarm,
      forced: forcedResult?.status,
    };
  },

  async store() {
    const gate = `gate-${Date.now()}`;
    const w = await sdk.trigger({ workflowId: "waiter", params: { gate } });
    await sleep(500);
    const before = await sdk.poll(w.executionId);
    const t0 = Date.now();
    const o = await sdk.trigger({ workflowId: "opener", params: { gate } });
    const { result } = await waitDone(w.executionId);
    check(result?.status === "completed", `status ${result?.status}`);
    const value = (result as any).result;
    check(value.openedBy === o.executionId, `openedBy ${value.openedBy}`);
    return { statusBefore: before?.status, wakeMs: value.at - t0 };
  },

  async fetch() {
    const ack = await sdk.trigger({
      workflowId: "fetcher",
      params: { url: process.env.E2E_FETCH_URL ?? "https://example.com/" },
    });
    const { result } = await waitDone(ack.executionId);
    check(result?.status === "completed", `status ${result?.status}: ${JSON.stringify(result)}`);
    return (result as any).result;
  },

  async idempotent() {
    const executionId = `idem-${Date.now()}`;
    const first = await sdk.trigger({
      workflowId: "steps",
      executionId,
      idempotent: true,
      params: { tag: executionId, delayMs: 10 },
    });
    const second = await sdk.trigger({
      workflowId: "steps",
      executionId,
      idempotent: true,
      params: { tag: executionId, delayMs: 10 },
    });
    let conflict: unknown;
    try {
      await sdk.trigger({
        workflowId: "steps",
        executionId,
        idempotent: true,
        params: { tag: "different", delayMs: 10 },
      });
    } catch (err) {
      conflict = err;
    }
    let duplicate: unknown;
    try {
      await sdk.trigger({ workflowId: "steps", executionId, params: { tag: executionId, delayMs: 10 } });
    } catch (err) {
      duplicate = err;
    }
    await waitDone(executionId);
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${executionId}:%`);
    check(first.status === "accepted" && second.status === "existing", `${first.status}/${second.status}`);
    check(counts.every((c: any) => c.count === 1), JSON.stringify(counts));
    return {
      first: first.status,
      second: second.status,
      changedParams: String(conflict),
      notIdempotent: String(duplicate),
    };
  },

  // A delivery that outlives the request that triggered it.
  async inflight() {
    const tag = `inflight-${Date.now()}`;
    const ms = Number(process.env.E2E_HOLD_MS ?? 25_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "inflight", params: { tag, ms } });
    const triggerMs = Date.now() - t0;
    const { result } = await waitDone(ack.executionId, 180_000);
    check(result?.status === "completed", `status ${result?.status}`);
    const value = (result as any).result;
    const log = await debug("/debug/log");
    return {
      triggerMs,
      attempt: value.attempt,
      finishedAfterMs: value.at - t0,
      instances: [...new Set(log.map((l: any) => `${l.isolate}/${l.instance}`))],
      alarmEvents: log.filter((l: any) => l.event !== "constructed").length,
    };
  },

  // Like `inflight`, but sends no request while the delivery runs, so only the
  // pending timer can keep the object in memory.
  async inflightQuiet() {
    const tag = `quiet-${Date.now()}`;
    const ms = Number(process.env.E2E_HOLD_MS ?? 120_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "inflight", params: { tag, ms } });
    await sleep(ms + 45_000);
    const view = await sdk.poll(ack.executionId);
    const log = await debug("/debug/log");
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
    check(view?.status === "completed", `status ${view?.status}`);
    return {
      attempt: (view as any).result.attempt,
      finishedAfterMs: (view as any).result.at - t0,
      counts,
      log: log.map((l: any) => ({ t: l.at - t0, i: `${l.isolate}/${l.instance}`, e: l.event })),
    };
  },

  // Resets the object mid-delivery and expects the alarm to redeliver after the lease.
  async abort() {
    const tag = `abort-${Date.now()}`;
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "inflight", params: { tag, ms: 5_000 } });
    await sleep(1000);
    const before = await debug("/debug/state");
    const aborted = await debug("/debug/abort");
    await sleep(500);
    const after = await debug("/debug/state");
    const { result } = await waitDone(ack.executionId, 180_000);
    check(result?.status === "completed", `status ${result?.status}`);
    const value = (result as any).result;
    const log = await debug("/debug/log");
    return {
      aborted,
      instanceBefore: before.instance,
      instanceAfter: after.instance,
      alarmAfterAbort: after.alarm && after.alarm - t0,
      attempt: value.attempt,
      completedAfterMs: value.at - t0,
      log: log.map((l: any) => ({ t: l.at - t0, i: l.instance, e: l.event, d: l.detail })),
    };
  },

  // A long delay that should resume from the alarm alone.
  async longDelay() {
    const tag = `long-${Date.now()}`;
    const delayMs = Number(process.env.E2E_DELAY_MS ?? 150_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "steps", params: { tag, delayMs } });
    await sleep(2000);
    const suspended = await debug("/debug/state");
    // Stay silent so the object can go idle; then poll once.
    await sleep(delayMs + 20_000);
    const view = await sdk.poll(ack.executionId);
    const log = await debug("/debug/log");
    check(view?.status === "completed", `status ${view?.status}`);
    const value = (view as any).result;
    return {
      alarmSetFor: suspended.alarm - t0,
      resumeLagMs: value.c.at - value.b.at - delayMs,
      instanceAtTrigger: suspended.instance,
      log: log.map((l: any) => ({ t: l.at - t0, i: `${l.isolate}/${l.instance}`, e: l.event })),
    };
  },

  // Phase 1 of the redeploy check: start an execution with keyless steps that
  // suspends in a delay. Prints the execution ID for phase 2.
  async redeployStart() {
    const tag = `redeploy-${Date.now()}`;
    const delayMs = Number(process.env.E2E_DELAY_MS ?? 90_000);
    const ack = await sdk.trigger({ workflowId: "steps", params: { tag, delayMs } });
    await sleep(2000);
    const keys = await sql(
      "SELECT step_key, step_attempt, step_done FROM step_responses WHERE execution_id = ? ORDER BY rowid",
      ack.executionId
    );
    return { executionId: ack.executionId, tag, keys, view: (await sdk.poll(ack.executionId))?.status };
  },

  async redeployCheck() {
    const executionId = process.env.E2E_EXECUTION!;
    const tag = process.env.E2E_TAG!;
    const { result } = await waitDone(executionId, 240_000);
    const keys = await sql(
      "SELECT step_key, step_attempt, step_done FROM step_responses WHERE execution_id = ? ORDER BY rowid",
      executionId
    );
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
    return { status: result?.status, result: (result as any)?.result, error: String((result as any)?.error ?? ""), keys, counts };
  },
};

const quick = ["steps", "throws", "children", "concurrency", "cancellation", "store", "fetch", "idempotent"];
const names = process.argv.slice(2).length ? process.argv.slice(2) : quick;
let failed = 0;
console.log(JSON.stringify({ base, object }));
for (const name of names) {
  const started = Date.now();
  try {
    const detail = await scenarios[name]();
    console.log(JSON.stringify({ scenario: name, pass: true, ms: Date.now() - started, detail }));
  } catch (err) {
    failed++;
    console.log(
      JSON.stringify({ scenario: name, pass: false, ms: Date.now() - started, error: String(err), stack: (err as Error).stack })
    );
  }
}
const final = await debug("/debug/state");
console.log(JSON.stringify({ final }));
process.exitCode = failed ? 1 : 0;
