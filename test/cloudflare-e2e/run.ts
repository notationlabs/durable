import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
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

  // A delivery started by the alarm that runs longer than the alarm handler
  // waits. With `E2E_SAMPLE_MS` it reads the alarm time at that interval;
  // without it, it sends no request until the hold should have ended.
  async alarmHold() {
    const tag = `alarmhold-${Date.now()}`;
    const ms = Number(process.env.E2E_HOLD_MS ?? 17 * 60_000);
    const sampleMs = Number(process.env.E2E_SAMPLE_MS ?? 0);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "alarmHold", params: { tag, ms } });
    const samples: { t: number; alarm: number | null; nextClaimAt: number | null }[] = [];
    const end = t0 + 3_000 + ms + 30_000;
    if (sampleMs > 0) {
      while (Date.now() < end) {
        await sleep(sampleMs);
        const state = await debug("/debug/state");
        samples.push({
          t: Date.now() - t0,
          alarm: state.alarm && state.alarm - t0,
          nextClaimAt: state.nextClaimAt && state.nextClaimAt - t0,
        });
      }
    } else {
      await sleep(end - Date.now());
    }
    const view = await sdk.poll(ack.executionId);
    const log = await debug("/debug/log");
    const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
    check(view?.status === "completed", `status ${view?.status}`);
    return {
      attempt: (view as any).result.attempt,
      finishedAfterMs: (view as any).result.at - t0,
      counts,
      alarmValues: [...new Set(samples.map((s) => s.alarm))],
      samples: samples.length,
      log: log.map((l: any) => ({ t: l.at - t0, i: `${l.isolate}/${l.instance}`, e: l.event })),
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

async function probe(name: string, path: string): Promise<any> {
  const url = new URL(base + path);
  url.searchParams.set("object", name);
  const res = await originalFetch(url, { headers });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { status: res.status, text: text.slice(0, 200) };
  }
}

// A new Worker version takes time to reach every location, and until it does
// some requests fail, so arming is retried until the object confirms it.
async function armProbe(name: string, query: string) {
  for (let attempt = 0; attempt < 10; attempt++) {
    const armed = await probe(name, `/probe/alarm-case?${query}`);
    if (typeof armed.t === "number") return;
    await sleep(1000);
  }
  throw new Error(`could not arm ${name}`);
}

// The alarm handler runs of one probe object. `ms` counts from the first run.
async function probeRuns(name: string) {
  const state = await probe(name, "/probe/state");
  const runs = (state.log ?? [])
    .filter((row: any) => row.event === "probe-run")
    .map((row: any) => JSON.parse(row.detail));
  const first = runs[0]?.now;
  return {
    object: name.slice(object.length + 1),
    armed: (state.log ?? []).some((row: any) => row.event === "probe-armed"),
    readAtMs: first === undefined ? null : state.now - first,
    alarm: state.alarm === null || first === undefined ? state.alarm : { msAfterFirstRun: state.alarm - first },
    runs: runs.map((run: any) => ({
      n: run.n,
      ms: run.now - first,
      retryCount: run.retryCount,
      isRetry: run.isRetry,
      ...(run.n === 1 && { firstRunMsAfterT: run.now - run.t }),
      ...(run.setTo !== undefined && { setToMsAfterT: run.setTo - run.t, setToMsAfterNow: run.setTo - run.now }),
      ...("before" in run && { getAlarmBefore: run.before === null ? null : run.before - run.t }),
      ...("after" in run && { getAlarmAfterMsAfterT: run.after === null ? null : run.after - run.t }),
      ...(run.end !== undefined && { handlerMs: run.end - run.now }),
    })),
  };
}

Object.assign(scenarios, {
  // What an alarm does after its handler sets it, or leaves it, on the first
  // run. Each case runs on fresh objects.
  async "alarm-set"() {
    const cases = [
      "untouched",
      "untouched&peek",
      "writes-only",
      "same",
      "same&peek",
      "same-unawaited",
      "same&hold=3000",
      "same-plus-1",
      "max-t-now",
      "now",
      "now&peek",
      "now&hold=3000",
      "now-plus-1000",
      "now-plus-1000&hold=3000",
      "past",
      "past&peek",
      "same-then-later",
    ];
    const repeats = Number(process.env.E2E_REPEATS ?? 3);
    const names: string[] = [];
    for (const c of cases) {
      for (let i = 0; i < repeats; i++) {
        const name = `${object}-${c.replace(/[&=]/g, "-")}-${i}`;
        names.push(name);
        await armProbe(name, `case=${c}`);
      }
    }
    await sleep(25_000);
    const early = await Promise.all(names.map(probeRuns));
    await sleep(Number(process.env.E2E_SETTLE_MS ?? 60_000));
    const late = await Promise.all(names.map(probeRuns));
    return { early, late };
  },

  // How Cloudflare runs an alarm handler again after it throws. `fail-0` is
  // read only at the end, in case reading an object changes what happens.
  async "alarm-fail"() {
    const waitMs = Number(process.env.E2E_FAIL_WAIT_MS ?? 10 * 60_000);
    const untouched = [`${object}-fail-0`];
    const sampled = [
      `${object}-fail-1`,
      `${object}-fail-2`,
      `${object}-fail-once-0`,
      `${object}-fail-set-0`,
    ];
    for (const name of [...untouched, ...sampled]) {
      const c = name.slice(object.length + 1).replace(/-\d$/, "");
      await armProbe(name, `case=${c}&peek`);
    }
    const samples: unknown[] = [];
    for (let i = 0; i < 3; i++) {
      await sleep(waitMs / 3);
      samples.push(await Promise.all(sampled.map(probeRuns)));
    }
    const final = await Promise.all([...untouched, ...sampled].map(probeRuns));
    // An alarm set after the retries have stopped.
    await armProbe(`${object}-fail-1`, "case=untouched&peek&delay=1000");
    await sleep(8_000);
    const afterNewAlarm = await probeRuns(`${object}-fail-1`);
    return { samples, final, afterNewAlarm };
  },
});

// What the runtime and the debug tables hold for one execution.
async function evidence(executionId: string, tag: string, t0: number) {
  const [execution] = await sql(
    "SELECT status, admitted_at, updated_at, lease_token, unacknowledged_delivery_count FROM executions WHERE execution_id = ?",
    executionId
  );
  const stepRows = await sql(
    "SELECT step_key, step_attempt, step_done FROM step_responses WHERE execution_id = ? ORDER BY rowid",
    executionId
  );
  const counts = await sql("SELECT * FROM e2e_counters WHERE name LIKE ?", `${tag}:%`);
  const log = (await debug("/debug/log"))
    .filter((l: any) => l.at >= t0 - 1000)
    .map((l: any) => ({ t: l.at - t0, i: `${l.isolate}/${l.instance}`, e: l.event, d: l.detail }));
  return { execution, stepRows, counts, log };
}

function percentile(sorted: number[], p: number) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function retry<T>(fn: () => Promise<T>, errors: string[], attempts = 4): Promise<T> {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      errors.push(String(err).slice(0, 200));
      if (i >= attempts) throw err;
      await sleep(500 * i);
    }
  }
}

Object.assign(scenarios, {
  // A long delay with no request to the object until it should have ended.
  // The run counts as a test of eviction only when `evicted` is true: the
  // step after the delay ran in a different instance from the step before it.
  async evict() {
    const tag = `evict-${Date.now()}`;
    const delayMs = Number(process.env.E2E_DELAY_MS ?? 60 * 60_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "evict", params: { tag, delayMs } });
    await sleep(delayMs + Number(process.env.E2E_SETTLE_MS ?? 90_000));
    const view = await sdk.poll(ack.executionId);
    const found = await evidence(ack.executionId, tag, t0);
    check(view?.status === "completed", `status ${view?.status}`);
    const { before, after } = (view as any).result;
    if (process.env.E2E_REQUIRE_EVICTED) {
      check(before.instance !== after.instance, `not evicted: instance ${before.instance} ran both steps`);
    }
    const constructed = found.log.filter((l: any) => l.e === "constructed");
    return {
      delayMs,
      afterStepLagMs: after.at - before.at - delayMs,
      instanceBefore: before.instance,
      instanceAfter: after.instance,
      evicted: before.instance !== after.instance,
      instancesConstructed: constructed.length,
      constructedAt: constructed.map((l: any) => ({ t: l.t, i: l.i })),
      ...found,
    };
  },

  // Runs `evict` once per delay in `E2E_EVICT_DELAYS_MS`, all at the same
  // time, each in its own process and on its own object. A run fails unless a
  // different instance ran the step after the delay.
  async "evict-many"() {
    const minutes = [5, 5, 10, 10, 15, 20, 30, 30, 45, 60];
    const delays = process.env.E2E_EVICT_DELAYS_MS
      ? process.env.E2E_EVICT_DELAYS_MS.split(",").map(Number)
      : minutes.map((m) => m * 60_000);
    const runs = await Promise.all(
      delays.map(async (delayMs, i) => {
        const child = spawn(
          process.execPath,
          [...process.execArgv, fileURLToPath(import.meta.url), "evict"],
          {
            env: {
              ...process.env,
              E2E_OBJECT: `${object}-evict-${i}`,
              E2E_DELAY_MS: String(delayMs),
              E2E_REQUIRE_EVICTED: "1",
            },
            stdio: ["ignore", "pipe", "inherit"],
          }
        );
        let out = "";
        child.stdout.on("data", (chunk) => (out += chunk));
        const code = await new Promise((resolve) => child.on("close", resolve));
        const line = out.split("\n").find((l) => l.startsWith('{"scenario":"evict"'));
        const result = line ? JSON.parse(line) : { pass: false, error: `exit code ${code}, no result` };
        return {
          object: `${object}-evict-${i}`,
          delayMs,
          pass: result.pass,
          evicted: result.detail?.evicted ?? false,
          afterStepLagMs: result.detail?.afterStepLagMs,
          instancesConstructed: result.detail?.instancesConstructed,
          error: result.error,
        };
      })
    );
    console.log(JSON.stringify({ runs }));
    const failed = runs.filter((run) => !run.pass);
    check(failed.length === 0, `${failed.length} of ${runs.length} runs failed: ${JSON.stringify(failed)}`);
    return { runs };
  },

  // Starts a held step and samples the object until the execution ends. Run
  // `wrangler deploy` with a changed `padding.ts` about 20 s after it starts.
  // With `E2E_QUIET_MS` the driver sends nothing for that long after the
  // trigger, so no request keeps the object that runs the old code in use.
  async deploy() {
    const tag = `deploy-${Date.now()}`;
    const ms = Number(process.env.E2E_HOLD_MS ?? 60_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "inflight", params: { tag, ms } });
    console.log(JSON.stringify({ started: tag, t0, executionId: ack.executionId }));
    const changes: unknown[] = [];
    const errors: { t: number; error: string }[] = [];
    let last = "";
    let status: string | undefined;
    await sleep(Number(process.env.E2E_QUIET_MS ?? 0));
    while (Date.now() - t0 < ms * 2 + 180_000) {
      const state = await debug("/debug/state").catch((err) => ({ text: String(err) }));
      const t = Date.now() - t0;
      if (state.instance === undefined) {
        errors.push({ t, error: JSON.stringify(state).slice(0, 200) });
      } else {
        const seen = `${state.isolateId}/${state.instance}/${state.padding}`;
        if (seen !== last) {
          changes.push({
            t,
            instance: `${state.isolateId}/${state.instance}`,
            padding: state.padding,
            alarm: state.alarm && state.alarm - t0,
            nextClaimAt: state.nextClaimAt && state.nextClaimAt - t0,
          });
        }
        last = seen;
        status = (await sdk.poll(ack.executionId).catch(() => undefined))?.status;
        if (status === "completed" || status === "failed" || status === "cancelled") break;
      }
      await sleep(1000);
    }
    const view = await sdk.poll(ack.executionId);
    const found = await evidence(ack.executionId, tag, t0);
    const events = (name: string) => found.log.filter((l: any) => l.e === name);
    check(view?.status === "completed", `status ${view?.status}`);
    const value = (view as any).result;
    return {
      status: view?.status,
      completedAfterMs: value.at - t0,
      attempt: value.attempt,
      completedBy: value.instance,
      bodyStarted: events("hold-start").length,
      bodyFinished: events("hold-end").length,
      changes,
      errors,
      ...found,
    };
  },

  // A step that spins without I/O for `E2E_SPIN_ROUNDS` rounds. The driver's
  // clock times it, because the Worker's clock stands still during the loop.
  // With `E2E_SPIN_QUIET_MS` the driver sends nothing for that long after the
  // trigger, so no request arrives during the spin.
  async spin() {
    const tag = `spin-${Date.now()}`;
    const rounds = Number(process.env.E2E_SPIN_ROUNDS ?? 10);
    const quietMs = Number(process.env.E2E_SPIN_QUIET_MS ?? 0);
    const timeout = Number(process.env.E2E_SPIN_TIMEOUT_MS ?? 180_000);
    const t0 = Date.now();
    const ack = await sdk.trigger({ workflowId: "spin", params: { tag, rounds } });
    const triggerMs = Date.now() - t0;
    if (quietMs) await sleep(quietMs);
    const requests: { t: number; ms: number; status?: string; error?: string }[] = [];
    let view: any;
    while (Date.now() - t0 < timeout) {
      const sent = Date.now();
      try {
        view = await sdk.poll(ack.executionId);
        requests.push({ t: sent - t0, ms: Date.now() - sent, status: view?.status });
      } catch (err) {
        requests.push({ t: sent - t0, ms: Date.now() - sent, error: String(err).slice(0, 300) });
      }
      if (view?.status === "completed" || view?.status === "failed" || view?.status === "cancelled") break;
      await sleep(500);
    }
    const doneAfterMs = Date.now() - t0;
    let forced: unknown;
    if (view?.status !== "completed") {
      // Stops an execution that would otherwise be delivered and spin again.
      forced = await sdk.cancel(ack.executionId, { force: true }).catch((err) => String(err));
    }
    const found = await evidence(ack.executionId, tag, t0);
    const value = view?.result;
    return {
      rounds,
      quietMs,
      triggerMs,
      status: view?.status,
      driverDoneAfterMs: doneAfterMs,
      result: value,
      spinMsByWorkerClock: value && value.endedAt - value.startedAt,
      clockMovedDuringLoopMs: value && value.clockAfterLoop - value.startedAt,
      slowOrFailedRequests: requests.filter((r) => r.error || r.ms > 2000),
      requests: requests.length,
      forced,
      ...found,
    };
  },

  // Triggers a mix of executions at a steady rate with bursts, then checks
  // that each one reached the status the driver expected.
  async soak() {
    const durationMs = Number(process.env.E2E_SOAK_MS ?? 60 * 60_000);
    const rate = Number(process.env.E2E_SOAK_RATE ?? 0.5);
    const burstEveryMs = Number(process.env.E2E_SOAK_BURST_EVERY_MS ?? 5 * 60_000);
    const burstSize = Number(process.env.E2E_SOAK_BURST ?? 100);
    const run = `soak-${Date.now()}`;
    type Item = {
      id: string;
      kind: string;
      expected: string;
      // How long the execution should take by design, in milliseconds.
      expectedMs: number;
      triggered: boolean;
      cancelSentAfterMs?: number;
    };
    const items: Item[] = [];
    const errors: string[] = [];
    const lost: string[] = [];
    const pending = new Set<Promise<void>>();

    const launch = (n: number) => {
      const roll = Math.random();
      const id = `${run}-${n}`;
      const item: Item =
        roll < 0.35
          ? { id, kind: "plain", expected: "completed", expectedMs: 0, triggered: false }
          : roll < 0.7
            ? { id, kind: "delay", expected: "completed", expectedMs: 1000 + Math.floor(Math.random() * 29_000), triggered: false }
            : roll < 0.8
              ? { id, kind: "parent", expected: "completed", expectedMs: 400, triggered: false }
              : roll < 0.9
                ? { id, kind: "throws", expected: "failed", expectedMs: 0, triggered: false }
                : { id, kind: "cancel", expected: "cancelled", expectedMs: 0, triggered: false };
      items.push(item);
      const request =
        item.kind === "plain" || item.kind === "delay"
          ? { workflowId: "soak", params: item.kind === "delay" ? { delayMs: item.expectedMs } : {} }
          : item.kind === "parent"
            ? { workflowId: "parent" }
            : item.kind === "throws"
              ? { workflowId: "throws" }
              : { workflowId: "cancellable", params: { tag: id } };
      const work = (async () => {
        const sent = Date.now();
        await retry(() => sdk.trigger({ ...request, executionId: id, idempotent: true } as never), errors);
        item.triggered = true;
        if (item.kind !== "cancel") return;
        await sleep(1000 + Math.random() * 2000);
        item.cancelSentAfterMs = Date.now() - sent;
        await retry(() => sdk.cancel(id, { reason: "soak" }), errors);
      })().catch(() => {
        lost.push(id);
      });
      pending.add(work);
      void work.finally(() => pending.delete(work));
    };

    const t0 = Date.now();
    let n = 0;
    let nextBurst = t0 + burstEveryMs / 2;
    let nextReport = t0 + 60_000;
    while (Date.now() - t0 < durationMs) {
      launch(n++);
      if (Date.now() >= nextBurst) {
        nextBurst += burstEveryMs;
        for (let i = 0; i < burstSize; i++) launch(n++);
      }
      if (Date.now() >= nextReport) {
        nextReport += 60_000;
        console.log(JSON.stringify({ soak: run, t: Date.now() - t0, triggered: n, errors: errors.length, lost: lost.length }));
      }
      // Uneven gaps with the mean of the rate.
      await sleep((-Math.log(1 - Math.random()) * 1000) / rate);
    }
    await Promise.all(pending);
    const triggerEnd = Date.now();

    const expectedByStatus: Record<string, number> = {};
    const add = (key: string, by = 1) => (expectedByStatus[key] = (expectedByStatus[key] ?? 0) + by);
    for (const item of items) {
      if (!item.triggered) continue;
      const workflowId = item.kind === "plain" || item.kind === "delay" ? "soak" : item.kind === "cancel" ? "cancellable" : item.kind;
      add(`${workflowId}/${item.expected}`);
      if (item.kind === "parent") add("child/completed", 3);
    }

    const byId = new Map(items.map((item) => [item.id, item]));
    const inspect = async () => {
      const rows: any[] = await sql(
        "SELECT execution_id, workflow_id, status, admitted_at, updated_at, unacknowledged_delivery_count FROM executions WHERE execution_id LIKE ? OR parent_execution_id LIKE ?",
        `${run}-%`,
        `${run}-%`
      );
      const actualByStatus: Record<string, number> = {};
      const terminal = new Set(["completed", "failed", "cancelled"]);
      const notTerminal: unknown[] = [];
      const wrongStatus: unknown[] = [];
      const lags: { id: string; kind: string; lag: number }[] = [];
      for (const row of rows) {
        const key = `${row.workflow_id}/${row.status}`;
        actualByStatus[key] = (actualByStatus[key] ?? 0) + 1;
        if (!terminal.has(row.status)) {
          notTerminal.push(row);
          continue;
        }
        const item = byId.get(row.execution_id);
        if (item && item.expected !== row.status) wrongStatus.push({ ...row, expected: item.expected });
        const kind = item?.kind ?? row.workflow_id;
        const expectedMs = item ? (item.cancelSentAfterMs ?? item.expectedMs) : 200;
        lags.push({ id: row.execution_id, kind, lag: row.updated_at - row.admitted_at - expectedMs });
      }
      const distribution = (list: number[]) => {
        const sorted = [...list].sort((a, b) => a - b);
        return { n: sorted.length, p50: percentile(sorted, 50), p90: percentile(sorted, 90), p99: percentile(sorted, 99), max: sorted.at(-1) ?? null };
      };
      const kinds = [...new Set(lags.map((l) => l.kind))];
      const tables: { name: string }[] = await sql("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name");
      const rowCounts: Record<string, number> = {};
      for (const { name } of tables) {
        const counted = await sql(`SELECT count(*) AS n FROM "${name}"`);
        rowCounts[name] = counted[0]?.n ?? counted;
      }
      const state = await debug("/debug/state");
      return {
        t: Date.now() - t0,
        rows: rows.length,
        expectedByStatus,
        actualByStatus,
        matches: JSON.stringify(Object.entries(expectedByStatus).sort()) === JSON.stringify(Object.entries(actualByStatus).sort()),
        notTerminal: notTerminal.slice(0, 20),
        notTerminalCount: notTerminal.length,
        wrongStatus: wrongStatus.slice(0, 20),
        wrongStatusCount: wrongStatus.length,
        lagMs: { all: distribution(lags.map((l) => l.lag)), ...Object.fromEntries(kinds.map((k) => [k, distribution(lags.filter((l) => l.kind === k).map((l) => l.lag))])) },
        slowest: [...lags].sort((a, b) => b.lag - a.lag).slice(0, 10),
        over5s: lags.filter((l) => l.lag > 5000).length,
        redelivered: rows.filter((r) => r.unacknowledged_delivery_count > 0).length,
        // A delay writes attempt 0 when it starts and attempt 1 when it ends.
        stepAttempts: await sql("SELECT step_key, step_attempt, count(*) AS n FROM step_responses GROUP BY step_key, step_attempt ORDER BY step_key, step_attempt"),
        databaseSize: state.databaseSize,
        rowCounts,
        alarm: state.alarm,
        nextClaimAt: state.nextClaimAt,
        instances: await sql("SELECT isolate, instance, min(at) - ? AS firstT, max(at) - ? AS lastT, count(*) AS rows FROM e2e_log GROUP BY isolate, instance ORDER BY firstT", t0, t0),
        logged: await sql("SELECT event, substr(detail, 1, 300) AS detail, count(*) AS n FROM e2e_log WHERE event LIKE 'log-%' GROUP BY event, substr(detail, 1, 120) ORDER BY n DESC LIMIT 30"),
      };
    };

    // The longest delay is 30 s.
    await sleep(Number(process.env.E2E_SETTLE_MS ?? 45_000));
    const atEnd = await inspect();
    console.log(JSON.stringify({ soak: run, atEnd }));
    await sleep(120_000);
    const later = await inspect();
    check(later.notTerminalCount === 0, `${later.notTerminalCount} executions are not terminal`);
    check(later.matches, `status counts differ: ${JSON.stringify(later.actualByStatus)} expected ${JSON.stringify(expectedByStatus)}`);
    return {
      run,
      triggered: items.filter((i) => i.triggered).length,
      triggerPhaseMs: triggerEnd - t0,
      lost,
      requestErrors: errors.length,
      requestErrorSamples: [...new Set(errors)].slice(0, 10),
      atEnd,
      later,
    };
  },
});

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
