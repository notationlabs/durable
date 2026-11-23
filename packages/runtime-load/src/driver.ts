import type {
  ExecutionListRequest,
  ExecutionView,
  ExecutionViewPage,
  ExecutionWaitOptions,
  WaitResult,
} from "@durable/core";
import { loadKinds, type LoadKind, type LoadWorkflowId } from "./kinds";

/**
 * The SDK methods the driver calls. The local SDK, `createTestSdk` and the
 * HTTP SDK all have them.
 */
export type LoadSdk = {
  trigger(event: {
    workflowId: LoadWorkflowId;
    executionId: string;
    params?: undefined;
  }): Promise<{ executionId: string }>;
  wait(executionId: string, options?: ExecutionWaitOptions): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
};

export type LoadTarget = {
  sdk: LoadSdk;
  /**
   * Reads numbers from the runtime under load, such as rows written or the
   * database size. The driver calls it before and after each stage.
   */
  probe?: () => Promise<Record<string, number>>;
};

type TriggeredKind = Exclude<LoadKind, "child">;

export type LoadOptions = {
  /** Trigger rates, in executions per second. Each stage holds one rate. */
  stages: number[];
  /** How long each stage sends triggers, in milliseconds. */
  stageMs: number;
  /**
   * Relative weights of the kinds the driver triggers.
   * @default { single: 35, ten: 20, delay: 25, parent: 10, fail: 10 }
   */
  mix?: Partial<Record<TriggeredKind, number>>;
  /**
   * How long the driver waits after a stage's last trigger for its executions
   * to end, in milliseconds. Executions still open after it count as
   * unexpected.
   * @default 60000
   */
  settleTimeoutMs?: number;
  /**
   * Single-step executions to run to the end before the first stage, so the
   * stages run against a runtime that already holds finished executions.
   * @default 0
   */
  prefill?: number;
  /** @default 20 */
  prefillConcurrency?: number;
  /** Seeds the gaps between triggers and the choice of kind. @default the clock */
  seed?: number;
  /** Prefixes every execution ID. @default `load-` and the clock in base 36 */
  runId?: string;
  log?: (message: string) => void;
};

export type Distribution = {
  n: number;
  p50: number | null;
  p90: number | null;
  p99: number | null;
  max: number | null;
};

type Unexpected = { executionId: string; kind: LoadKind; expected: string; status: string };

export type LoadStageReport = {
  /** The rate the stage aimed for, in triggers per second. */
  rate: number;
  /** Triggers that resolved, divided by the stage's length. */
  sentRate: number;
  triggered: number;
  triggerErrors: number;
  triggerErrorSamples: string[];
  /** Time for `trigger` to resolve, measured by the driver. */
  triggerMs: Distribution;
  /** Executions the stage triggered, and the children they started. */
  executions: number;
  /** Executions by status at the end of the stage, including statuses that are not terminal. */
  ended: Record<string, number>;
  /**
   * `updatedAt - admittedAt` of each terminal execution, less the time its
   * delays take by design.
   */
  lagMs: Distribution;
  lagMsByKind: Partial<Record<LoadKind, Distribution>>;
  /** Time from the stage's last trigger until every execution ended or the settle timeout passed. */
  settleMs: number;
  settled: boolean;
  unexpected: number;
  unexpectedSamples: Unexpected[];
  probeBefore?: Record<string, number>;
  probeAfter?: Record<string, number>;
};

export type LoadReport = {
  runId: string;
  seed: number;
  startedAt: string;
  options: Omit<LoadOptions, "log">;
  prefill?: {
    executions: number;
    ms: number;
    unexpected: number;
    probeAfter?: Record<string, number>;
  };
  stages: LoadStageReport[];
  /** True when every execution ended in the status its kind should end in. */
  ok: boolean;
};

const DEFAULT_MIX: Record<TriggeredKind, number> = {
  single: 35,
  ten: 20,
  delay: 25,
  parent: 10,
  fail: 10,
};

const LIST_LIMIT = 1000;
const TERMINAL = new Set(["completed", "failed", "cancelled"]);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Triggers the load router's workflows through `target.sdk` at each rate in
 * `options.stages`, and reads the outcome of every execution back through
 * `list`. Each stage waits for its executions to end before the next starts.
 */
export async function runLoad(target: LoadTarget, options: LoadOptions): Promise<LoadReport> {
  const { sdk, probe } = target;
  const { log = () => {}, ...reported } = options;
  const seed = options.seed ?? Date.now() % 2 ** 32;
  const random = mulberry32(seed);
  const runId = options.runId ?? `load-${Date.now().toString(36)}`;
  const settleTimeoutMs = options.settleTimeoutMs ?? 60_000;
  const mix = Object.entries({ ...DEFAULT_MIX, ...options.mix }).filter(
    ([, weight]) => weight > 0
  ) as [TriggeredKind, number][];
  const totalWeight = mix.reduce((sum, [, weight]) => sum + weight, 0);
  if (!options.stages.every((rate) => rate > 0)) throw new RangeError("every stage rate must be positive");
  if (!(totalWeight > 0)) throw new RangeError("mix must give at least one kind a positive weight");

  const pickKind = (): TriggeredKind => {
    let roll = random() * totalWeight;
    for (const [kind, weight] of mix) {
      roll -= weight;
      if (roll < 0) return kind;
    }
    return mix[mix.length - 1][0];
  };

  const report: LoadReport = {
    runId,
    seed,
    startedAt: new Date().toISOString(),
    options: reported,
    stages: [],
    ok: true,
  };

  if (options.prefill) {
    const count = options.prefill;
    log(`prefill: ${count} executions`);
    const started = Date.now();
    const roots = new Map<string, LoadKind>();
    let next = 0;
    const lane = async () => {
      while (next < count) {
        const executionId = `${runId}-prefill-${next++}`;
        await sdk.trigger({ workflowId: loadKinds.single.workflowId, executionId });
        roots.set(executionId, "single");
      }
    };
    await Promise.all(Array.from({ length: options.prefillConcurrency ?? 20 }, lane));
    const { views } = await settle(sdk, roots, Date.now() + Math.max(settleTimeoutMs, count * 100));
    const unexpected = findUnexpected(roots, views);
    report.prefill = {
      executions: count,
      ms: Date.now() - started,
      unexpected: unexpected.length,
      probeAfter: await probe?.(),
    };
    if (unexpected.length) report.ok = false;
  }

  for (const [index, rate] of options.stages.entries()) {
    log(`stage ${index + 1}/${options.stages.length}: ${rate}/s for ${options.stageMs} ms`);
    const probeBefore = await probe?.();
    const roots = new Map<string, LoadKind>();
    const triggerMs: number[] = [];
    const errors: string[] = [];
    const pending = new Set<Promise<void>>();

    const launch = (n: number) => {
      const kind = pickKind();
      const executionId = `${runId}-${index}-${n}`;
      const sent = performance.now();
      const work = sdk
        .trigger({ workflowId: loadKinds[kind].workflowId, executionId })
        .then(
          () => {
            triggerMs.push(performance.now() - sent);
            roots.set(executionId, kind);
          },
          (err) => {
            errors.push(String(err).slice(0, 200));
          }
        )
        .finally(() => pending.delete(work));
      pending.add(work);
    };

    // Gaps between triggers are exponential with a mean of 1 / rate, so
    // triggers arrive unevenly. Each gap counts from the previous planned
    // time, so a late timer does not lower the rate.
    const t0 = Date.now();
    let planned = t0;
    let n = 0;
    while (true) {
      planned += (-Math.log(1 - random()) * 1000) / rate;
      if (planned - t0 >= options.stageMs) break;
      const wait = planned - Date.now();
      if (wait > 0) await sleep(wait);
      launch(n++);
    }
    await Promise.all(pending);
    const sentAt = Date.now();

    const { views, settled } = await settle(sdk, roots, sentAt + settleTimeoutMs);
    const settleMs = Date.now() - sentAt;
    const unexpected = findUnexpected(roots, views);
    const ended: Record<string, number> = {};
    const lags: { kind: LoadKind; lag: number }[] = [];
    for (const { view, kind } of views.values()) {
      ended[view.status] = (ended[view.status] ?? 0) + 1;
      if (TERMINAL.has(view.status)) {
        lags.push({ kind, lag: view.updatedAt - view.admittedAt - loadKinds[kind].delayMs });
      }
    }
    const kinds = [...new Set(lags.map((l) => l.kind))].sort();
    const stage: LoadStageReport = {
      rate,
      sentRate: roots.size / (options.stageMs / 1000),
      triggered: roots.size,
      triggerErrors: errors.length,
      triggerErrorSamples: [...new Set(errors)].slice(0, 5),
      triggerMs: distribution(triggerMs),
      executions: views.size,
      ended,
      lagMs: distribution(lags.map((l) => l.lag)),
      lagMsByKind: Object.fromEntries(
        kinds.map((kind) => [kind, distribution(lags.filter((l) => l.kind === kind).map((l) => l.lag))])
      ),
      settleMs,
      settled,
      unexpected: unexpected.length,
      unexpectedSamples: unexpected.slice(0, 10),
      probeBefore,
      probeAfter: await probe?.(),
    };
    if (stage.unexpected) report.ok = false;
    report.stages.push(stage);
    log(
      `stage ${index + 1}: ${stage.triggered} triggered, lag p50 ${stage.lagMs.p50} ms, p99 ${stage.lagMs.p99} ms, settled in ${settleMs} ms`
    );
  }
  return report;
}

type Seen = Map<string, { view: ExecutionView; kind: LoadKind }>;

/**
 * Reads the views of `roots` and of the children they started, newest first,
 * and stops once it has seen every root. Children are admitted after their
 * parent, so they come before it in the listing.
 */
async function collect(sdk: LoadSdk, roots: Map<string, LoadKind>): Promise<Seen> {
  const views: Seen = new Map();
  let found = 0;
  let cursor: string | undefined;
  do {
    const page = await sdk.list({ limit: LIST_LIMIT, ...(cursor ? { cursor } : {}) });
    for (const view of page.executions) {
      const kind = roots.get(view.executionId);
      if (kind) {
        found++;
        views.set(view.executionId, { view, kind });
      } else if (view.parent && roots.has(view.parent.executionId)) {
        views.set(view.executionId, { view, kind: "child" });
      }
    }
    cursor = page.cursor;
  } while (cursor && found < roots.size);
  return views;
}

/** Lists until every root and child is terminal, or `deadline` passes. */
async function settle(sdk: LoadSdk, roots: Map<string, LoadKind>, deadline: number) {
  while (true) {
    const views = await collect(sdk, roots);
    const open = [...views.values()].filter(({ view }) => !TERMINAL.has(view.status));
    const missing = [...roots.keys()].some((id) => !views.has(id));
    if (!open.length && !missing) return { views, settled: true };
    const remaining = deadline - Date.now();
    if (remaining <= 0) return { views, settled: false };
    // Waiting on the oldest open execution returns as soon as it ends, instead
    // of listing again after a fixed pause.
    const oldest = open.at(-1);
    if (oldest) await sdk.wait(oldest.view.executionId, { timeout: Math.min(1000, remaining) });
    else await sleep(Math.min(250, remaining));
  }
}

function findUnexpected(roots: Map<string, LoadKind>, views: Seen): Unexpected[] {
  const unexpected: Unexpected[] = [];
  for (const [executionId, kind] of roots) {
    if (!views.has(executionId)) {
      unexpected.push({ executionId, kind, expected: loadKinds[kind].expected, status: "missing" });
    }
  }
  for (const [executionId, { view, kind }] of views) {
    const expected = loadKinds[kind].expected;
    if (view.status !== expected) unexpected.push({ executionId, kind, expected, status: view.status });
  }
  return unexpected;
}

function distribution(values: number[]): Distribution {
  const sorted = [...values].sort((a, b) => a - b);
  const at = (p: number) =>
    sorted.length ? round(sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]) : null;
  return { n: sorted.length, p50: at(50), p90: at(90), p99: at(99), max: sorted.length ? round(sorted[sorted.length - 1]) : null };
}

const round = (value: number) => Math.round(value * 10) / 10;

function mulberry32(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Formats a report as plain-text tables: one row per stage, then lag by kind, then probe values. */
export function formatLoadReport(report: LoadReport): string {
  const show = (value: number | null | undefined) => (value === null || value === undefined ? "-" : String(value));
  const quartet = (d: Distribution) => `${show(d.p50)} / ${show(d.p90)} / ${show(d.p99)} / ${show(d.max)}`;
  const sections: string[][][] = [];

  sections.push([
    ["rate/s", "sent/s", "triggered", "errors", "trigger p50 / p99 ms", "ended", "lag p50 / p90 / p99 / max ms", "settle ms", "unexpected"],
    ...report.stages.map((s) => [
      String(s.rate),
      String(round(s.sentRate)),
      String(s.triggered),
      String(s.triggerErrors),
      `${show(s.triggerMs.p50)} / ${show(s.triggerMs.p99)}`,
      Object.entries(s.ended)
        .sort()
        .map(([status, n]) => `${status} ${n}`)
        .join(", "),
      quartet(s.lagMs),
      `${s.settleMs}${s.settled ? "" : " (timed out)"}`,
      String(s.unexpected),
    ]),
  ]);

  const kinds = [...new Set(report.stages.flatMap((s) => Object.keys(s.lagMsByKind)))].sort() as LoadKind[];
  sections.push([
    ["rate/s", ...kinds.map((kind) => `${kind} lag p50 / p99 ms`)],
    ...report.stages.map((s) => [
      String(s.rate),
      ...kinds.map((kind) => {
        const d = s.lagMsByKind[kind];
        return d ? `${show(d.p50)} / ${show(d.p99)}` : "-";
      }),
    ]),
  ]);

  const keys = [...new Set(report.stages.flatMap((s) => Object.keys({ ...s.probeBefore, ...s.probeAfter })))].sort();
  if (keys.length) {
    sections.push([
      ["rate/s", ...keys.map((key) => `${key} before -> after`)],
      ...report.stages.map((s) => [
        String(s.rate),
        ...keys.map((key) => `${show(s.probeBefore?.[key])} -> ${show(s.probeAfter?.[key])}`),
      ]),
    ]);
  }

  const lines: string[] = [];
  if (report.prefill) {
    lines.push(
      `prefill: ${report.prefill.executions} executions in ${report.prefill.ms} ms, ${report.prefill.unexpected} unexpected`
    );
  }
  for (const rows of sections) {
    const widths = rows[0].map((_, i) => Math.max(...rows.map((row) => row[i].length)));
    for (const row of rows) lines.push(row.map((cell, i) => cell.padEnd(widths[i])).join("  ").trimEnd());
    lines.push("");
  }
  lines.push(report.ok ? "Every execution ended in its expected status." : "Some executions did not end in their expected status.");
  return lines.join("\n");
}
