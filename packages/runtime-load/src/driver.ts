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
   * database size. The driver calls it before and after each stage. A probe
   * that throws is recorded as an observation error, and its values are left
   * out.
   */
  probe?: () => Promise<Record<string, number>>;
  /**
   * A probe key whose value changes when the runtime restarts, such as the
   * time the instance was constructed. The report marks a stage where it
   * changed as `restarted`, because counters held in memory start again from
   * zero there.
   */
  instanceKey?: string;
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
   * to end, in milliseconds. When executions are still open after it, they
   * count as unexpected and the ramp stops.
   * @default 60000
   */
  settleTimeoutMs?: number;
  /**
   * Stops the ramp after a stage whose lag p50 is above this many
   * milliseconds. Without it, only a stage that does not settle stops the
   * ramp.
   */
  maxLagMs?: number;
  /**
   * Times every nth triggered execution from the client: from sending the
   * trigger until `wait` returns it terminal. Use it where the runtime's own
   * timestamps cannot show lag, as on Cloudflare. When set, `maxLagMs` applies
   * to this measure. Each timed execution adds `wait` requests to the load.
   */
  waitEvery?: number;
  /**
   * Single-step executions to run to the end before the first stage, so the
   * stages run against a runtime that already holds finished executions.
   * @default 0
   */
  prefill?: number;
  /** How many pre-fill triggers are in flight at once. @default 20 */
  prefillConcurrency?: number;
  /** The `limit` of each `list` request. @default 1000 */
  listLimit?: number;
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
  /** Triggers that rejected, such as a `503` from a Worker in front of the runtime. */
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
  /**
   * With `waitEvery`: time from sending the trigger until `wait` returned the
   * execution terminal, less the time its delays take by design. Includes the
   * network time of the trigger request and of the last `wait` response.
   */
  waitedLagMs?: Distribution;
  /** Time from the stage's last trigger until every execution ended or the settle timeout passed. */
  settleMs: number;
  settled: boolean;
  unexpected: number;
  unexpectedSamples: Unexpected[];
  /** Failed `list`, `wait` and probe calls. The driver retries them until the settle timeout. */
  observeErrors: number;
  observeErrorSamples: string[];
  probeBefore?: Record<string, number>;
  probeAfter?: Record<string, number>;
  /** The `instanceKey` probe value changed during the stage. */
  restarted?: boolean;
};

export type LoadReport = {
  runId: string;
  seed: number;
  startedAt: string;
  options: Omit<LoadOptions, "log">;
  prefill?: {
    executions: number;
    triggerErrors: number;
    triggerErrorSamples: string[];
    ms: number;
    settled: boolean;
    unexpected: number;
    observeErrors: number;
    probeBefore?: Record<string, number>;
    probeAfter?: Record<string, number>;
    restarted?: boolean;
  };
  stages: LoadStageReport[];
  /** Why the driver ran fewer stages than `options.stages` lists. */
  stoppedEarly?: string;
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

const TERMINAL = new Set(["completed", "failed", "cancelled"]);
/** The ramp stops after a stage in which more than this share of triggers rejected. */
const MAX_TRIGGER_ERROR_SHARE = 0.1;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

class Errors {
  count = 0;
  private seen = new Set<string>();

  add(err: unknown) {
    this.count++;
    if (this.seen.size < 5) this.seen.add(String(err).slice(0, 200));
  }

  get samples() {
    return [...this.seen];
  }
}

/**
 * Triggers the load router's workflows through `target.sdk` at each rate in
 * `options.stages`, and reads the outcome of every execution back through
 * `list`. Each stage waits for its executions to end before the next starts.
 * The ramp stops early after a stage that does not settle, whose lag p50 is
 * above `maxLagMs`, or in which more than a tenth of triggers rejected.
 */
export async function runLoad(target: LoadTarget, options: LoadOptions): Promise<LoadReport> {
  const { sdk, instanceKey } = target;
  const { log = () => {}, ...reported } = options;
  const seed = options.seed ?? Date.now() % 2 ** 32;
  const random = mulberry32(seed);
  const runId = options.runId ?? `load-${Date.now().toString(36)}`;
  const settleTimeoutMs = options.settleTimeoutMs ?? 60_000;
  const listLimit = options.listLimit ?? 1000;
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

  const probe = async (errors: Errors) => {
    if (!target.probe) return undefined;
    try {
      return await target.probe();
    } catch (err) {
      errors.add(err);
      return undefined;
    }
  };
  const restarted = (before?: Record<string, number>, after?: Record<string, number>) =>
    instanceKey && before?.[instanceKey] !== undefined && after?.[instanceKey] !== undefined
      ? before[instanceKey] !== after[instanceKey]
      : undefined;

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
    const concurrency = options.prefillConcurrency ?? 20;
    log(`prefill: ${count} executions, ${concurrency} triggers in flight`);
    const errors = new Errors();
    const triggerErrors = new Errors();
    const probeBefore = await probe(errors);
    const started = Date.now();
    const roots = new Map<string, LoadKind>();
    let next = 0;
    let lastLog = started;
    const lane = async () => {
      while (next < count) {
        const executionId = `${runId}-prefill-${next++}`;
        try {
          await sdk.trigger({ workflowId: loadKinds.single.workflowId, executionId });
          roots.set(executionId, "single");
        } catch (err) {
          triggerErrors.add(err);
        }
        if (Date.now() - lastLog >= 10_000) {
          lastLog = Date.now();
          log(`prefill: ${roots.size} triggered, ${triggerErrors.count} errors`);
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, lane));
    log(`prefill: triggered in ${Date.now() - started} ms, waiting for executions to end`);
    const { views, settled } = await settle(
      sdk,
      roots,
      Date.now() + Math.max(settleTimeoutMs, count * 20),
      listLimit,
      errors
    );
    const unexpected = findUnexpected(roots, views);
    const probeAfter = await probe(errors);
    report.prefill = {
      executions: roots.size,
      triggerErrors: triggerErrors.count,
      triggerErrorSamples: triggerErrors.samples,
      ms: Date.now() - started,
      settled,
      unexpected: unexpected.length,
      observeErrors: errors.count,
      probeBefore,
      probeAfter,
      restarted: restarted(probeBefore, probeAfter),
    };
    log(`prefill: done in ${report.prefill.ms} ms`);
    if (unexpected.length) report.ok = false;
    if (!settled) {
      report.stoppedEarly = `pre-fill executions were still open after ${Math.max(settleTimeoutMs, count * 20)} ms`;
      return report;
    }
  }

  for (const [index, rate] of options.stages.entries()) {
    log(`stage ${index + 1}/${options.stages.length}: ${rate}/s for ${options.stageMs} ms`);
    const errors = new Errors();
    const triggerErrors = new Errors();
    const probeBefore = await probe(errors);
    const roots = new Map<string, LoadKind>();
    const triggerMs: number[] = [];
    const pending = new Set<Promise<void>>();
    const waitedLags: number[] = [];
    // Kept apart from `pending`, so that settle time counts from the last
    // trigger and not from the end of the timed waits.
    const timed: Promise<void>[] = [];
    const waitDeadline = Date.now() + options.stageMs + settleTimeoutMs;

    const timeUntilEnd = async (executionId: string, kind: LoadKind, sent: number) => {
      while (Date.now() < waitDeadline) {
        try {
          const result = await sdk.wait(executionId, {
            timeout: Math.min(10_000, waitDeadline - Date.now()),
          });
          if (result && result.status !== "timed-out") {
            waitedLags.push(performance.now() - sent - loadKinds[kind].delayMs);
            return;
          }
        } catch (err) {
          errors.add(err);
          await sleep(1000);
        }
      }
    };

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
            if (options.waitEvery && n % options.waitEvery === 0) timed.push(timeUntilEnd(executionId, kind, sent));
          },
          (err) => triggerErrors.add(err)
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

    const { views, settled, read } = await settle(sdk, roots, sentAt + settleTimeoutMs, listLimit, errors);
    const settleMs = Date.now() - sentAt;
    await Promise.all(timed);
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
    const probeAfter = await probe(errors);
    const stage: LoadStageReport = {
      rate,
      sentRate: roots.size / (options.stageMs / 1000),
      triggered: roots.size,
      triggerErrors: triggerErrors.count,
      triggerErrorSamples: triggerErrors.samples,
      triggerMs: distribution(triggerMs),
      executions: views.size,
      ended,
      lagMs: distribution(lags.map((l) => l.lag)),
      lagMsByKind: Object.fromEntries(
        kinds.map((kind) => [kind, distribution(lags.filter((l) => l.kind === kind).map((l) => l.lag))])
      ),
      ...(options.waitEvery ? { waitedLagMs: distribution(waitedLags) } : {}),
      settleMs,
      settled,
      unexpected: unexpected.length,
      unexpectedSamples: unexpected.slice(0, 10),
      observeErrors: errors.count,
      observeErrorSamples: errors.samples,
      probeBefore,
      probeAfter,
      restarted: restarted(probeBefore, probeAfter),
    };
    if (stage.unexpected) report.ok = false;
    report.stages.push(stage);
    log(
      `stage ${index + 1}: ${stage.triggered} triggered, ${stage.triggerErrors} trigger errors, lag p50 ${stage.lagMs.p50} ms, p99 ${stage.lagMs.p99} ms${stage.waitedLagMs ? `, waited lag p50 ${stage.waitedLagMs.p50} ms (n ${stage.waitedLagMs.n})` : ""}, settled ${settled} in ${settleMs} ms`
    );

    const watchedLag = stage.waitedLagMs ?? stage.lagMs;
    const last = index === options.stages.length - 1;
    const attempts = stage.triggered + stage.triggerErrors;
    const stop =
      attempts > 0 && stage.triggerErrors / attempts > MAX_TRIGGER_ERROR_SHARE
        ? `${stage.triggerErrors} of ${attempts} triggers of the ${rate}/s stage rejected, for example: ${stage.triggerErrorSamples[0]}`
        : !settled && !read
          ? `the executions of the ${rate}/s stage could not be listed before the settle timeout, for example: ${stage.observeErrorSamples[0]}`
          : !settled
            ? `executions of the ${rate}/s stage were still open ${settleTimeoutMs} ms after its last trigger`
            : options.maxLagMs !== undefined && (watchedLag.p50 ?? 0) > options.maxLagMs
              ? `${stage.waitedLagMs ? "waited lag" : "lag"} p50 of the ${rate}/s stage was ${watchedLag.p50} ms, above maxLagMs ${options.maxLagMs}`
              : undefined;
    if (stop && !last) {
      report.stoppedEarly = stop;
      log(`stopping: ${stop}`);
      break;
    }
  }
  return report;
}

type Seen = Map<string, { view: ExecutionView; kind: LoadKind }>;

/**
 * Reads the views of `roots` and of the children they started, newest first,
 * and stops once it has seen every root. Children are admitted after their
 * parent, so they come before it in the listing. Older executions, such as
 * pre-filled ones, are not read.
 */
async function collect(sdk: LoadSdk, roots: Map<string, LoadKind>, limit: number): Promise<Seen> {
  const views: Seen = new Map();
  let found = 0;
  let cursor: string | undefined;
  do {
    const page = await sdk.list({ limit, ...(cursor ? { cursor } : {}) });
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

/**
 * Lists until every root and child is terminal, or `deadline` passes.
 * Between listings it waits on the newest open execution: by the time it
 * ends, the older ones have usually ended too, so the driver lists the stage
 * only a few times. A failed call is recorded and tried again after a pause.
 */
async function settle(
  sdk: LoadSdk,
  roots: Map<string, LoadKind>,
  deadline: number,
  limit: number,
  errors: Errors
) {
  let views: Seen = new Map();
  if (!roots.size) return { views, settled: true, read: true };
  // Whether the latest listing succeeded, so a timeout can be told apart
  // from a runtime that could not be read.
  let read = false;
  while (true) {
    let open: { view: ExecutionView }[] = [];
    try {
      views = await collect(sdk, roots, limit);
      read = true;
      open = [...views.values()].filter(({ view }) => !TERMINAL.has(view.status));
      const missing = [...roots.keys()].some((id) => !views.has(id));
      if (!open.length && !missing) return { views, settled: true, read };
    } catch (err) {
      read = false;
      errors.add(err);
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) return { views, settled: false, read };
    const newest = open[0];
    try {
      if (newest) {
        await sdk.wait(newest.view.executionId, { timeout: Math.min(10_000, remaining) });
        continue;
      }
    } catch (err) {
      errors.add(err);
    }
    await sleep(Math.min(1000, remaining));
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

  const waited = report.stages.some((s) => s.waitedLagMs);
  sections.push([
    [
      "rate/s",
      "sent/s",
      "triggered",
      "errors",
      "trigger p50 / p99 ms",
      "ended",
      "lag p50 / p90 / p99 / max ms",
      ...(waited ? ["waited lag p50 / p90 / p99 / max ms (n)"] : []),
      "settle ms",
      "unexpected",
    ],
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
      ...(waited ? [s.waitedLagMs ? `${quartet(s.waitedLagMs)} (${s.waitedLagMs.n})` : "-"] : []),
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
        `${s.rate}${s.restarted ? " (restarted)" : ""}`,
        ...keys.map((key) => `${show(s.probeBefore?.[key])} -> ${show(s.probeAfter?.[key])}`),
      ]),
    ]);
  }

  const lines: string[] = [];
  const prefill = report.prefill;
  if (prefill) {
    lines.push(
      `prefill: ${prefill.executions} executions in ${prefill.ms} ms, ${prefill.triggerErrors} trigger errors, ${prefill.unexpected} unexpected${prefill.settled ? "" : ", timed out"}${prefill.restarted ? ", restarted" : ""}`,
      ""
    );
  }
  for (const rows of sections) {
    const widths = rows[0].map((_, i) => Math.max(...rows.map((row) => row[i].length)));
    for (const row of rows) lines.push(row.map((cell, i) => cell.padEnd(widths[i])).join("  ").trimEnd());
    lines.push("");
  }
  const observeErrors = report.stages.reduce((sum, s) => sum + s.observeErrors, prefill?.observeErrors ?? 0);
  if (observeErrors) lines.push(`${observeErrors} list, wait or probe calls failed and were tried again.`);
  if (report.stages.some((s) => s.restarted)) {
    lines.push("(restarted): the runtime restarted during the stage, so counters held in memory started again from zero.");
  }
  if (report.stoppedEarly) lines.push(`Stopped early: ${report.stoppedEarly}.`);
  lines.push(report.ok ? "Every execution ended in its expected status." : "Some executions did not end in their expected status.");
  return lines.join("\n");
}
