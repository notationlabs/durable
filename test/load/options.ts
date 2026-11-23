import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { formatLoadReport, type LoadOptions, type LoadReport } from "@durable/runtime-load";

const numbers = (value: string | undefined) => value?.split(",").map(Number);

/** Reads the load options from `LOAD_*` environment variables, with defaults for a run of about a minute. */
export function loadOptions(): LoadOptions {
  const env = process.env;
  return {
    stages: numbers(env.LOAD_STAGES) ?? [1, 5, 20, 50],
    stageMs: Number(env.LOAD_STAGE_MS ?? 10_000),
    settleTimeoutMs: Number(env.LOAD_SETTLE_MS ?? 60_000),
    prefill: Number(env.LOAD_PREFILL ?? 0),
    ...(env.LOAD_SEED ? { seed: Number(env.LOAD_SEED) } : {}),
    log: (message) => console.error(message),
  };
}

/**
 * Prints the report's tables and writes the report as JSON to `LOAD_OUT`, or
 * to `test/load/reports/<runtime>-<run ID>.json`. Exits with 1 when an
 * execution did not end in its expected status.
 */
export function finish(runtime: string, report: LoadReport) {
  const path =
    process.env.LOAD_OUT ?? join(import.meta.dirname, "reports", `${runtime}-${report.runId}.json`);
  mkdirSync(join(path, ".."), { recursive: true });
  writeFileSync(path, JSON.stringify({ runtime, ...report }, null, 2));
  console.log(`\n${runtime}\n\n${formatLoadReport(report)}\nReport: ${path}`);
  process.exitCode = report.ok ? 0 : 1;
}
