import type { ExecutionClient, ExecutionRecord } from "../base/execution";
import { isTerminalExecutionStatus } from "../base/execution";
import {
  isTerminalExecutionView,
  toExecutionView,
  type ExecutionObserver,
  type ExecutionWaitOptions,
} from "../base/observation";

/** How often `wait` reads the record, in milliseconds. */
export const DEFAULT_OBSERVER_POLL_INTERVAL = 25;

/**
 * A signal, keyed by execution ID, that an execution may have changed, such
 * as an invoker's `workflowEndEmitter`. A wait that receives one reads the
 * record early.
 */
export type ExecutionNotifier = {
  on(executionId: string, listener: (...args: any[]) => void): unknown;
  off(executionId: string, listener: (...args: any[]) => void): unknown;
};

export type ExecutionObserverOptions = {
  /**
   * How often `wait` reads the record, in milliseconds. It must be greater
   * than zero.
   * @default 25
   */
  pollInterval?: number;
  /** An optional fast path. No result depends on it. */
  notifier?: ExecutionNotifier;
};

/**
 * Observes executions through `executions`. `wait` reads the record, and then
 * reads it again every `pollInterval` until it is terminal, so it returns
 * outcomes recorded by any process sharing the backend, before or after the
 * call, and across a restart.
 */
export function createExecutionObserver(
  executions: Pick<ExecutionClient, "get" | "list">,
  options: ExecutionObserverOptions = {}
): ExecutionObserver {
  const pollInterval = options.pollInterval ?? DEFAULT_OBSERVER_POLL_INTERVAL;
  if (!isDuration(pollInterval) || pollInterval === 0) {
    throw new RangeError("pollInterval must be a positive number of milliseconds");
  }

  return {
    async poll(executionId) {
      const record = await executions.get(executionId);
      return record && toExecutionView(record);
    },

    async wait(executionId, waitOptions?: ExecutionWaitOptions) {
      const timeout = waitOptions?.timeout ?? Infinity;
      assertDuration("timeout", timeout);
      const record = await waitForExecutionRecord(executions, executionId, {
        pollInterval,
        notifier: options.notifier,
        deadline: Date.now() + timeout,
        signal: waitOptions?.signal,
      });
      if (!record) return undefined;
      const view = toExecutionView(record);
      if (isTerminalExecutionView(view)) return view;
      return { status: "timed-out", execution: view };
    },

    async list(request) {
      const page = await executions.list(request);
      return {
        executions: page.executions.map(toExecutionView),
        ...(page.cursor !== undefined ? { cursor: page.cursor } : {}),
      };
    },
  };
}

/**
 * Reads the record until it is terminal or `deadline` passes, and returns the
 * last read. Returns undefined if no record exists. Rejects with the signal's
 * reason once `signal` aborts.
 */
export async function waitForExecutionRecord(
  executions: Pick<ExecutionClient, "get">,
  executionId: string,
  options: {
    pollInterval: number;
    /** A `Date.now()` time. */
    deadline: number;
    notifier?: ExecutionNotifier;
    signal?: AbortSignal;
  }
): Promise<ExecutionRecord | undefined> {
  const { notifier, signal } = options;
  // A notification that arrives while the record is being read must still
  // cut the next sleep short, so it is remembered until then.
  let notified = false;
  let wake: (() => void) | undefined;
  const onNotify = () => {
    notified = true;
    wake?.();
  };
  const onAbort = () => wake?.();
  notifier?.on(executionId, onNotify);
  signal?.addEventListener("abort", onAbort);
  try {
    while (true) {
      signal?.throwIfAborted();
      notified = false;
      const record = await executions.get(executionId);
      if (!record || isTerminalExecutionStatus(record.status)) return record;
      const remaining = options.deadline - Date.now();
      if (remaining <= 0) return record;
      if (notified || signal?.aborted) continue;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(done, Math.min(options.pollInterval, remaining));
        function done() {
          clearTimeout(timer);
          wake = undefined;
          resolve();
        }
        wake = done;
      });
    }
  } finally {
    notifier?.off(executionId, onNotify);
    signal?.removeEventListener("abort", onAbort);
  }
}

/** Whether `value` is a non-negative number of milliseconds, as a wait `timeout` must be. */
export function isDuration(value: unknown): value is number {
  return typeof value === "number" && !Number.isNaN(value) && value >= 0;
}

function assertDuration(name: string, value: unknown) {
  if (!isDuration(value)) {
    throw new RangeError(`${name} must be a non-negative number of milliseconds`);
  }
}
