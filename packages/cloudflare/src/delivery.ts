import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WorkerExitedError,
  WorkflowRunner,
  decodeExecutionEvent,
  decodeWorkflowRunOutcome,
  encodeExecutionEvent,
  encodeWorkflowRunOutcome,
  type EncodedWorkflowRunOutcome,
  type ExecutionCancellation,
  type ExecutionLease,
  type HeapRecord,
  type Runtime,
  type WorkflowEvent,
  type WorkflowInvoker,
  type WorkflowRouter,
} from "@durable/core";
import {
  RemoteRuntime,
  decodeError,
  encodeError,
  serveRuntime,
  type EncodedError,
  type RuntimeCall,
  type StepRecords,
} from "./remote-runtime";

export type { RuntimeCall };

export type DeliveryRequest = {
  /** `params` and `context` as encoded by `encodeExecutionEvent`. */
  event: Omit<WorkflowEvent, "params" | "context"> & {
    params: string;
    context: string;
  };
  lease: ExecutionLease;
  cancellation?: ExecutionCancellation;
  /** Milliseconds between the instance's calls to `DeliveryHost.ping`. */
  heartbeat: number;
  /** The execution's step records, when the object that claimed it sends them. */
  steps?: StepRecords;
};

/** What the object that claimed a delivery serves to the instance that runs it. */
export type DeliveryHost = {
  call: RuntimeCall;
  /** Rejects once the object that claimed the delivery has stopped. */
  ping(): Promise<void>;
};

/**
 * `outcome` reports a finished delivery. `error` reports an infrastructure
 * error, which is not an outcome.
 */
export type DeliveryReply =
  | { status: "outcome"; outcome: EncodedWorkflowRunOutcome }
  | ({ status: "error" } & EncodedError);

/** The reason an instance aborts with when `durableStopDelivery` stops its delivery. */
export const DELIVERY_STOPPED = "The delivery was stopped";

/** The methods the instance that runs a delivery serves to the object that claimed it. */
export interface DeliveryRunner {
  durableRunDelivery(request: DeliveryRequest, host: DeliveryHost): Promise<DeliveryReply>;
  /**
   * Aborts the instance if it is running a delivery, which rejects this call
   * and the delivery's. Resolves when the instance is idle.
   */
  durableStopDelivery(): Promise<void>;
}

/**
 * Runs one delivery in this isolate, against the runtime that `host.call`
 * reaches. The object that claimed the delivery stops it by aborting this
 * instance, so the delivery's signal never aborts here.
 *
 * That object renews the delivery's lease. If it stops, nothing renews the
 * lease and another delivery of the execution follows, so `onOrphaned` is
 * called at the first `host.ping` that rejects, to abort this instance before
 * then.
 */
export async function runDelivery(
  request: DeliveryRequest,
  host: DeliveryHost,
  options: {
    router: WorkflowRouter;
    logger: Logger;
    onOrphaned: () => void;
    /** The clock the delivery reads, such as for the end of a delay. @default Date.now */
    clock?: () => number;
  }
): Promise<DeliveryReply> {
  const { event, lease, cancellation } = request;
  const steps = request.steps && { executionId: event.executionId, records: request.steps };
  const runtime = new RemoteRuntime(host.call, { steps, clock: options.clock });
  const runner = new WorkflowRunner(runtime, options);
  const { signal } = new AbortController();
  const heartbeat = setInterval(() => {
    host.ping().catch(options.onOrphaned);
  }, request.heartbeat);
  try {
    const outcome = await runner.run(
      { ...event, ...decodeExecutionEvent(event) },
      { lease, signal, ...(cancellation ? { cancellation } : {}) }
    );
    return { status: "outcome", outcome: encodeWorkflowRunOutcome(outcome) };
  } catch (error) {
    return { status: "error", ...encodeError(error) };
  } finally {
    clearInterval(heartbeat);
  }
}

const messageOf = (error: unknown) => (error instanceof Error ? error.message : String(error));

// Cloudflare sets `retryable` on an error from a stub when the failure is
// transient, and `overloaded` when the object must not be called again at
// once, as after it exceeds the CPU limit.
function isRetryable(error: unknown) {
  const { retryable, overloaded } = (error ?? {}) as { retryable?: unknown; overloaded?: unknown };
  return retryable === true && overloaded !== true;
}

/**
 * The most step record text sent with a delivery. A delivery reads the
 * records beyond it from the runtime, one call each.
 */
const MAX_STEP_RECORD_CHARACTERS = 1_000_000;

function collectSteps(records: Iterable<[string, HeapRecord]>): StepRecords {
  const steps: StepRecords = {};
  let characters = 0;
  for (const [stepKey, record] of records) {
    characters += record.stepResponseJson.length;
    steps[stepKey] = characters <= MAX_STEP_RECORD_CHARACTERS ? record : null;
  }
  return steps;
}

/**
 * Returns a `deliver` that runs each delivery in one of a pool of instances.
 * `getRunner(slot)` returns a stub for the instance with that number, and a
 * slot runs one delivery at a time, so each delivery has an isolate to
 * itself. The pool grows to the most deliveries that have run at once.
 *
 * `deliver` must be called with the delivery's lease already held, as the
 * dispatcher calls it: the step records it sends are those of the lease
 * holder.
 *
 * When the delivery's signal aborts, `deliver` stops the instance, which
 * stops a step function that is awaiting, and rejects at once with an
 * `InfrastructureError`.
 *
 * When the call to the instance fails, `deliver` rejects in one of two ways:
 *
 * - With a `WorkerExitedError`, so the dispatcher leaves the lease to expire
 *   and counts an expired delivery. This is what follows a delivery that
 *   exceeds the CPU limit.
 * - With another error, so the dispatcher releases the lease and delivers the
 *   execution again after the redelivery delay. This is what follows an
 *   error that Cloudflare marks `retryable`, such as a lost connection, once
 *   the instance is known to have stopped.
 */
export function createIsolatedDeliver(params: {
  runtime: Runtime;
  getRunner: (slot: number) => DeliveryRunner;
  /**
   * Returns the record `HeapClient.readStep` would return for every step of
   * an execution that has one. With it, each delivery starts with the
   * execution's records and replays its finished steps without a call for
   * each.
   * @default the delivery reads each step from the runtime
   */
  readSteps?: (executionId: string) => Iterable<[string, HeapRecord]>;
  /** @default 30_000 */
  leaseDuration?: number;
}): WorkflowInvoker["deliver"] {
  const { getRunner } = params;
  const host: DeliveryHost = { call: serveRuntime(params.runtime), async ping() {} };
  // An instance whose object has stopped aborts well before the lease expires.
  const heartbeat = (params.leaseDuration ?? DEFAULT_LEASE_DURATION) / 3;
  const free: number[] = [];
  let size = 0;
  // The slots whose instance is known to be idle. An instance may still be
  // running a delivery that an earlier instance of this object started, or
  // one whose call failed, so any other slot is stopped before it is used.
  const idle = new Set<number>();

  const acquire = () => {
    if (free.length === 0) return size++;
    free.sort((a, b) => b - a);
    return free.pop()!;
  };
  // Resolves with whether the instance is known to be idle: the stop
  // resolved, or it rejected because it aborted the instance. After any other
  // rejection the instance may still be running a delivery.
  const stop = (slot: number) =>
    Promise.resolve()
      .then(() => getRunner(slot).durableStopDelivery())
      .then(
        () => true,
        (error) => messageOf(error).includes(DELIVERY_STOPPED)
      )
      .then((stopped) => {
        if (stopped) idle.add(slot);
        return stopped;
      });

  return async (event, { lease, signal, cancellation }) => {
    if (signal.aborted) throw new InfrastructureError(signal.reason);
    const request: DeliveryRequest = {
      event: { ...event, ...encodeExecutionEvent(event) },
      lease,
      ...(cancellation ? { cancellation } : {}),
      heartbeat,
    };
    const slot = acquire();
    if (!idle.has(slot)) await stop(slot);
    // Read after the wait above, with no wait between the read and the call.
    if (params.readSteps && !signal.aborted) {
      request.steps = collectSteps(params.readSteps(event.executionId));
    }
    if (signal.aborted) {
      free.push(slot);
      throw new InfrastructureError(signal.reason);
    }

    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (fn: () => unknown) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        fn();
      };
      // The slot is held until the instance has stopped, so that the next
      // delivery does not arrive at an instance that is about to abort.
      const onAbort = () =>
        settle(() => {
          idle.delete(slot);
          void stop(slot).then(() => free.push(slot));
          reject(new InfrastructureError(signal.reason));
        });
      signal.addEventListener("abort", onAbort, { once: true });
      Promise.resolve()
        .then(() => getRunner(slot).durableRunDelivery(request, host))
        .then(
          (reply) =>
            settle(() => {
              free.push(slot);
              if (reply.status === "error") {
                reject(decodeError(reply));
                return;
              }
              try {
                resolve(decodeWorkflowRunOutcome(reply.outcome));
              } catch (error) {
                reject(error);
              }
            }),
          (error) =>
            settle(async () => {
              idle.delete(slot);
              const message = `The Durable Object that ran the delivery stopped before replying: ${messageOf(error)}`;
              // A step function may still be running after a lost connection.
              // The lease is released only once it cannot be.
              const released = isRetryable(error) && (await stop(slot));
              free.push(slot);
              reject(released ? new Error(message) : new WorkerExitedError(message));
            })
        );
    });
  };
}
