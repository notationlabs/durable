import type { Logger } from "pino";
import {
  InfrastructureError,
  WorkerExitedError,
  WorkflowRunner,
  decodeExecutionEvent,
  decodeThrownValue,
  decodeWorkflowRunOutcome,
  encodeExecutionEvent,
  encodeThrownValue,
  encodeWorkflowRunOutcome,
  type EncodedWorkflowRunOutcome,
  type ExecutionCancellation,
  type ExecutionLease,
  type Runtime,
  type WorkflowEvent,
  type WorkflowInvoker,
  type WorkflowRouter,
} from "@durable/core";
import { RemoteRuntime, serveRuntime, type RuntimeCall } from "./remote-runtime";

export type DeliveryRequest = {
  /** `params` and `context` as encoded by `encodeExecutionEvent`. */
  event: Omit<WorkflowEvent, "params" | "context"> & {
    params: string;
    context: string;
  };
  lease: ExecutionLease;
  cancellation?: ExecutionCancellation;
};

/**
 * `outcome` reports a finished delivery. `error` reports an infrastructure
 * error, which is not an outcome.
 */
export type DeliveryReply =
  | { status: "outcome"; outcome: EncodedWorkflowRunOutcome }
  | { status: "error"; error: string };

/** The methods the instance that runs a delivery serves to the object that claimed it. */
export interface DeliveryRunner {
  durableRunDelivery(request: DeliveryRequest, call: RuntimeCall): Promise<DeliveryReply>;
  /**
   * Aborts the instance if it is running a delivery, which rejects this call
   * and the delivery's. Resolves when the instance is idle.
   */
  durableStopDelivery(): Promise<void>;
}

/**
 * Runs one delivery in this isolate, against the runtime that `call` reaches.
 * The instance that claimed the delivery stops it by aborting this instance,
 * so the delivery's signal never aborts here.
 */
export async function runDelivery(
  request: DeliveryRequest,
  call: RuntimeCall,
  options: { router: WorkflowRouter; logger: Logger }
): Promise<DeliveryReply> {
  const runner = new WorkflowRunner(new RemoteRuntime(call), options);
  const { signal } = new AbortController();
  const { event, lease, cancellation } = request;
  try {
    const outcome = await runner.run(
      { ...event, ...decodeExecutionEvent(event) },
      { lease, signal, ...(cancellation ? { cancellation } : {}) }
    );
    return { status: "outcome", outcome: encodeWorkflowRunOutcome(outcome) };
  } catch (error) {
    return { status: "error", error: encodeThrownValue(error) };
  }
}

/**
 * Returns a `deliver` that runs each delivery in one of a pool of instances.
 * `getRunner(slot)` returns a stub for the instance with that number, and a
 * slot runs one delivery at a time, so each delivery has an isolate to
 * itself. The pool grows to the most deliveries that have run at once.
 *
 * When the delivery's signal aborts, `deliver` stops the instance, which
 * stops a step function that is awaiting, and rejects at once with an
 * `InfrastructureError`. When the instance stops before replying, such as
 * after it exceeds the CPU limit, `deliver` rejects with a
 * `WorkerExitedError`, so the dispatcher leaves the lease to expire.
 */
export function createIsolatedDeliver(params: {
  runtime: Runtime;
  getRunner: (slot: number) => DeliveryRunner;
}): WorkflowInvoker["deliver"] {
  const { getRunner } = params;
  const call = serveRuntime(params.runtime);
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
  // The stop rejects when it aborts the instance, and the instance is idle
  // either way once it settles.
  const stop = (slot: number) =>
    Promise.resolve()
      .then(() => getRunner(slot).durableStopDelivery())
      .catch(() => {})
      .then(() => {
        idle.add(slot);
      });

  return async (event, { lease, signal, cancellation }) => {
    if (signal.aborted) throw new InfrastructureError(signal.reason);
    const request: DeliveryRequest = {
      event: { ...event, ...encodeExecutionEvent(event) },
      lease,
      ...(cancellation ? { cancellation } : {}),
    };
    const slot = acquire();
    if (!idle.has(slot)) await stop(slot);
    if (signal.aborted) {
      free.push(slot);
      throw new InfrastructureError(signal.reason);
    }

    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (fn: () => void) => {
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
        .then(() => getRunner(slot).durableRunDelivery(request, call))
        .then(
          (reply) =>
            settle(() => {
              free.push(slot);
              if (reply.status === "error") {
                reject(decodeThrownValue(reply.error));
                return;
              }
              try {
                resolve(decodeWorkflowRunOutcome(reply.outcome));
              } catch (error) {
                reject(error);
              }
            }),
          (error) =>
            settle(() => {
              idle.delete(slot);
              free.push(slot);
              reject(
                new WorkerExitedError(
                  `The Durable Object that ran the delivery stopped before replying: ${
                    error instanceof Error ? error.message : String(error)
                  }`
                )
              );
            })
        );
    });
  };
}
