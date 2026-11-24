import type { Logger } from "pino";
import type {
  EncodedWorkflowRunOutcome,
  EventContext,
  ExecutionCancellation,
  ExecutionLease,
  WorkflowEvent,
  WorkflowRunner,
} from "@durable/core";
import {
  decodeExecutionEvent,
  encodeThrownValue,
  encodeWorkflowRunOutcome,
} from "@durable/core";

let context: EventContext;

export const getContext = () => context;

/**
 * Messages a worker sends its parent. `outcome` reports a finished delivery;
 * `error` reports an infrastructure error, which is not an outcome. Errors and
 * results are durable value encodings, so they keep their class and shape.
 */
export type WorkerMessage =
  | { status: "ready" }
  | { status: "outcome"; outcome: SerializedOutcome }
  | { status: "error"; error: string };

/**
 * The message a parent sends its worker to start one delivery under its
 * lease. The parent stops a delivery whose lease is lost by killing the
 * worker, so no abort message exists.
 */
export type WorkerDelivery = {
  type: "deliver";
  /** `params` and `context` as encoded by `encodeExecutionEvent`. */
  event: Omit<WorkflowEvent, "params" | "context"> & {
    params: string;
    context: string;
  };
  lease: ExecutionLease;
  cancellation?: ExecutionCancellation;
};

export type WorkerRequest = WorkerDelivery;

export type SerializedOutcome = EncodedWorkflowRunOutcome;

/** The exit code of a worker whose runtime process has gone. */
export const WORKER_ORPHANED_EXIT_CODE = 75;

export function createWorkflowWorker(
  workflowRunner: WorkflowRunner<any>,
  logger: Logger
) {
  const send = (message: WorkerMessage) => process.send!(message);
  return {
    listen() {
      // The parent process holds this delivery's lease and renews it. Once
      // the IPC channel closes, the parent has gone (or let the worker go),
      // nothing renews the lease, and another runtime may claim it. Exit at
      // once so this worker makes no further fenced writes. With a synchronous
      // backend such as SQLite, the exit falls between writes, never inside one.
      process.on("disconnect", () => {
        logger.error(
          "Worker lost its runtime process; exiting without further writes"
        );
        process.exit(WORKER_ORPHANED_EXIT_CODE);
      });
      // The parent kills the worker when the lease is lost, so the
      // delivery's signal never aborts inside the worker.
      const { signal } = new AbortController();
      process.on("message", async (message: WorkerRequest) => {
        const { event, lease, cancellation } = message;
        try {
          const workflowEvent: WorkflowEvent = {
            ...event,
            ...decodeExecutionEvent(event),
          };
          context = workflowEvent.context;
          const outcome = await workflowRunner.run(workflowEvent, {
            lease,
            signal,
            ...(cancellation ? { cancellation } : {}),
            logger,
          });
          send({ status: "outcome", outcome: encodeWorkflowRunOutcome(outcome) });
        } catch (error: any) {
          send({ status: "error", error: encodeThrownValue(error) });
        }
      });
      send({ status: "ready" });
    },
  };
}
