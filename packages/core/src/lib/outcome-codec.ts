import { durableValueCodec, encodeThrownValue } from "../base/durable-value";
import { WORKFLOW_RESULT_STEP_KEY, WorkflowValueError } from "../base/errors";
import type { WorkflowRunOutcome } from "../base/execution";

/**
 * A `WorkflowRunOutcome` that can cross a process or isolate boundary. Results
 * and errors are durable value encodings, so they keep their class and shape.
 */
export type EncodedWorkflowRunOutcome =
  | Extract<WorkflowRunOutcome, { status: "suspended" }>
  | Extract<WorkflowRunOutcome, { status: "cancelled" }>
  | { status: "completed"; result: string }
  | { status: "failed"; error: string };

export function encodeWorkflowRunOutcome(
  outcome: WorkflowRunOutcome
): EncodedWorkflowRunOutcome {
  if (outcome.status === "suspended" || outcome.status === "cancelled") return outcome;
  if (outcome.status === "failed") {
    return { status: "failed", error: encodeThrownValue(outcome.error) };
  }
  const result = durableValueCodec.encode(outcome.result);
  if (result.status === "encoded") {
    return { status: "completed", result: result.canonical };
  }
  // As acknowledgement would record it.
  const error = new WorkflowValueError(
    "INVALID_STEP_VALUE",
    WORKFLOW_RESULT_STEP_KEY,
    `Workflow result is not a durable value: ${result.message}`
  );
  return { status: "failed", error: encodeThrownValue(error) };
}

/** Throws when `outcome` holds an encoding the codec rejects. */
export function decodeWorkflowRunOutcome(
  outcome: EncodedWorkflowRunOutcome
): WorkflowRunOutcome {
  switch (outcome.status) {
    case "suspended":
    case "cancelled":
      return outcome;
    case "completed":
      return { status: "completed", result: decodeOrThrow(outcome.result) };
    case "failed": {
      const error = decodeOrThrow(outcome.error);
      return {
        status: "failed",
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }
}

/**
 * Decodes an error encoded by `encodeThrownValue`. An encoding the codec
 * rejects decodes to an `Error` that says so.
 */
export function decodeThrownValue(encoded: string): Error {
  const decoding = durableValueCodec.decode(encoded);
  if (decoding.status === "invalid") {
    return new Error(`Received an invalid error encoding: ${decoding.message}`);
  }
  const error = decoding.value;
  return error instanceof Error ? error : new Error(String(error));
}

function decodeOrThrow(encoded: string) {
  const decoding = durableValueCodec.decode(encoded);
  if (decoding.status === "invalid") {
    throw new Error(`Received an invalid outcome encoding: ${decoding.message}`);
  }
  return decoding.value;
}
