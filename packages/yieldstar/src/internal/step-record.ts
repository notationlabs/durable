import {
  DurableValueError,
  StepCacheCheck,
  StepDelay,
  StepEffectWait,
  StepError,
  StepInvalid,
  StepJoinWait,
  StepResult,
  StepResponse,
  StepStoreWait,
  WorkflowResult,
  WorkflowValueError,
  decodeParsedValue,
  durableValueCodec,
  encodeThrownValue,
} from "@yieldstar/core";

/** A `StepCacheCheck` that resumes with the step's unfinished record as a `StepUnfinished`, not null. */
export class UnfinishedCacheCheck extends StepCacheCheck {}

/** The latest unfinished record of a step that yielded an `UnfinishedCacheCheck`. */
export class StepUnfinished extends StepResponse {
  static type = "step-unfinished";
  readonly type = "step-unfinished";
  readonly response: StepResponse;
  constructor(response: StepResponse) {
    super();
    this.response = response;
  }
}

/**
 * A cancellation delivered at this step. It is a separate type from
 * `StepError` so that a `WorkflowCancelledError` thrown by a step function
 * stays a step error.
 */
export class StepCancelled extends StepResponse {
  static type = "step-cancelled";
  readonly type = "step-cancelled";
  readonly err: unknown;
  constructor(err: unknown) {
    super();
    this.err = err;
  }
}

/**
 * Encodes a step response for its step record, and returns the response that
 * workflow code must see: the record decoded again, so that a first run sees
 * exactly what replay reads. A result the codec rejects is recorded as a step
 * error with `WorkflowValueError` (`INVALID_STEP_VALUE`).
 */
export function encodeStepRecord(
  response: StepResponse,
  stepKey: string
): { json: string; response: StepResponse } {
  const json = encodeResponse(response, stepKey);
  return { json, response: decodeStepRecord(json) };
}

function encodeResponse(response: StepResponse, stepKey: string): string {
  if (response instanceof StepResult || response instanceof WorkflowResult) {
    const result = durableValueCodec.encode(response.result);
    if (result.status === "invalid") {
      const label =
        response instanceof WorkflowResult ? "Workflow result" : `Step "${stepKey}" result`;
      return encodeResponse(
        new StepError(
          new WorkflowValueError(
            "INVALID_STEP_VALUE",
            stepKey,
            `${label} is not a durable value: ${result.message}`
          )
        ),
        stepKey
      );
    }
    const schema =
      response instanceof StepResult && response.schema !== undefined
        ? `,"schema":${JSON.stringify(response.schema)}`
        : "";
    return `{"result":${result.canonical}${schema},"type":${JSON.stringify(response.type)}}`;
  }
  if (response instanceof StepError) {
    return (
      `{"backOffStrategy":${JSON.stringify(response.backOffStrategy)}` +
      `,"err":${encodeThrownValue(response.err)}` +
      `,"maxAttempts":${JSON.stringify(response.maxAttempts)}` +
      `,"retryInterval":${JSON.stringify(response.retryInterval)}` +
      `,"type":"step-error"}`
    );
  }
  if (response instanceof StepCancelled) {
    return `{"err":${encodeThrownValue(response.err)},"type":"step-cancelled"}`;
  }
  if (response instanceof StepDelay) {
    return `{"type":"step-delay","wakeAt":${JSON.stringify(response.wakeAt)}}`;
  }
  if (response instanceof StepEffectWait) {
    return `{"type":"effect-wait","wakeAt":${JSON.stringify(response.wakeAt)}}`;
  }
  // JSON.stringify drops an undefined wakeAt.
  if (response instanceof StepStoreWait || response instanceof StepJoinWait) {
    return JSON.stringify({ type: response.type, wakeAt: response.wakeAt });
  }
  if (response instanceof StepInvalid) {
    return `{"type":"step-invalid"}`;
  }
  throw new Error(`Cannot record a ${response.type} step response`);
}

export function decodeStepRecord(json: string): StepResponse {
  const record = JSON.parse(json);
  try {
    switch (record.type) {
      case "step-result":
        return new StepResult(
          decodeParsedValue(record.result, "result"),
          record.schema === "accepted" || record.schema === "rejected"
            ? record.schema
            : undefined
        );
      case "workflow-result":
        return new WorkflowResult(decodeParsedValue(record.result, "result"));
      case "step-error":
        return new StepError(decodeParsedValue(record.err, "err"), {
          maxAttempts: record.maxAttempts,
          retryInterval: record.retryInterval,
          backoffStrategy: record.backOffStrategy,
        });
      case "step-cancelled":
        return new StepCancelled(decodeParsedValue(record.err, "err"));
      case "step-delay":
        return new StepDelay(record.wakeAt);
      case "effect-wait":
        return new StepEffectWait(record.wakeAt);
      case "store-wait":
      case "join-wait": {
        const wakeAt = typeof record.wakeAt === "number" ? record.wakeAt : undefined;
        return record.type === "store-wait"
          ? new StepStoreWait(wakeAt)
          : new StepJoinWait(wakeAt);
      }
      case "step-invalid":
        return new StepInvalid();
    }
  } catch (err) {
    if (err instanceof DurableValueError) {
      throw new Error(`Invalid step record: ${err.message}`);
    }
    throw err;
  }
  throw new Error(`Unknown step record type: ${record.type}`);
}
