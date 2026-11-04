export abstract class StepResponse {
  abstract type: string;
}

export class StepKey extends StepResponse {
  static type = "step-key";
  readonly type = "step-key";
  key: string;
  constructor(key: string) {
    super();
    this.key = key;
  }
}

export class StepCacheCheck extends StepResponse {
  static type = "cache-check";
  readonly type = "cache-check";
  /**
   * When the step's latest record is unfinished, the driver resumes the step
   * with that record wrapped in a `StepUnfinished`, instead of with null.
   */
  readonly unfinished: boolean;
  constructor(options?: { unfinished?: boolean }) {
    super();
    this.unfinished = options?.unfinished ?? false;
  }
}

/**
 * The latest unfinished record of a step, given to a step that asked for it
 * with `StepCacheCheck({ unfinished: true })`. The step runs again; the record
 * only carries state from the earlier attempt, such as a timed wait's deadline.
 */
export class StepUnfinished extends StepResponse {
  static type = "step-unfinished";
  readonly type = "step-unfinished";
  response: StepResponse;
  constructor(response: StepResponse) {
    super();
    this.response = response;
  }
}

export class StepInvalid extends StepResponse {
  static type = "step-invalid";
  readonly type = "step-invalid";
}

export type StepSchemaVerdict = "accepted" | "rejected";

export class StepResult extends StepResponse {
  static type = "step-result";
  readonly type = "step-result";
  result: any;
  /** Whether the step's schema accepted its value on the first run. */
  schema?: StepSchemaVerdict;
  constructor(result: any, schema?: StepSchemaVerdict) {
    super();
    this.result = result;
    if (schema !== undefined) this.schema = schema;
  }
}

export class WorkflowResult<T extends any> extends StepResponse {
  static type = "workflow-result";
  readonly type = "workflow-result";
  result: T;
  constructor(result: T) {
    super();
    this.result = result;
  }
}

export class WorkflowDelay extends StepResponse {
  static type = "workflow-delay";
  readonly type = "workflow-delay";
  /** The absolute time the step's pause ends, read from the runtime clock. */
  wakeAt: number;
  constructor(wakeAt: number) {
    super();
    this.wakeAt = wakeAt;
  }
}

/**
 * A store wait, recorded as an unfinished step after its waiter is registered.
 * A timed `when` sets `wakeAt` to its deadline, so the delivery is also due
 * then (RFC 007).
 */
export class StepStoreWait extends StepResponse {
  static type = "store-wait";
  readonly type = "store-wait";
  wakeAt?: number;
  constructor(wakeAt?: number) {
    super();
    if (wakeAt !== undefined) this.wakeAt = wakeAt;
  }
}

/**
 * An effect step whose outcome is uncertain. The driver records it as an
 * unfinished step after watching the effect, so the next delivery begins the
 * effect again; `wakeAt` is when that delivery is due.
 */
export class StepEffectWait extends StepResponse {
  static type = "effect-wait";
  readonly type = "effect-wait";
  wakeAt: number;
  constructor(wakeAt: number) {
    super();
    this.wakeAt = wakeAt;
  }
}

export class WorkflowRestart extends StepResponse {
  static type = "workflow-restart";
  readonly type = "workflow-restart";
  constructor() {
    super();
  }
}

export class StepError extends StepResponse {
  static type = "step-error";
  readonly type = "step-error";
  err: any;
  maxAttempts: number;
  retryInterval: number;
  backOffStrategy: "linear" | "decay";
  constructor(
    err: any,
    opts?: {
      maxAttempts: number;
      retryInterval?: number;
      backoffStrategy?: "linear" | "decay";
    }
  ) {
    super();
    this.err = err;
    this.maxAttempts = opts?.maxAttempts ? opts.maxAttempts : 1;
    this.retryInterval = opts?.retryInterval ? opts.retryInterval : 60000;
    this.backOffStrategy = opts?.backoffStrategy
      ? opts.backoffStrategy
      : "decay";
  }
}

export class StepDelay extends StepResponse {
  static type = "step-delay";
  readonly type = "step-delay";
  /** The absolute time the step's pause ends, read from the runtime clock. */
  wakeAt: number;
  constructor(wakeAt: number) {
    super();
    this.wakeAt = wakeAt;
  }
}
