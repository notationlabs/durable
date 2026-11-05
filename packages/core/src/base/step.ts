export abstract class StepResponse {
  abstract type: string;
}

export class StepKey extends StepResponse {
  static type = "step-key";
  readonly type = "step-key";
  key: string;
  /**
   * The kind of step, such as `run` or `delay`. It is stored with the step's
   * records, and replay that requests the key as another kind diverges.
   */
  kind?: string;
  /** Whether the key was derived from the call site because the step has none. */
  keyless?: boolean;
  constructor(key: string, options?: { kind?: string; keyless?: boolean }) {
    super();
    this.key = key;
    if (options?.kind !== undefined) this.kind = options.kind;
    if (options?.keyless) this.keyless = true;
  }
}

export class StepCacheCheck extends StepResponse {
  static type = "cache-check";
  readonly type = "cache-check";
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
 * A store wait, recorded as an unfinished step.
 * @param wakeAt the deadline of a timed `when`. The execution is also due then.
 */
export class StepStoreWait extends StepResponse {
  static type = "store-wait";
  readonly type = "store-wait";
  constructor(readonly wakeAt?: number) {
    super();
  }
}

/**
 * An unresolved `step.join` or `step.call`, recorded as an unfinished step.
 * For a timed join, `wakeAt` is the deadline, and the execution is also due
 * then.
 */
export class StepJoinWait extends StepResponse {
  static type = "join-wait";
  readonly type = "join-wait";
  constructor(readonly wakeAt?: number) {
    super();
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
