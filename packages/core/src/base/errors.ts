import {
  EffectClientInvariantError,
  EffectDeliveryError,
  WorkflowEffectError,
} from "./effect";
import { WorkflowCancelledError, WorkflowStartError } from "./execution";
import { WorkflowJoinError } from "./join";

export type WorkflowValueErrorCode = "INVALID_STEP_VALUE" | "STEP_SCHEMA_INVALID";

/** The `stepKey` of a `WorkflowValueError` raised by an invalid workflow result. */
export const WORKFLOW_RESULT_STEP_KEY = "$$workflow-result$$";

/**
 * Thrown to workflow code when a step produces a value the durable value codec
 * rejects, or a step's schema rejects its decoded value. It is recorded as the
 * step's error, so replay throws it again.
 */
export class WorkflowValueError extends Error {
  readonly code: WorkflowValueErrorCode;
  readonly stepKey: string;

  constructor(code: WorkflowValueErrorCode, stepKey: string, message: string) {
    super(message);
    this.name = "WorkflowValueError";
    this.code = code;
    this.stepKey = stepKey;
  }
}

/**
 * Thrown by `step.race` when every branch fails, and by an empty race.
 * `errors` holds each branch's error in branch order. It is a non-enumerable
 * array, as on `AggregateError`, so a recorded outcome keeps it.
 */
export class WorkflowRaceError extends Error {
  readonly code: "ALL_BRANCHES_FAILED";
  declare readonly errors: unknown[];

  constructor(stepKey: string, errors: unknown[]) {
    super(
      errors.length === 0
        ? `Race "${stepKey}" has no branches`
        : `Every branch of race "${stepKey}" failed`
    );
    this.name = "WorkflowRaceError";
    this.code = "ALL_BRANCHES_FAILED";
    Object.defineProperty(this, "errors", {
      value: errors,
      enumerable: false,
      writable: true,
      configurable: true,
    });
  }
}

export type ReplayErrorCode = "STEP_KEY_REQUIRED" | "PATCH_IN_COMBINATOR";

/**
 * Thrown to workflow code when a step cannot be recorded safely for replay:
 * a keyless step without a record under `requireStepKeys`
 * (`STEP_KEY_REQUIRED`), or `step.patched` inside a combinator branch
 * (`PATCH_IN_COMBINATOR`). It is recorded as the step's error, so replay
 * throws it again.
 */
export class WorkflowReplayError extends Error {
  readonly code: ReplayErrorCode;
  readonly stepKey?: string;

  constructor(code: ReplayErrorCode, message: string, stepKey?: string) {
    super(message);
    this.name = "WorkflowReplayError";
    this.code = code;
    if (stepKey !== undefined) this.stepKey = stepKey;
  }
}

export type ErrorClass = abstract new (...args: any[]) => Error;

const errorClasses = new Map<string, ErrorClass>();

/**
 * Registers the class that decoding revives errors named `name` as, replacing
 * any class registered under that name. Registration is per process: a worker
 * that has not registered a class revives its errors as plain `Error`s with
 * the same name, message, code, cause and fields.
 */
export function registerErrorClass(name: string, errorClass: ErrorClass): void {
  if (typeof name !== "string" || name === "") {
    throw new TypeError("Error class name must be a non-empty string");
  }
  if (
    typeof errorClass !== "function" ||
    !(errorClass.prototype instanceof Error || errorClass === Error)
  ) {
    throw new TypeError(`Error class "${name}" must extend Error`);
  }
  errorClasses.set(name, errorClass);
}

export function registeredErrorClass(name: string): ErrorClass | undefined {
  return errorClasses.get(name);
}

// Names are spelled out so a minifying bundler cannot change them.
const defaults: [string, ErrorClass][] = [
  ["TypeError", TypeError],
  ["RangeError", RangeError],
  ["SyntaxError", SyntaxError],
  ["ReferenceError", ReferenceError],
  ["EvalError", EvalError],
  ["URIError", URIError],
  ["AggregateError", AggregateError],
  ["WorkflowStartError", WorkflowStartError],
  ["WorkflowJoinError", WorkflowJoinError],
  ["WorkflowCancelledError", WorkflowCancelledError],
  ["WorkflowValueError", WorkflowValueError],
  ["WorkflowRaceError", WorkflowRaceError],
  ["WorkflowReplayError", WorkflowReplayError],
  ["WorkflowEffectError", WorkflowEffectError],
  ["EffectClientInvariantError", EffectClientInvariantError],
  ["EffectDeliveryError", EffectDeliveryError],
];
for (const [name, errorClass] of defaults) registerErrorClass(name, errorClass);
