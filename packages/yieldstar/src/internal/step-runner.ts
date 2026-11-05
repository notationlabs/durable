import {
  assertSynchronousClaim,
  assertSynchronousUpdater,
  cloneStoreState,
  defineStore,
  type Draft,
  type DurableObject,
  type DurableStoreSchema,
  type DurableValue,
  type IfDurable,
  type ExecutionStartRequest,
  type ExecutionStartResult,
  type EventParamsOf,
  type JoinResult,
  type StandardSchemaV1,
  StepResponse,
  StepKey,
  StepError,
  StepResult,
  StepDelay,
  StepCacheCheck,
  StepStoreWait,
  type StoreDefinition,
  type StoreDeleteFromResult,
  type StoreKey,
  type StoreSelector,
  type StoreSnapshot,
  type StoreState,
  type StoreTakeResult,
  type StoreUpdateFromResult,
  type StoreUpdateResult,
  type StoreWaiter,
  trackStoreSelector,
  type Durable,
  type StepSchemaVerdict,
  durableValueCodec,
  StepEffectWait,
  StepJoinWait,
  validateEffectRequest,
  WorkflowEffectError,
  WorkflowJoinError,
  WorkflowStartError,
  WorkflowValueError,
  type WorkflowGenerator,
  type WorkflowGeneratorReturnType,
} from "@yieldstar/core";
import { RetryableError } from "../exports/errors";
import { getCallSiteHash } from "./utils";
import { StepUnfinished, UnfinishedCacheCheck } from "./step-record";
import {
  StepRuntimeCall,
  StepStoreWaitRequest,
  infrastructure,
  workflowCallback,
  type RuntimeCallResult,
} from "./runtime-call";
import type { WorkflowEvent } from "@yieldstar/core";
import {
  BranchesStepKey,
  StepBranches,
  type AllOptions,
  type Branch,
  type BranchResults,
  type RaceResult,
  type SettledResults,
} from "./branches";
import {
  StepEffectCall,
  type EffectCallOutcome,
  type EffectLookup,
} from "./effect-call";

export type { EffectLookup };

/**
 * @description A library of step generators, each of which:
 * @yields a StepResponse to workflow consumers
 * @returns the result of executing a user-defined function, or
 * @returns the cached result from a previous run, or
 * @throws any error caught when running the user-defined function,
 * once retries have been exhausted, or if there are no retry semantics
 */
export { defineStore };

export type WorkflowStore<T> = {
  readonly definition: StoreDefinition<any>;
  readonly id: string;
  readonly key: StoreKey;
  get(key?: string): AsyncGenerator<StepResponse, StoreSnapshot<T>>;
  select<R>(
    key: string,
    selector: StoreSelector<T, R> & DurableSelector<T, R>
  ): AsyncGenerator<StepResponse, R>;
  update(
    key: string,
    updater: (draft: Draft<T>) => void | T
  ): AsyncGenerator<StepResponse, StoreUpdateResult<T>>;
  updateFrom(
    key: string,
    snapshot: StoreSnapshot<T>,
    updater: (draft: Draft<T>) => void | T
  ): AsyncGenerator<StepResponse, StoreUpdateFromResult<T>>;
  deleteFrom(
    key: string,
    snapshot: StoreSnapshot<T>
  ): AsyncGenerator<StepResponse, StoreDeleteFromResult>;
  when<R>(
    selector: StoreSelector<T, R | undefined | null | false> &
      DurableMatchSelector<T, R>
  ): AsyncGenerator<StepResponse, NonNullable<R>>;
  when<R>(
    key: string,
    selector: StoreSelector<T, R | undefined | null | false> &
      DurableMatchSelector<T, R>
  ): AsyncGenerator<StepResponse, NonNullable<R>>;
  /**
   * Waits until the selector matches or the deadline passes, and returns the
   * first result recorded. The selector is checked first, so a match returns
   * `matched` even after the deadline.
   * @param options.deadline absolute time in milliseconds, compared with the
   * runtime clock. The first evaluation records it and later evaluations
   * ignore the argument.
   */
  when<R>(
    key: string,
    selector: StoreSelector<T, R | undefined | null | false> &
      DurableMatchSelector<T, R>,
    options: { deadline: number }
  ): AsyncGenerator<StepResponse, TimedWhenResult<R>>;
  take<R>(
    key: string,
    selector: StoreSelector<T, R | undefined | null | false> &
      DurableMatchSelector<T, R>,
    claim: (draft: Draft<T>, selected: NonNullable<R>) => void
  ): AsyncGenerator<StepResponse, NonNullable<R>>;
};

export type TimedWhenResult<R> =
  | { status: "matched"; value: NonNullable<R> }
  | { status: "timed-out" };

export type TimedJoinResult<R> =
  | { status: "completed"; result: R }
  | { status: "timed-out" };

// `params` is required when the child's params type does not admit undefined.
type CallTarget<W extends WorkflowGenerator<any, any, any>> = {
  workflowId: string;
} & (undefined extends EventParamsOf<W>
  ? { params?: EventParamsOf<W> }
  : { params: EventParamsOf<W> });

// The child of a `step.call` without a type argument, which cannot be
// inferred: it takes no params, and its result is a `DurableValue`, as for an
// untyped `step.join`.
type UntypedChild = WorkflowGenerator<void, DurableValue, any>;

/** A store selector whose result, which the step records, is durable. */
type DurableSelector<T, R> = IfDurable<R, StoreSelector<T, Durable<R>>>;

/**
 * A `when` or `take` selector whose match is durable. `false` is excluded
 * before the check so that `R` is inferred as it would be without it.
 */
type DurableMatchSelector<T, R> = IfDurable<
  Exclude<R, false>,
  StoreSelector<T, Durable<Exclude<R, false>> | undefined | null | false>
>;

export type StepRunner = ReturnType<typeof createStepRunner>;

export function createStepRunner(params: {
  event: WorkflowEvent;
  clock: () => number;
}) {
  const { event, clock } = params;

  /**
   * Returns a handle to the store `definition.name` + `id`, where `id`
   * defaults to this execution's ID. Obtaining a handle reads, creates and
   * records nothing, so any caller may obtain the same handle again. Each
   * operation on a missing store first creates it from `definition.initial`.
   */
  function store<Schema extends StandardSchemaV1>(
    definition: StoreDefinition<Schema> & {
      schema: DurableStoreSchema<Schema>;
    },
    params?: { id?: string }
  ): WorkflowStore<StoreState<Schema>> {
    return createWorkflowStore({
      definition,
      id: params?.id ?? event.executionId,
      event,
      clock,
    });
  }

  /**
   * Starts another workflow as a durable step and returns its execution ID.
   * The start is identified by this execution's ID and `stepKey`, so every
   * replay of the step names the same target. A rejected start throws a
   * `WorkflowStartError`.
   * @param options.detached the target receives no cancellation propagated
   * from this execution. A repeat that changes it is a different request.
   */
  function start<Params extends Record<string, any>>(
    stepKey: string,
    target: { workflowId: string; params?: Params & Durable<Params> },
    options?: StartOptions
  ): AsyncGenerator<
    StepResponse,
    { executionId: string },
    StepResult | StepError
  > {
    if (stepKey === "") {
      throw new Error("step.start requires a non-empty step key");
    }
    const request: ExecutionStartRequest = {
      requestId: { executionId: event.executionId, stepKey },
      workflowId: target.workflowId,
    };
    // A missing params and an empty object are different requests.
    if (target.params !== undefined) {
      request.params = target.params as DurableObject;
    }
    const detached = startDetached("step.start", options);
    if (detached !== undefined) request.detached = detached;
    return startStep(stepKey, request);
  }

  /**
   * Runs an external operation identified by `descriptor.effectId`, at most
   * once in effect: after an uncertain attempt, `lookup` decides whether the
   * operation already happened before `perform` runs again.
   */
  function effect<T, Input extends Record<string, any> = DurableObject>(
    stepKey: string,
    descriptor: EffectDescriptor<T, Input> & DurableEffectResult<T>
  ): AsyncGenerator<StepResponse, T, StepResult | StepError> {
    if (typeof stepKey !== "string" || stepKey === "") {
      throw new Error("step.effect requires a non-empty step key");
    }
    return effectStep(stepKey, descriptor) as AsyncGenerator<
      StepResponse,
      T,
      StepResult | StepError
    >;
  }

  /**
   * Waits until the execution `executionId` is terminal. Returns its result,
   * or throws a `WorkflowJoinError`: `JOINED_EXECUTION_FAILED` with its
   * recorded error as `cause`, or `JOINED_EXECUTION_CANCELLED` with its
   * `reason` and `source`. The outcome is recorded under `stepKey`,
   * so replay returns or throws it without reading the target again.
   */
  function join<R = DurableValue>(
    stepKey: string,
    executionId: string
  ): AsyncGenerator<StepResponse, R, StepResult | StepError>;
  /**
   * Waits until the execution is terminal or the deadline passes, and returns
   * the first result recorded. The target is read first, so a terminal target
   * wins even after the deadline. A failed or cancelled target still throws.
   * @param options.deadline absolute time in milliseconds, compared with the
   * runtime clock. The first evaluation records it and later evaluations
   * ignore the argument.
   */
  function join<R = DurableValue>(
    stepKey: string,
    executionId: string,
    options: { deadline: number }
  ): AsyncGenerator<StepResponse, TimedJoinResult<R>, StepResult | StepError>;
  function join(
    stepKey: string,
    executionId: string,
    options?: { deadline: number }
  ): AsyncGenerator<StepResponse, unknown, StepResult | StepError> {
    assertNonEmpty("step.join", "step key", stepKey);
    assertNonEmpty("step.join", "execution ID", executionId);
    return joinStep({
      stepKey,
      event,
      clock,
      deadline: joinDeadline("step.join", options),
      target: { executionId },
    });
  }

  /**
   * Starts the workflow `target.workflowId` and waits until it is terminal,
   * as `step.start` followed by `step.join`, recorded as one step under
   * `stepKey`. Until the step is recorded, each replay repeats the start,
   * which returns the same child, and joins it. The type parameter `W` types
   * `params` and the result. It is not checked against `workflowId`.
   * `options.detached` is as in `step.start`.
   */
  function call<W extends WorkflowGenerator<any, any, any> = UntypedChild>(
    stepKey: string,
    target: CallTarget<W>,
    options?: StartOptions
  ): AsyncGenerator<StepResponse, WorkflowGeneratorReturnType<W>, StepResult | StepError>;
  /**
   * `step.call` with the deadline rules of the timed `step.join`.
   * @param options.deadline absolute time in milliseconds. The first
   * evaluation records it and later evaluations ignore the argument.
   */
  function call<W extends WorkflowGenerator<any, any, any> = UntypedChild>(
    stepKey: string,
    target: CallTarget<W>,
    options: StartOptions & { deadline: number }
  ): AsyncGenerator<
    StepResponse,
    TimedJoinResult<WorkflowGeneratorReturnType<W>>,
    StepResult | StepError
  >;
  function call(
    stepKey: string,
    target: { workflowId: string; params?: unknown },
    options?: StartOptions & { deadline?: number }
  ): AsyncGenerator<StepResponse, unknown, StepResult | StepError> {
    assertNonEmpty("step.call", "step key", stepKey);
    const request: ExecutionStartRequest = {
      requestId: { executionId: event.executionId, stepKey },
      workflowId: target.workflowId,
    };
    // A missing params and an empty object are different requests.
    if (target.params !== undefined) {
      request.params = target.params as DurableObject;
    }
    const detached = startDetached("step.call", options);
    if (detached !== undefined) request.detached = detached;
    return joinStep({
      stepKey,
      event,
      clock,
      // `detached` alone does not make the call timed.
      deadline: joinDeadline(
        "step.call",
        options && "deadline" in options ? { deadline: options.deadline! } : undefined
      ),
      target: { start: request },
    });
  }

  /**
   * Drives `branches` concurrently and returns their results in branch order.
   * The outcome is recorded under `stepKey`, so replay returns or throws it
   * again. In `fail-fast` mode the first branch failure is thrown and the
   * other branches start no new steps.
   */
  function all<const B extends readonly Branch<unknown>[]>(
    stepKey: string,
    branches: B,
    options?: AllOptions & { mode?: "fail-fast" }
  ): AsyncGenerator<StepResponse, BranchResults<B>, any>;
  /** In `settle` mode, every branch runs to completion or failure. */
  function all<const B extends readonly Branch<unknown>[]>(
    stepKey: string,
    branches: B,
    options: AllOptions & { mode: "settle" }
  ): AsyncGenerator<StepResponse, SettledResults<B>, any>;
  function all(
    stepKey: string,
    branches: readonly Branch<unknown>[],
    options?: AllOptions
  ): AsyncGenerator<StepResponse, unknown, any> {
    assertNonEmpty("step.all", "step key", stepKey);
    assertBranches("step.all", branches);
    const mode = options?.mode ?? "fail-fast";
    if (mode !== "fail-fast" && mode !== "settle") {
      throw new TypeError(`step.all mode must be "fail-fast" or "settle"`);
    }
    const concurrency = options?.concurrency ?? Infinity;
    if (
      concurrency !== Infinity &&
      (!Number.isInteger(concurrency) || concurrency < 1)
    ) {
      throw new TypeError("step.all concurrency must be a positive integer");
    }
    return branchesStep(
      stepKey,
      new StepBranches({ combinator: "all", mode, concurrency, branches: [...branches] })
    );
  }

  /**
   * Drives `branches` concurrently and returns the index and value of the
   * first branch to succeed. A failed branch does not end the race; if every
   * branch fails, throws `WorkflowRaceError`. The winner is recorded under
   * `stepKey`, so replay returns the same winner. The other branches are
   * abandoned: they replay their recorded steps and start no new steps.
   */
  function race<const B extends readonly Branch<unknown>[]>(
    stepKey: string,
    branches: B
  ): AsyncGenerator<StepResponse, RaceResult<B>, any> {
    assertNonEmpty("step.race", "step key", stepKey);
    assertBranches("step.race", branches);
    return branchesStep(
      stepKey,
      new StepBranches({
        combinator: "race",
        mode: "fail-fast",
        concurrency: Infinity,
        branches: [...branches],
      })
    ) as AsyncGenerator<StepResponse, RaceResult<B>, any>;
  }

  return { run, delay: createDelay(clock), poll, store, start, effect, join, call, all, race };
}

function assertBranches(method: string, branches: unknown) {
  if (!Array.isArray(branches)) {
    throw new TypeError(`${method} requires an array of branches`);
  }
  for (const branch of branches) {
    if (
      typeof branch !== "object" ||
      branch === null ||
      typeof (branch as AsyncGenerator).next !== "function" ||
      typeof (branch as AsyncGenerator).throw !== "function" ||
      !(Symbol.asyncIterator in branch)
    ) {
      throw new TypeError(
        `${method} branches must be step generators, such as step.run(...) or the result of calling an async function*`
      );
    }
  }
}

async function* branchesStep(
  stepKey: string,
  request: StepBranches
): AsyncGenerator<StepResponse, any, any> {
  yield new BranchesStepKey(stepKey);
  yield new StepCacheCheck();
  const outcome: StepResult = yield request;
  return outcome.result;
}

// As in step.start and a timed store.when: a value of the wrong type is a
// TypeError, and an empty string is an Error.
function assertNonEmpty(method: string, name: string, value: unknown) {
  if (typeof value !== "string") {
    throw new TypeError(`${method} requires the ${name} to be a string`);
  }
  if (value === "") {
    throw new Error(`${method} requires a non-empty ${name}`);
  }
}

export type StartOptions = {
  /**
   * Records the target's parent link as detached, so the target receives no
   * cancellation propagated from this execution.
   * @default false
   */
  detached?: boolean;
};

function startDetached(method: string, options: StartOptions | undefined) {
  if (options === undefined) return undefined;
  const { detached } = options;
  if (detached !== undefined && typeof detached !== "boolean") {
    throw new TypeError(`${method} detached must be a boolean`);
  }
  return detached;
}

function joinDeadline(method: string, options: { deadline: number } | undefined) {
  if (options === undefined) return undefined;
  const { deadline } = options;
  if (typeof deadline !== "number" || !Number.isFinite(deadline)) {
    throw new TypeError(`${method} deadline must be a finite number`);
  }
  return deadline;
}

/**
 * The step behind `step.join` and `step.call`. A `start` target is started
 * first, on every evaluation until the step is recorded. With a deadline,
 * returns a `TimedJoinResult`; without one, returns the target's result.
 */
async function* joinStep(params: {
  stepKey: string;
  event: WorkflowEvent;
  clock: () => number;
  deadline: number | undefined;
  target: { executionId: string } | { start: ExecutionStartRequest };
}): AsyncGenerator<StepResponse, unknown, any> {
  const { stepKey, event, clock, target } = params;
  const timed = params.deadline !== undefined;

  yield new StepKey(stepKey);

  const cached = yield new UnfinishedCacheCheck();

  // The recorded deadline wins over the argument, so a deadline computed
  // from Date.now() does not move on replay.
  let recordedDeadline: number | undefined;
  if (cached instanceof StepUnfinished) {
    if (cached.response instanceof StepJoinWait) {
      recordedDeadline = cached.response.wakeAt;
    }
  } else if (cached) {
    return yield* replay(cached);
  }
  const deadline = timed ? (recordedDeadline ?? params.deadline) : undefined;

  const targetId =
    "start" in target ? yield* startTarget(target.start) : target.executionId;

  if (targetId === event.executionId) {
    const err = new WorkflowJoinError(
      "JOIN_SELF",
      targetId,
      `Execution "${targetId}" cannot join itself`
    );
    yield new StepError(err);
    throw err;
  }

  const joined: RuntimeCallResult<JoinResult> = yield new StepRuntimeCall(
    ({ joins }) =>
      infrastructure(() =>
        joins.join({
          targetExecutionId: targetId,
          executionId: event.executionId,
          stepKey,
        })
      )
  );
  if (!joined.ok) {
    throw new Error("JoinClient.join failed without an infrastructure error");
  }

  const result = joined.value;
  switch (result.status) {
    case "terminal": {
      const { outcome } = result;
      if (outcome.status === "failed") {
        const err = new WorkflowJoinError(
          "JOINED_EXECUTION_FAILED",
          targetId,
          `Joined execution "${targetId}" failed: ${outcome.error?.message}`,
          { cause: outcome.error }
        );
        yield new StepError(err);
        throw err;
      }
      if (outcome.status === "cancelled") {
        const err = new WorkflowJoinError(
          "JOINED_EXECUTION_CANCELLED",
          targetId,
          outcome.reason === undefined
            ? `Joined execution "${targetId}" was cancelled`
            : `Joined execution "${targetId}" was cancelled: ${outcome.reason}`,
          {
            ...(outcome.reason !== undefined ? { reason: outcome.reason } : {}),
            ...(outcome.source !== undefined ? { source: outcome.source } : {}),
          }
        );
        yield new StepError(err);
        throw err;
      }
      if (!timed) return yield* record(outcome.result);
      return yield* record<TimedJoinResult<unknown>>({
        status: "completed",
        result: outcome.result,
      });
    }
    case "not-found": {
      const err = new WorkflowJoinError(
        "JOIN_TARGET_NOT_FOUND",
        targetId,
        `No execution has ID "${targetId}"`
      );
      yield new StepError(err);
      throw err;
    }
    case "waiting":
      if (deadline !== undefined && clock() >= deadline) {
        return yield* record<TimedJoinResult<unknown>>({ status: "timed-out" });
      }
      yield new StepJoinWait(deadline);
      throw new Error("Join wait yielded control unexpectedly");
  }
}

/**
 * Describes one external operation for `step.effect`. `effectId` names the
 * operation in the external system, so every caller that means it, in any
 * execution or runtime, uses the same ID, and no caller reuses it for another
 * operation. The external system must guarantee that repeated `perform` calls
 * for one ID make one operation with one result, that `not-found` proves no
 * call exists or can still finish, and that `settled` and `failed` never
 * change.
 */
export type EffectDescriptor<T, Input = DurableObject> = {
  effectId: string;
  /** Names the version of the behaviour of `input`, `lookup` and `perform`. */
  operation: string;
  input: Input & Durable<Input>;
  /** Milliseconds before an uncertain outcome is looked up again. */
  retryIn: number;
  /**
   * The number of attempts that may end uncertain before the step throws a
   * `WorkflowEffectError` with code `EFFECT_OUTCOME_UNKNOWN`. Without it, the
   * step looks the effect up until its outcome is known.
   */
  maxAttempts?: number;
  /** Reports what the external system knows about `effectId`. */
  lookup(effectId: string): Promise<EffectLookup<T>>;
  /** Performs the operation under `effectId`. */
  perform(effectId: string): Promise<T>;
  /**
   * Returns true when an error thrown by `perform` proves that the operation
   * failed and will never succeed. The effect then fails with the error, and
   * the step throws it. Any other thrown value leaves the outcome uncertain.
   */
  failsOn?(error: unknown): boolean;
};

/**
 * Checks the effect's result type without taking part in its inference. A
 * `perform` that resolves with nothing settles the effect with `undefined`.
 */
type DurableEffectResult<T> = [T] extends [void]
  ? unknown
  : IfDurable<T, { perform(effectId: string): Promise<Durable<T>> }>;

async function* effectStep(
  key: string,
  descriptor: EffectDescriptor<unknown, unknown>
): AsyncGenerator<StepResponse, unknown, any> {
  yield new StepKey(key);

  const cached = yield new StepCacheCheck();

  if (cached) return yield* replay(cached);

  const { effectId, operation, input, retryIn, maxAttempts, lookup, perform, failsOn } =
    (descriptor ?? {}) as Partial<EffectDescriptor<unknown, unknown>>;
  const invalid = invalidEffectRequest({
    effectId,
    operation,
    input,
    retryIn,
    maxAttempts,
    lookup,
    perform,
    failsOn,
  });
  if (invalid) {
    const err = new WorkflowEffectError(
      "INVALID_EFFECT_REQUEST",
      `step.effect "${key}": ${invalid}`
    );
    yield new StepError(err);
    // unreachable – consumer calls throw() on the generator first
    throw err;
  }

  const outcome: EffectCallOutcome = yield new StepEffectCall({
    stepKey: key,
    effectId: effectId!,
    operation: operation!,
    input: input as DurableObject,
    retryIn: retryIn!,
    maxAttempts,
    lookup: (id) => lookup!.call(descriptor, id),
    perform: (id) => perform!.call(descriptor, id),
    failsOn: failsOn && ((error) => failsOn.call(descriptor, error)),
  });

  switch (outcome.status) {
    case "settled":
      return yield* record(outcome.result);
    case "failed":
      yield new StepError(outcome.error);
      throw outcome.error;
    case "unknown": {
      const err = new WorkflowEffectError(
        "EFFECT_OUTCOME_UNKNOWN",
        `The outcome of effect "${effectId}" is still unknown after ${outcome.attempts} ${
          outcome.attempts === 1 ? "attempt" : "attempts"
        }`
      );
      yield new StepError(err);
      throw err;
    }
    case "rejected": {
      const err = new WorkflowEffectError(
        outcome.code,
        `Effect "${effectId}" was begun with a different operation or input`
      );
      yield new StepError(err);
      throw err;
    }
    case "suspended":
      yield new StepEffectWait(outcome.wakeAt);
      throw new Error("Effect wait yielded control unexpectedly");
  }
}

function invalidEffectRequest(request: {
  effectId: unknown;
  operation: unknown;
  input: unknown;
  retryIn: unknown;
  maxAttempts: unknown;
  lookup: unknown;
  perform: unknown;
  failsOn: unknown;
}): string | undefined {
  const validated = validateEffectRequest(request);
  if (!validated.ok) return validated.message;
  const { retryIn } = request;
  if (typeof retryIn !== "number" || !Number.isFinite(retryIn) || retryIn < 0) {
    return "retryIn must be a finite, non-negative number";
  }
  const { maxAttempts } = request;
  if (
    maxAttempts !== undefined &&
    (typeof maxAttempts !== "number" || !Number.isInteger(maxAttempts) || maxAttempts < 1)
  ) {
    return "maxAttempts must be a positive integer";
  }
  if (typeof request.lookup !== "function") return "lookup must be a function";
  if (typeof request.perform !== "function") return "perform must be a function";
  if (request.failsOn !== undefined && typeof request.failsOn !== "function") {
    return "failsOn must be a function";
  }
  return undefined;
}

async function* startStep(
  key: string,
  request: ExecutionStartRequest
): AsyncGenerator<StepResponse, { executionId: string }, any> {
  yield new StepKey(key);

  const cached = yield new StepCacheCheck();

  if (cached) return yield* replay(cached);

  const executionId = yield* startTarget(request);

  return yield* record({ executionId });
}

/**
 * Starts the request's target and returns its execution ID. A rejected start
 * is yielded as a `WorkflowStartError` step error.
 */
async function* startTarget(
  request: ExecutionStartRequest
): AsyncGenerator<StepResponse, string, any> {
  const started: RuntimeCallResult<ExecutionStartResult> =
    // The client's failures are never the workflow's to catch: whatever it
    // throws, the delivery ends and the next attempt calls it again.
    yield new StepRuntimeCall(({ executions }) =>
      infrastructure(() => executions.start(request))
    );

  if (!started.ok) {
    throw new Error("ExecutionClient.start failed without an infrastructure error");
  }

  const outcome = started.value;

  if (outcome.status === "rejected") {
    const err = new WorkflowStartError(outcome.code, outcome.message);
    yield new StepError(err);
    // unreachable – consumer calls throw() on the generator first
    throw err;
  }

  return outcome.executionId;
}

function createWorkflowStore<T>(params: {
  definition: StoreDefinition<any>;
  id: string;
  event: WorkflowEvent;
  clock: () => number;
}): WorkflowStore<T> {
  const { id, event, clock } = params;
  const key = { storeName: params.definition.name, storeId: id };
  // Any operation may run `initial` on a missing store, so its errors must
  // reach workflow code instead of ending the delivery as infrastructure.
  const definition: StoreDefinition<any> = {
    ...params.definition,
    initial: workflowCallback(params.definition.initial),
  };

  return {
    definition: params.definition,
    id,
    key,
    get(stepKey?: string) {
      return runtimeStep(
        stepKey ?? getCallSiteHash(this.get),
        async ({ stores }) =>
          (await stores.getStore({ definition, id })) as StoreSnapshot<T>
      );
    },
    select<R>(stepKey: string, selector: StoreSelector<T, R>) {
      return runtimeStep(stepKey, async ({ stores }) => {
        const snapshot = await stores.getStore({ definition, id });
        // Run the selector against a clone so accidental mutation cannot
        // corrupt shared state (consistent with when, which throws via
        // its tracking proxy).
        return workflowCallback(selector)(cloneStoreState(snapshot.state as T));
      });
    },
    update(stepKey, updater) {
      return runtimeStep(
        stepKey,
        async ({ stores }) =>
          (await stores.updateStore({
            definition,
            id,
            updater: workflowCallback(updater, assertSynchronousUpdater) as any,
            // Exactly-once across the store-commit/heap-write gap: on replay
            // after a crash, the store's applied-steps ledger returns the
            // recorded result instead of re-running the updater.
            stepId: { executionId: event.executionId, stepKey },
          })) as StoreUpdateResult<T>
      );
    },
    updateFrom(stepKey, snapshot, updater) {
      return runtimeStep(
        stepKey,
        async ({ stores }) =>
          (await stores.updateStoreFrom({
            definition,
            id,
            snapshot,
            updater: workflowCallback(updater, assertSynchronousUpdater) as any,
            stepId: { executionId: event.executionId, stepKey },
          })) as StoreUpdateFromResult<T>
      );
    },
    deleteFrom(stepKey, snapshot) {
      return runtimeStep<StoreDeleteFromResult>(stepKey, ({ stores }) =>
        stores.deleteStoreFrom({
          definition,
          id,
          snapshot,
          stepId: { executionId: event.executionId, stepKey },
        })
      );
    },
    when<R>(
      arg1:
        | string
        | StoreSelector<T, R | undefined | null | false>,
      arg2?: StoreSelector<T, R | undefined | null | false>,
      options?: { deadline: number }
    ): any {
      const timed = options !== undefined || (typeof arg1 !== "string" && arg2 !== undefined);
      if (timed) {
        if (typeof arg1 !== "string") {
          throw new TypeError(
            "A timed store.when takes (key, selector, { deadline }); it was called without a key"
          );
        }
        if (arg1 === "") {
          throw new Error("A timed store.when requires a non-empty step key");
        }
        const deadline = options?.deadline;
        if (typeof deadline !== "number" || !Number.isFinite(deadline)) {
          throw new TypeError("store.when deadline must be a finite number");
        }
        return timedWhenStep({
          definition,
          id,
          event,
          stepKey: arg1,
          selector: arg2!,
          deadline,
          clock,
        });
      }

      const stepKey =
        typeof arg1 === "string" ? arg1 : getCallSiteHash(this.when);
      const selector = (
        typeof arg1 === "string" ? arg2 : arg1
      ) as StoreSelector<T, R | undefined | null | false>;

      return whenStep({
        definition,
        id,
        event,
        stepKey,
        selector,
      });
    },
    take<R>(
      stepKey: string,
      selector: StoreSelector<T, R | undefined | null | false>,
      claim: (draft: Draft<T>, selected: NonNullable<R>) => void
    ) {
      return takeStep({
        definition,
        id,
        event,
        stepKey,
        selector,
        claim,
      });
    },
  };
}

export type StepRunOptions<S extends StandardSchemaV1> = {
  /**
   * Validates the step's decoded value, on the first run and on every
   * replay; the step returns the schema's output. The step function must
   * return the schema's input as a durable value.
   */
  schema: S;
};

/**
 * A step function whose result, or the value its promise resolves to, is a
 * durable value. Checking the whole function type keeps the result's type as
 * TypeScript infers it, and names the offending property in the error.
 */
export type DurableStepFn<F> = F extends () => infer R
  ? () => R extends PromiseLike<infer U> ? PromiseLike<DurableUnion<U>> : DurableUnion<R>
  : never;

type DurableUnion<T> = T extends unknown ? Durable<T> : never;

function run<F extends () => unknown>(
  fn: F & DurableStepFn<F>
): AsyncGenerator<StepResponse, Awaited<ReturnType<F>>, StepResponse>;

function run<F extends () => unknown>(
  key: string,
  fn: F & DurableStepFn<F>
): AsyncGenerator<StepResponse, Awaited<ReturnType<F>>, StepResponse>;

function run<S extends StandardSchemaV1>(
  fn: () => StepSchemaInput<S> | PromiseLike<StepSchemaInput<S>>,
  options: StepRunOptions<S>
): AsyncGenerator<StepResponse, StandardSchemaV1.InferOutput<S>, StepResponse>;

function run<S extends StandardSchemaV1>(
  key: string,
  fn: () => StepSchemaInput<S> | PromiseLike<StepSchemaInput<S>>,
  options: StepRunOptions<S>
): AsyncGenerator<StepResponse, StandardSchemaV1.InferOutput<S>, StepResponse>;

// NOTE: run/delay/poll are plain (synchronous) functions that delegate to
// async generators. This matters for keyless steps: the call-site hash must be
// captured while the user's call site is still on the stack. An async
// generator body doesn't execute until its first .next(), which happens inside
// the workflow driver, so hashing there would give every keyless step the same
// key.
function run(
  arg1: string | (() => unknown),
  arg2?: (() => unknown) | StepRunOptions<StandardSchemaV1>,
  arg3?: StepRunOptions<StandardSchemaV1>
): AsyncGenerator<StepResponse, any, StepResponse> {
  let key: string | null = null;
  let fn: () => unknown;
  let options: StepRunOptions<StandardSchemaV1> | undefined;

  if (typeof arg1 === "string") {
    key = arg1;
    fn = arg2 as () => unknown;
    options = arg3;
  } else {
    fn = arg1;
    options = arg2 as StepRunOptions<StandardSchemaV1> | undefined;
  }

  if (!key) key = getCallSiteHash(run);

  return durableStep(key, fn, options?.schema);
}

type StepSchemaInput<S extends StandardSchemaV1> = Durable<
  StandardSchemaV1.InferInput<S>
>;

/** `step.delay`, reading its wake time from the runtime clock. */
function createDelay(clock: () => number) {
  function delay(retryInterval: number): AsyncGenerator<StepResponse, void, any>;
  function delay(key: string, retryInterval: number): AsyncGenerator<StepResponse, void, any>;
  function delay(
    arg1: string | number,
    arg2?: number
  ): AsyncGenerator<any, void, StepDelay> {
    let key: string | null = null;
    let retryInterval: number;

    if (typeof arg1 === "string") {
      key = arg1;
      retryInterval = arg2!;
    } else {
      retryInterval = arg1;
    }

    if (!key) key = getCallSiteHash(delay);

    return delayStep(key, retryInterval, clock);
  }

  return delay;
}

async function* delayStep(
  key: string,
  retryInterval: number,
  clock: () => number
): AsyncGenerator<any, void, StepDelay> {
  yield new StepKey(key);

  const cached = yield new StepCacheCheck();

  if (cached) {
    yield new StepDelay(cached.wakeAt);
  } else {
    yield new StepDelay(clock() + retryInterval);
  }
}

type PollOpts = { maxAttempts: number; retryInterval: number };
type PollPredicate = () => boolean | Promise<boolean>;

function poll(opts: PollOpts, predicate: PollPredicate): AsyncGenerator<StepResponse, void, any>;
function poll(
  key: string,
  opts: PollOpts,
  predicate: PollPredicate
): AsyncGenerator<StepResponse, void, any>;

function poll(
  arg1: string | PollOpts,
  arg2: PollOpts | PollPredicate,
  arg3?: PollPredicate
) {
  let key: string | null = null;
  let opts: PollOpts;
  let predicate: PollPredicate;

  if (typeof arg1 === "string") {
    key = arg1;
    opts = arg2 as PollOpts;
    predicate = arg3 as PollPredicate;
  } else {
    opts = arg1;
    predicate = arg2 as PollPredicate;
  }

  if (!key) key  = getCallSiteHash(poll);

  const task = async () => {
    if (!(await predicate())) {
      throw new RetryableError("Polling reached max retries", {
        maxAttempts: opts.maxAttempts,
        retryInterval: opts.retryInterval,
      });
    }
  };

  return run(key, task);
}

async function* durableStep(
  key: string,
  fn: () => unknown,
  schema: StandardSchemaV1 | undefined
): AsyncGenerator<StepResponse, any, any> {
  yield new StepKey(key);

  const cached = yield new StepCacheCheck();

  if (cached) {
    const value = yield* replay(cached);
    if (!schema) return value;
    const validated = await validateStepValue(key, schema, value);
    if (cached.schema === "rejected" && validated.ok) {
      throw new WorkflowValueError(
        "STEP_SCHEMA_INVALID",
        key,
        `Step "${key}" schema rejected its value on the first run`
      );
    }
    return unwrapValidated(validated);
  }

  let result: unknown;
  try {
    result = await fn();
  } catch (err: unknown) {
    if (err instanceof RetryableError) {
      yield new StepError(err, {
        maxAttempts: err.maxAttempts,
        retryInterval: err.retryInterval,
      });
    } else {
      yield new StepError(err);
    }
    // unreachable – consumer calls throw() on the generator first
    throw err;
  }

  if (!schema) return yield* record(result);

  // The schema validates the value as replay will decode it. A value the
  // codec rejects is left for the driver to record as INVALID_STEP_VALUE.
  const encoding = durableValueCodec.encode(result);
  if (encoding.status === "invalid") return yield* record(result);
  const decoding = durableValueCodec.decode(encoding.canonical);
  if (decoding.status === "invalid") throw new Error(decoding.message);
  const validated = await validateStepValue(key, schema, decoding.value);
  yield* record(result, validated.ok ? "accepted" : "rejected");
  return unwrapValidated(validated);
}

type ValidatedStepValue =
  | { ok: true; value: unknown }
  | { ok: false; error: WorkflowValueError };

async function validateStepValue(
  key: string,
  schema: StandardSchemaV1,
  value: unknown
): Promise<ValidatedStepValue> {
  const result = await schema["~standard"].validate(value);
  if (result.issues) {
    const messages = result.issues.map((issue) => issue.message).join("; ");
    return {
      ok: false,
      error: new WorkflowValueError(
        "STEP_SCHEMA_INVALID",
        key,
        `Step "${key}" value failed its schema: ${messages}`
      ),
    };
  }
  return { ok: true, value: result.value };
}

function unwrapValidated(validated: ValidatedStepValue) {
  if (!validated.ok) throw validated.error;
  return validated.value;
}

/**
 * Yields a step's result for the driver to record, and returns the decoded
 * record the driver resumes the step with, so that the first run returns
 * what replay will.
 */
async function* record<T>(
  result: T,
  schema?: StepSchemaVerdict
): AsyncGenerator<StepResponse, T, any> {
  const recorded: StepResult = yield new StepResult(result, schema);
  return recorded.result;
}

/** Yields a recorded response; the driver throws a recorded error into the step. */
async function* replay(cached: StepResponse): AsyncGenerator<StepResponse, any, any> {
  const recorded: StepResult = yield cached;
  return recorded.result;
}

/**
 * Yields a store step's error for the driver to record and throw. A value
 * error raised by a store client is given this step's key.
 */
async function* fail(key: string, error: unknown): AsyncGenerator<StepResponse, never, any> {
  const err =
    error instanceof WorkflowValueError && error.stepKey !== key
      ? new WorkflowValueError(error.code, key, error.message)
      : error;
  yield new StepError(err);
  // unreachable – consumer calls throw() on the generator first
  throw err;
}

/**
 * A durable step whose work is a runtime client call. The driver makes the
 * call and resumes the step with its result.
 */
async function* runtimeStep<T>(
  key: string,
  call: StepRuntimeCall<T>["call"]
): AsyncGenerator<StepResponse, T, any> {
  yield new StepKey(key);

  const cached = yield new StepCacheCheck();

  if (cached) return yield* replay(cached);

  const outcome: RuntimeCallResult<T> = yield new StepRuntimeCall(call);

  if (!outcome.ok) return yield* fail(key, outcome.error);

  return yield* record(outcome.value);
}

async function* whenStep<T, R>(params: {
  definition: StoreDefinition<any>;
  id: string;
  event: WorkflowEvent;
  stepKey: string;
  selector: StoreSelector<T, R | undefined | null | false>;
}): AsyncGenerator<StepResponse, NonNullable<R>, any> {
  const { stepKey } = params;

  yield new StepKey(stepKey);

  const cached = yield new StepCacheCheck();

  if (cached) return yield* replay(cached);

  const evaluated = yield* evaluateWhen(params);

  if (evaluated.matched) return yield* record(evaluated.value);

  yield* waitOnStore(evaluated.waiter);

  throw new Error("Store wait yielded control unexpectedly");
}

/**
 * Records `matched` if the selector matches, otherwise `timed-out` once the
 * deadline has passed. Otherwise suspends with the deadline as `wakeAt`.
 */
async function* timedWhenStep<T, R>(params: {
  definition: StoreDefinition<any>;
  id: string;
  event: WorkflowEvent;
  stepKey: string;
  selector: StoreSelector<T, R | undefined | null | false>;
  deadline: number;
  clock: () => number;
}): AsyncGenerator<StepResponse, TimedWhenResult<R>, any> {
  const { stepKey, clock } = params;

  yield new StepKey(stepKey);

  const cached = yield new UnfinishedCacheCheck();

  // The recorded deadline wins over the argument, so a deadline computed
  // from Date.now() does not move on replay. An untimed `when` under the
  // same key leaves no deadline, and the argument is used.
  let recordedDeadline: number | undefined;
  if (cached instanceof StepUnfinished) {
    if (cached.response instanceof StepStoreWait) {
      recordedDeadline = cached.response.wakeAt;
    }
  } else if (cached) {
    return yield* replay(cached);
  }
  const deadline = recordedDeadline ?? params.deadline;

  const evaluated = yield* evaluateWhen(params);

  if (evaluated.matched) {
    return yield* record<TimedWhenResult<R>>({
      status: "matched",
      value: evaluated.value,
    });
  }

  if (clock() >= deadline) {
    return yield* record<TimedWhenResult<R>>({ status: "timed-out" });
  }

  yield* waitOnStore(evaluated.waiter, deadline);

  throw new Error("Store wait yielded control unexpectedly");
}

/**
 * Reads the store and runs the selector. Without a match, returns the waiter
 * that a store change satisfying the selector's read paths would wake.
 */
async function* evaluateWhen<T, R>(params: {
  definition: StoreDefinition<any>;
  id: string;
  event: WorkflowEvent;
  stepKey: string;
  selector: StoreSelector<T, R | undefined | null | false>;
}): AsyncGenerator<
  StepResponse,
  { matched: true; value: NonNullable<R> } | { matched: false; waiter: StoreWaiter },
  any
> {
  const { definition, id, event, stepKey, selector } = params;

  const read: RuntimeCallResult<StoreSnapshot<unknown>> =
    yield new StepRuntimeCall(({ stores }) =>
      stores.getStore({ definition, id })
    );

  if (!read.ok) return yield* fail(stepKey, read.error);

  const snapshot = read.value;
  const { result, readPaths } = trackStoreSelector(
    cloneStoreState(snapshot.state as T),
    selector
  );

  if (result !== undefined && result !== null && result !== false) {
    return { matched: true, value: result as NonNullable<R> };
  }

  return {
    matched: false,
    waiter: {
      workflowId: event.workflowId,
      executionId: event.executionId,
      stepKey,
      storeName: definition.name,
      storeId: id,
      instanceId: snapshot.instanceId,
      sinceVersion: snapshot.version,
      readPaths,
    },
  };
}

async function* takeStep<T, R>(params: {
  definition: StoreDefinition<any>;
  id: string;
  event: WorkflowEvent;
  stepKey: string;
  selector: StoreSelector<T, R | undefined | null | false>;
  claim: (draft: Draft<T>, selected: NonNullable<R>) => void;
}): AsyncGenerator<StepResponse, NonNullable<R>, any> {
  const { definition, id, event, stepKey, selector, claim } = params;

  yield new StepKey(stepKey);

  const cached = yield new StepCacheCheck();

  if (cached) return yield* replay(cached);

  const taken: RuntimeCallResult<StoreTakeResult<R>> =
    yield new StepRuntimeCall(({ stores }) =>
      stores.takeFromStore({
        definition,
        id,
        selector: workflowCallback(selector) as any,
        claim: workflowCallback(claim, assertSynchronousClaim) as any,
        // Exactly-once across the store-commit/heap-write gap: on replay
        // after a crash, the store's applied-steps ledger returns the
        // recorded (matched) outcome instead of re-running selector/claim.
        stepId: { executionId: event.executionId, stepKey },
      })
    );

  if (!taken.ok) return yield* fail(stepKey, taken.error);

  const outcome = taken.value;

  if (outcome.matched) return yield* record(outcome.selected as NonNullable<R>);

  yield* waitOnStore({
    workflowId: event.workflowId,
    executionId: event.executionId,
    stepKey,
    storeName: definition.name,
    storeId: id,
    instanceId: outcome.instanceId,
    sinceVersion: outcome.version,
    readPaths: outcome.readPaths,
  });

  throw new Error("Store take yielded control unexpectedly");
}

/**
 * Suspends the step until a store change wakes it, or until `wakeAt`. Resumes
 * only when registering the waiter fails with a workflow-facing error.
 */
async function* waitOnStore(
  waiter: StoreWaiter,
  wakeAt?: number
): AsyncGenerator<StepResponse, void, any> {
  const registered: RuntimeCallResult<void> = yield new StepStoreWaitRequest(
    waiter,
    wakeAt
  );

  if (!registered.ok) return yield* fail(waiter.stepKey, registered.error);
}
