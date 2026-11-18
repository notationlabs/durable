# @notation/durable

`@notation/durable` is the primary package entry point for defining workflows, driving steps and stores, creating local or HTTP SDK clients, and running executions with `WorkflowRunner` and `ExecutionDispatcher`.

## Workflows and routing

Define workflows with `workflow`, or its alias `createWorkflow`, and group them into a typed router map with `createWorkflowRouter`:

```ts [@notation/durable]
import type { Logger } from "pino";

export type EventContext = ReadonlyMap<any, any>;

export type WorkflowEvent<
  Params extends EventParams = EventParams,
  Context extends ReadonlyMap<any, any> = ReadonlyMap<any, any>,
> = {
  workflowId: string;
  executionId: string;
  params: Params;
  context: Context;
};

export type WorkflowFn<
  Params extends EventParams,
  Result,
  Context extends EventContext,
> = (
  step: StepRunner,
  event: WorkflowEvent<Params, Context>,
  logger: Logger
) => AsyncGenerator<any, Result>;

export type DurableWorkflowFn<Params, Result> = IfDurable<
  Params,
  { "Workflow params must be durable values": Durable<Params> }
> &
  IfDurable<
    Result,
    (step: any, event: any, logger: any) => AsyncGenerator<any, Durable<Result>>
  >;

export function workflow<
  Params extends EventParams = EventParams,
  Result = DurableValue | void,
  Context extends EventContext = EventContext,
>(
  workflowFn: WorkflowFn<Params, Result, Context> &
    DurableWorkflowFn<Params, Result>
): WorkflowGenerator<Params, Result, Context>;

export const createWorkflow: typeof workflow;

export function createWorkflowRouter<W extends WorkflowRouter>(workflows: W): W;
```

| Function | Description |
| :--- | :--- |
| `workflow` | Wraps an async generator workflow function for durable replay |
| `createWorkflow` | Alias for `workflow` |
| `createWorkflowRouter` | Returns a typed workflow router map for runners and SDKs |

`WorkflowFn` receives `step`, `event`, and a Pino `logger`. `DurableWorkflowFn` enforces at compile time that both `Params` and `Result` satisfy `Durable<T>`.

`WorkflowEvent` provides the admitted event data on every delivery:

| Field | Description |
| :--- | :--- |
| `workflowId` | Registered workflow key from the router |
| `executionId` | Unique identifier for the execution |
| `params` | Admitted parameters for this execution |
| `context` | Read-only map of context entries from admission or middleware |

A child execution started via `step.start` or `step.call` inherits its caller's `context`. See [Workflows](../core/workflows.md) for authoring patterns.

## `StepRunner`

The `step` parameter passed to a workflow function implements `StepRunner`:

```ts [@notation/durable]
export type StepRunner = {
  run: {
    <F extends () => unknown>(
      fn: F & DurableStepFn<F>
    ): AsyncGenerator<StepResponse, Awaited<ReturnType<F>>, StepResponse>;
    <F extends () => unknown>(
      key: string,
      fn: F & DurableStepFn<F>
    ): AsyncGenerator<StepResponse, Awaited<ReturnType<F>>, StepResponse>;
    <S extends StandardSchemaV1>(
      fn: () => StepSchemaInput<S> | PromiseLike<StepSchemaInput<S>>,
      options: StepRunOptions<S>
    ): AsyncGenerator<StepResponse, StandardSchemaV1.InferOutput<S>, StepResponse>;
    <S extends StandardSchemaV1>(
      key: string,
      fn: () => StepSchemaInput<S> | PromiseLike<StepSchemaInput<S>>,
      options: StepRunOptions<S>
    ): AsyncGenerator<StepResponse, StandardSchemaV1.InferOutput<S>, StepResponse>;
  };

  delay: {
    (retryInterval: number): AsyncGenerator<StepResponse, void, any>;
    (key: string, retryInterval: number): AsyncGenerator<StepResponse, void, any>;
  };

  poll: {
    (
      opts: { maxAttempts: number; retryInterval: number },
      predicate: () => boolean | Promise<boolean>
    ): AsyncGenerator<StepResponse, void, any>;
    (
      key: string,
      opts: { maxAttempts: number; retryInterval: number },
      predicate: () => boolean | Promise<boolean>
    ): AsyncGenerator<StepResponse, void, any>;
  };

  store<Schema extends StandardSchemaV1>(
    definition: StoreDefinition<Schema> & { schema: DurableStoreSchema<Schema> },
    params?: { id?: string }
  ): WorkflowStore<StoreState<Schema>>;

  start<Params extends Record<string, any>>(
    stepKey: string,
    target: { workflowId: string; params?: Params & Durable<Params> },
    options?: StartOptions
  ): AsyncGenerator<StepResponse, { executionId: string }, StepResult | StepError>;

  effect<T, Input extends Record<string, any> = DurableObject>(
    stepKey: string,
    descriptor: EffectDescriptor<T, Input> & DurableEffectResult<T>
  ): AsyncGenerator<StepResponse, T, StepResult | StepError>;

  join: {
    <R = DurableValue>(
      stepKey: string,
      executionId: string
    ): AsyncGenerator<StepResponse, R, StepResult | StepError>;
    <R = DurableValue>(
      stepKey: string,
      executionId: string,
      options: { deadline: number }
    ): AsyncGenerator<StepResponse, TimedJoinResult<R>, StepResult | StepError>;
  };

  call: {
    <W extends WorkflowGenerator<any, any, any> = UntypedChild>(
      stepKey: string,
      target: CallTarget<W>,
      options?: StartOptions
    ): AsyncGenerator<StepResponse, WorkflowGeneratorReturnType<W>, StepResult | StepError>;
    <W extends WorkflowGenerator<any, any, any> = UntypedChild>(
      stepKey: string,
      target: CallTarget<W>,
      options: StartOptions & { deadline: number }
    ): AsyncGenerator<
      StepResponse,
      TimedJoinResult<WorkflowGeneratorReturnType<W>>,
      StepResult | StepError
    >;
  };

  all: {
    <const B extends readonly Branch<unknown>[]>(
      stepKey: string,
      branches: B,
      options?: AllOptions & { mode?: "fail-fast" }
    ): AsyncGenerator<StepResponse, BranchResults<B>, any>;
    <const B extends readonly Branch<unknown>[]>(
      stepKey: string,
      branches: B,
      options: AllOptions & { mode: "settle" }
    ): AsyncGenerator<StepResponse, SettledResults<B>, any>;
    <const B extends readonly Branch<unknown>[]>(
      stepKey: string,
      branches: B,
      options?: AllOptions
    ): AsyncGenerator<StepResponse, BranchResults<B> | SettledResults<B>, any>;
  };

  race<const B extends readonly Branch<unknown>[]>(
    stepKey: string,
    branches: B
  ): AsyncGenerator<StepResponse, RaceResult<B>, any>;
};
```

Every `StepRunner` method except `store` returns a generator that must be yielded with `yield*`. Step keys must be unique across an entire execution, including across loops and concurrent branches.

### `step.run`, `step.delay`, and `step.poll`

```ts [@notation/durable]
export type StepRunOptions<S extends StandardSchemaV1> = {
  schema: S;
};

export type DurableStepFn<F> = F extends () => infer R
  ? () => R extends PromiseLike<infer U>
      ? PromiseLike<DurableUnion<U>>
      : DurableUnion<R>
  : never;
```

| Option | Description |
| :--- | :--- |
| `schema` | Standard Schema validating the decoded step result on first run and replay |

Each step accepts an optional explicit key or derives a call-site hash when omitted; inside loops or dynamic call sites, always pass a unique key per execution.

When a Standard Schema validates `step.run`, the step callback returns a durable input type and resolves to the schema's output type, throwing `WorkflowValueError` (`STEP_SCHEMA_INVALID`) on failure.

`step.delay` pauses until `clock() + retryInterval` and writes a passed marker once elapsed so replays stay deterministic. `step.poll` checks its predicate per attempt and throws `RetryableError` whenever the predicate returns false, validating both attempt limits and intervals.

### `step.start`, `step.join`, and `step.call`

```ts [@notation/durable]
export type StartOptions = {
  detached?: boolean;
};

export type TimedJoinResult<R> =
  | { status: "completed"; result: R }
  | { status: "timed-out" };
```

| Option | Description |
| :--- | :--- |
| `detached` | Records the child's parent link as detached so cancellation does not propagate |
| `deadline` | Absolute epoch timestamp in milliseconds compared against the runtime clock |

Detached mode defaults to false. Altering the target workflow, canonical parameters, or detached mode on an unfinished repeated start throws `WorkflowStartError` (`START_REQUEST_MISMATCH`).

In `step.call`, parameters are required unless undefined is valid for the child's parameters type; untyped calls make parameters optional and default the return type to `DurableValue`.

When a join carries a deadline, the first waiting attempt persists it and subsequent replays ignore new arguments. The target execution is read before checking the deadline, so already-terminal targets resolve or fail normally even past the timestamp.

### `step.effect`

```ts [@notation/durable]
export type EffectLookup<T> =
  | { status: "settled"; result: T }
  | { status: "failed"; error: Error }
  | { status: "not-found" }
  | { status: "unknown" };

export type EffectDescriptor<T, Input = DurableObject> = {
  effectId: string;
  operation: string;
  input: Input & Durable<Input>;
  retryIn: number;
  maxAttempts?: number;
  lookup(effectId: string): Promise<EffectLookup<NoInfer<T>>>;
  perform(effectId: string): Promise<T>;
  failsOn?(error: unknown): boolean;
};

export type EffectRecord = {
  effectId: string;
  operation: string;
  input: DurableObject;
  status: "pending" | "settled" | "failed";
  result?: DurableValue;
  error?: Error;
  conflicts: EffectConflictReport[];
};
```

| Field | Description |
| :--- | :--- |
| `effectId` | Globally unique identifier for the external operation |
| `operation` | Versioned operation name compared against prior begins for the same `effectId` |
| `input` | Durable input payload compared against prior begins for the same `effectId` |
| `retryIn` | Milliseconds before an uncertain attempt is looked up again |
| `maxAttempts` | Maximum uncertain attempts before throwing `EFFECT_OUTCOME_UNKNOWN` |
| `lookup` | Checks the external system for an existing `effectId` outcome |
| `perform` | Executes the external operation under `effectId` |
| `failsOn` | Returns `true` when an error thrown by `perform` is a definite failure |

`retryIn` must be a finite, non-negative number. `maxAttempts` is optional and must be a positive integer when provided; omitting it allows unlimited lookup retries.

| Lookup status | Description |
| :--- | :--- |
| `"settled"` | Settles the effect with `result` and returns it to the workflow |
| `"failed"` | Settles the effect as failed with `error` and throws it to the workflow |
| `"not-found"` | Proceeds to call `perform` with `effectId` |
| `"unknown"` | Suspends the step for `retryIn` milliseconds or until external settlement |

When `perform` throws an error matched by `failsOn`, the effect settles permanently failed with that error; any other failure suspends the step as uncertain until `maxAttempts` is reached, throwing `WorkflowEffectError` (`EFFECT_OUTCOME_UNKNOWN`) (see [External Effects](../core/external-effects.md)).

### `step.all` and `step.race`

```ts [@notation/durable]
export type Branch<T> = AsyncGenerator<StepResponse, T, any>;

export type AllOptions = {
  concurrency?: number;
  mode?: "fail-fast" | "settle";
};

export type BranchResults<B extends readonly Branch<unknown>[]> = {
  -readonly [I in keyof B]: B[I] extends Branch<infer T> ? T : never;
};

export type SettledResult<T> =
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; error: unknown };

export type SettledResults<B extends readonly Branch<unknown>[]> = {
  -readonly [I in keyof B]: B[I] extends Branch<infer T> ? SettledResult<T> : never;
};

export type RaceResult<B extends readonly Branch<unknown>[]> = {
  [I in keyof B]: B[I] extends Branch<infer T>
    ? { index: I extends `${infer N extends number}` ? N : number; value: T }
    : never;
}[number];
```

| Option | Description |
| :--- | :--- |
| `concurrency` | Maximum number of branches with a step in flight simultaneously |
| `mode` | Failure handling strategy across branches in `step.all` |

Concurrency defaults to unbounded (`Infinity`) and requires either infinity or a positive integer; fail-fast mode throws the first branch error and abandons siblings, whereas settle mode collects `SettledResults` across all branches.

`step.race` runs branches with unbounded concurrency and returns the winning branch index and value, throwing `WorkflowRaceError` if every branch fails or the list is empty.

## Stores

`defineStore` declares a typed, schema-validated store definition that workflows access through `step.store(definition, { id? })` and non-workflow callers access through `RuntimeStore<T>`:

```ts [@notation/durable]
export type StoreVersion = number;
export type StorePath = readonly (string | number)[];

export type StoreKey = {
  storeName: string;
  storeId: string;
};

export type Draft<T> = T extends Date
  ? T
  : T extends ReadonlyMap<infer K, infer V>
    ? Map<Draft<K>, Draft<V>>
    : T extends ReadonlySet<infer U>
      ? Set<Draft<U>>
      : T extends object
        ? { -readonly [K in keyof T]: Draft<T[K]> }
        : T;

export type StoreDefinition<Schema extends StandardSchemaV1 = StandardSchemaV1> = {
  name: string;
  schema: Schema;
  initial: (id: string) => StoreState<Schema>;
};

export type StoreState<Schema extends StandardSchemaV1> =
  StandardSchemaV1.InferOutput<Schema>;

export type DurableStoreSchema<Schema extends StandardSchemaV1> = IfDurable<
  StoreState<Schema>,
  StandardSchemaV1<
    StandardSchemaV1.InferInput<Schema>,
    Durable<StoreState<Schema>>
  >
>;

export type StoreSelector<T, R> = (state: Readonly<T>) => R;

export type StoreSnapshot<T> = {
  state: T;
  instanceId: string;
  version: StoreVersion;
};

export type StoreUpdateResult<T> = {
  state: T;
  previousVersion: StoreVersion;
  version: StoreVersion;
};

export type StoreUpdateFromResult<T> =
  | ({ updated: true } & StoreUpdateResult<T>)
  | {
      updated: false;
      expectedInstanceId: string;
      actualInstanceId: string;
      expectedVersion: StoreVersion;
      actualVersion: StoreVersion;
    };

export type StoreDeleteFromResult =
  | { deleted: true }
  | {
      deleted: false;
      reason: "conflict";
      expectedInstanceId: string;
      actualInstanceId: string;
      expectedVersion: StoreVersion;
      actualVersion: StoreVersion;
    };

export type TimedWhenResult<R> =
  | { status: "matched"; value: NonNullable<R> }
  | { status: "timed-out" };

export function defineStore<
  Schema extends StandardSchemaV1<any, State>,
  State = StoreState<Schema>,
>(
  name: string,
  schema: Schema & StandardSchemaV1<any, State> & DurableStoreSchema<Schema>,
  options: {
    initial: (id: string) => NoInfer<Durable<State>>;
  }
): StoreDefinition<Schema>;
```

`step.store(definition, params?)` returns a `WorkflowStore<T>` synchronously without yielding or recording a step. `params.id` defaults to `event.executionId`.

```ts [@notation/durable]
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

export type RuntimeStore<T> = {
  get(): Promise<StoreSnapshot<T>>;
  update(updater: (draft: Draft<T>) => void | T): Promise<StoreUpdateResult<T>>;
  updateFrom(
    snapshot: StoreSnapshot<T>,
    updater: (draft: Draft<T>) => void | T
  ): Promise<StoreUpdateFromResult<T>>;
  deleteFrom(snapshot: StoreSnapshot<T>): Promise<StoreDeleteFromResult>;
};
```

| Member | Description |
| :--- | :--- |
| `get` | Reads the current snapshot, initializing from `initial` if missing |
| `select` | Runs a read-only selector against a cloned state and records its result |
| `update` | Atomically updates the store and retries on compare-and-swap conflict |
| `updateFrom` | Updates the store only if `instanceId` and `version` match `snapshot` |
| `deleteFrom` | Deletes the store only if `instanceId` and `version` match `snapshot` |
| `when` | Suspends until `selector` returns a non-nullish, non-false value or `deadline` passes |
| `take` | Atomically selects and mutates a matching item, or suspends until a match arrives |

Every operation on a missing store first creates it from `definition.initial(id)` at version `0` with a new `instanceId`. Updaters, selectors, and `take` claims must be synchronous and side-effect-free; async callbacks are rejected at runtime.

In `when(key, selector, { deadline })`, `key` and `options.deadline` are both required, and the first evaluation records `deadline` so later evaluations ignore the argument. A match wins over an elapsed deadline when `selector` matches at evaluation time.

## SDKs and execution lifecycle

`@notation/durable` exports both `createLocalSdk` for in-process runtimes and `createHttpSdkFactory` for remote HTTP servers:

```ts [@notation/durable]
export function createLocalSdk<W extends WorkflowRouter>(
  runtime: { executions: ExecutionClient },
  options?: {
    invoker?: Pick<WorkflowInvoker, "workflowEndEmitter">;
    pollInterval?: number;
  }
): {
  trigger<K extends string & keyof W>(
    event: TriggerEvent<K, EventParamsOf<W[K]>>
  ): Promise<TriggerAck>;
  triggerAndWait<K extends string & keyof W>(
    event: TriggerEvent<K, EventParamsOf<W[K]>>
  ): Promise<WorkflowGeneratorReturnType<W[K]>>;
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
  cancel(
    executionId: string,
    options?: CancelOptions
  ): Promise<CancelResult | undefined>;
};

export function createHttpSdkFactory<W extends WorkflowRouter>(): (params: {
  url: string;
  fetchOptions?: Omit<RequestInit, "body" | "method">;
}) => {
  trigger<K extends string & keyof W>(
    event: TriggerEvent<K, EventParamsOf<W[K]>>
  ): Promise<
    TriggerAck & {
      ack: () => Promise<TriggerAck>;
      waitForResult: () => Promise<WorkflowGeneratorReturnType<W[K]>>;
    }
  >;
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
  cancel(
    executionId: string,
    options?: CancelOptions
  ): Promise<CancelResult | undefined>;
  waitForResult<K extends string & keyof W>(
    ack: Pick<TriggerAck, "executionId">
  ): Promise<WorkflowGeneratorReturnType<W[K]>>;
};
```

| Option | Description |
| :--- | :--- |
| `invoker` | Optional local invoker whose `workflowEndEmitter` wakes `wait` calls early |
| `pollInterval` | Milliseconds between execution record reads during `wait` |

Pass your router type such as `typeof router` as the generic parameter to either SDK factory, constrained by `WorkflowRouter`.

`pollInterval` defaults to 25 ms and requires a positive finite number. `createLocalSdk` does not dispatch executions on its own – pair it with `ExecutionDispatcher` – and exposes `triggerAndWait` while the HTTP SDK provides `waitForResult` (see [`@notation/durable/client`](./client.md)).

### Execution types

```ts [@notation/durable]
export type ExecutionStatus =
  | "admitted"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export type TriggerAck = {
  executionId: string;
  status: "accepted" | "existing";
};

export type ExecutionParent = {
  executionId: string;
  stepKey: string;
  detached: boolean;
};

export type ExecutionCancellation = {
  reason?: string;
  source?: string;
  forced: boolean;
  requestedAt: number;
  deliveredAt?: number;
};

export type ExecutionOutcome =
  | { status: "completed"; result: unknown }
  | { status: "failed"; error: Error }
  | { status: "cancelled"; reason?: string; source?: string };

export interface ExecutionRecord {
  executionId: string;
  workflowId: string;
  params: EventParams;
  context: EventContext;
  status: ExecutionStatus;
  outcome?: ExecutionOutcome;
  admittedAt: number;
  updatedAt: number;
  unacknowledgedDeliveryCount: number;
  parent?: ExecutionParent;
  cancellation?: ExecutionCancellation;
}

export type ActiveExecutionView = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
  parent?: ExecutionParent;
  cancellation?: ExecutionCancellation;
  status: "admitted" | "running" | "suspended";
};

export type TerminalExecutionView =
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "completed";
      result: unknown;
    }
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "failed";
      error: Error;
    }
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "cancelled";
      reason?: string;
      source?: string;
    };

export type ExecutionView = ActiveExecutionView | TerminalExecutionView;

export type WaitResult =
  | TerminalExecutionView
  | { status: "timed-out"; execution: ActiveExecutionView };

export type ExecutionWaitOptions = {
  timeout?: number;
  signal?: AbortSignal;
};

export type ExecutionListRequest = {
  workflowId?: string;
  status?: ExecutionStatus;
  cursor?: string;
  limit?: number;
};

export type ExecutionViewPage = {
  executions: ExecutionView[];
  cursor?: string;
};

export type CancelOptions = {
  reason?: string;
  force?: boolean;
};

export type CancelResult =
  | { status: "requested"; execution: ActiveExecutionView }
  | TerminalExecutionView;

export interface ExecutionObserver {
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
}

export interface ExecutionControl {
  cancel(
    executionId: string,
    options?: CancelOptions
  ): Promise<CancelResult | undefined>;
}
```

`ExecutionWaitOptions` accepts two fields:

| Option | Description |
| :--- | :--- |
| `timeout` | Maximum milliseconds to wait before returning `timed-out` |
| `signal` | `AbortSignal` that stops waiting and rejects with `signal.reason` |

`timeout` defaults to `Infinity` and throws `RangeError` if negative or `NaN`.

`ExecutionListRequest` accepts four fields:

| Field | Description |
| :--- | :--- |
| `workflowId` | Filters results to a single workflow identifier |
| `status` | Filters results by status evaluated at the runtime clock |
| `cursor` | Opaque pagination cursor returned by the previous page |
| `limit` | Maximum number of executions returned per page |

`limit` defaults to `50` and is clamped at `1000`. Non-positive or non-integer limits, unknown statuses, and malformed cursors throw `ExecutionListRequestError`. Pages are ordered newest first by admission sequence.

`CancelOptions` accepts two fields:

| Option | Description |
| :--- | :--- |
| `reason` | Optional string stored on the cancellation request and outcome |
| `force` | Records `cancelled` immediately without running workflow code |

`force` defaults to false; invalid cancel arguments throw `ExecutionCancelRequestError` (see [Executions](../core/executions.md)).

## Runner and dispatcher

`WorkflowRunner` executes a single workflow delivery against a `Runtime`, and `ExecutionDispatcher` claims due executions under leases and delivers them through a `WorkflowInvoker`:

```ts [@notation/durable]
export class WorkflowRunner<
  Router extends Record<string, WorkflowGenerator<any, any, any>>,
> {
  constructor(runtime: Runtime, options: { router: Router; logger: Logger });

  run: (
    event: WorkflowEvent,
    options: DeliveryOptions & { logger?: Logger }
  ) => Promise<WorkflowRunOutcome>;
}

export class ExecutionDispatcher {
  constructor(
    runtime: Runtime,
    options: {
      invoker: WorkflowInvoker;
      logger: Logger;
      concurrency?: number;
      pollInterval?: number;
      keepAlive?: boolean;
      leaseDuration?: number;
      renewInterval?: number;
      abortGracePeriod?: number;
      filter?: ExecutionClaimFilter;
    }
  );

  start(): void;
  stop(): Promise<void>;
  tick(): Promise<number>;
  drain(): Promise<void>;
  get activeDeliveries(): number;
}
```

`WorkflowRunner.run` resolves with an outcome across completed, suspended, failed, or cancelled statuses, rejecting only on `InfrastructureError`; unknown workflows resolve with a failed outcome.

`ExecutionDispatcher` options, defaults, and constraints:

| Option | Default and constraint |
| :--- | :--- |
| `invoker` | Required `WorkflowInvoker` for claimed deliveries |
| `logger` | Required Pino logger instance |
| `concurrency` | `10` active deliveries; positive integer |
| `pollInterval` | `10` ms between idle claim polls |
| `keepAlive` | `true`; set `false` to unref timers |
| `leaseDuration` | `30_000` ms per lease; greater than zero |
| `renewInterval` | `leaseDuration / 3`; positive and below `leaseDuration` |
| `abortGracePeriod` | `1_000` ms after lease loss; non-negative |
| `filter` | Claims any due execution; empty `executionIds` throws `RangeError` |

| Member | Description |
| :--- | :--- |
| `start` | Starts the background claim polling loop |
| `stop` | Stops claiming new work and awaits `drain` |
| `tick` | Claims up to the free concurrency slots and returns the claimed count |
| `drain` | Resolves once all currently in-flight deliveries have settled |
| `activeDeliveries` | Count of deliveries currently in flight |

See [Leases & Delivery](../authoring/delivery-guarantees.md) for lease renewal, fencing, and abandonment semantics.

## Durable values and errors

```ts [@notation/durable]
export type DurableValue =
  | null
  | boolean
  | number
  | string
  | undefined
  | bigint
  | Date
  | Error
  | readonly DurableValue[]
  | ReadonlyMap<DurableValue, DurableValue>
  | ReadonlySet<DurableValue>
  | DurableObject;

export type DurableObject = { [key: string]: DurableValue };

export type Durable<T> = /* compile-time durability check */;

export type ErrorClass = abstract new (...args: any[]) => Error;

export function registerErrorClass(name: string, errorClass: ErrorClass): void;

export const WORKFLOW_RESULT_STEP_KEY = "$$workflow-result$$";
```

`Durable<T>` verifies types at compile time – rejecting functions while keeping arrays, tuples, dates, maps, sets, bigints, undefined, and errors intact – while runtime encoding enforces remaining structural rules on non-finite numbers, invalid dates, symbols, accessors, cycles, and non-plain instances (see [Step Values](../core/step-values.md)).

`registerErrorClass` registers custom `Error` subclasses per process so decoded step failures and SDK responses revive with matching prototypes.

`@notation/durable` exports these error types (see [Errors](./errors.md)):

| Category | Exports |
| :--- | :--- |
| Workflow errors | `RetryableError`, `StoreValidationError`, `WorkflowStartError`, `WorkflowJoinError`, `WorkflowCancelledError`, `WorkflowValueError`, `WorkflowRaceError`, `WorkflowEffectError` |
| Error code unions | `WorkflowValueErrorCode`, `WorkflowStartErrorCode`, `WorkflowJoinErrorCode`, `WorkflowEffectErrorCode`, `ExecutionAdmissionErrorCode`, `EffectDeliveryErrorCode` |
| SDK errors | `ExecutionAdmissionError`, `ExecutionListRequestError`, `ExecutionCancelRequestError`, `HttpRouteError` |
| Effect delivery errors | `EffectDeliveryError`, `EffectClientInvariantError` |
