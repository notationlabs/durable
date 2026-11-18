# @notation/durable/runtime

`@notation/durable/runtime` exports the abstract `Runtime` contract, the five client contracts, pure state-transition helpers for runtime authors, `WorkflowRunner`, `ExecutionDispatcher`, observation helpers, and the durable value codec. Application code normally imports workflows, steps, and SDK factories from [`@notation/durable`](./durable.md). For implementation guides, see [Custom Runtimes](../authoring/runtimes.md).

```ts [@notation/durable/runtime]
import { Runtime, ExecutionDispatcher, WorkflowRunner } from "@notation/durable/runtime";
```

## Runtime

A custom runtime extends `Runtime` and accepts `RuntimeOptions`:

```ts [@notation/durable/runtime]
export interface RuntimeOptions {
  clock?: () => number;
}

export abstract class Runtime {
  abstract readonly clock: () => number;
  abstract readonly executions: ExecutionClient;
  abstract readonly effects: EffectClient;
  abstract readonly heap: HeapClient;
  abstract readonly joins: JoinClient;
  abstract readonly stores: StoreClient;
}
```

| Property | Contract |
| :--- | :--- |
| `clock` | Returns current Unix epoch milliseconds used for due times and leases |
| `executions` | Execution admission, leasing, acknowledgement, listing, and cancellation |
| `effects` | External effect coordination and settlement records |
| `heap` | Fenced step-record storage and waiter cleanup |
| `joins` | Child and sibling workflow join waiter registration |
| `stores` | Versioned durable state stores and store waiters |

`SqliteRuntime` and `MemoryRuntime` default `options.clock` to `Date.now`.

## Client contracts

### ExecutionClient

`ExecutionClient` manages execution records, leases, claims, wakes, listing, and cancellation:

```ts [@notation/durable/runtime]
export type ExecutionStatus =
  | "admitted"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export type ExecutionOutcome =
  | { status: "completed"; result: unknown }
  | { status: "failed"; error: Error }
  | { status: "cancelled"; reason?: string; source?: string };

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

export type ExecutionLease = {
  executionId: string;
  token: number;
  expiresAt: number;
};

export type ExecutionClaimFilter = {
  executionIds?: readonly string[];
};

export type LeaseLost = { status: "lost" };

export type WorkflowRunOutcome =
  | { status: "completed"; result: unknown }
  | { status: "suspended"; wakeAt?: number }
  | { status: "failed"; error: Error }
  | { status: "cancelled" };

export type ExecutionAdmissionErrorCode =
  | "EXECUTION_ID_CONFLICT"
  | "INVALID_EXECUTION_EVENT"
  | "EXECUTION_REQUEST_MISMATCH"
  | "IDEMPOTENT_TRIGGER_REQUIRES_ID";

export type ExecutionAdmissionRequest = {
  executionId?: string;
  workflowId: string;
  params: EventParams;
  context: EventContext;
  idempotent?: boolean;
};

export type ExecutionAdmissionResult =
  | { status: "accepted"; executionId: string }
  | { status: "existing"; executionId: string }
  | { status: "rejected"; code: ExecutionAdmissionErrorCode; message: string };

export type ExecutionListRequest = {
  workflowId?: string;
  status?: ExecutionStatus;
  cursor?: string;
  limit?: number;
};

export type ExecutionListPage = {
  executions: ExecutionRecord[];
  cursor?: string;
};

export type WorkflowStartErrorCode =
  | "INVALID_START_PARAMETERS"
  | "START_REQUEST_MISMATCH";

export type ExecutionStartRequest = {
  requestId: { executionId: string; stepKey: string };
  workflowId: string;
  params?: DurableObject;
  detached?: boolean;
};

export type ExecutionStartResult =
  | { status: "accepted"; executionId: string }
  | { status: "rejected"; code: WorkflowStartErrorCode; message: string };

export type ExecutionWakeResult =
  | { status: "requested" }
  | { status: "ignored" }
  | { status: "rejected"; code: "EXECUTION_NOT_FOUND"; message: string };

export type ExecutionCancelRequest = {
  executionId: string;
  reason?: string;
  force?: boolean;
};

export type ExecutionCancelResult =
  | { status: "requested" }
  | ExecutionOutcome
  | { status: "rejected"; code: "EXECUTION_NOT_FOUND"; message: string };

export type ExecutionAcknowledgeResult =
  | { status: "suspended" }
  | ExecutionOutcome
  | LeaseLost;

export type ExecutionRenewResult =
  | { status: "renewed"; lease: ExecutionLease }
  | LeaseLost;

export type ExecutionReleaseResult = { status: "released" } | LeaseLost;

export interface ExecutionClient {
  admit(request: ExecutionAdmissionRequest): Promise<ExecutionAdmissionResult>;
  get(executionId: string): Promise<ExecutionRecord | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionListPage>;
  start(request: ExecutionStartRequest): Promise<ExecutionStartResult>;
  cancel(request: ExecutionCancelRequest): Promise<ExecutionCancelResult>;
  requestWake(request: {
    executionId: string;
    wakeAt: number;
  }): Promise<ExecutionWakeResult>;
  claimDue(request: {
    limit: number;
    duration: number;
    filter?: ExecutionClaimFilter;
  }): Promise<ExecutionLease[]>;
  renew(request: {
    lease: ExecutionLease;
    duration: number;
  }): Promise<ExecutionRenewResult>;
  release(request: { lease: ExecutionLease }): Promise<ExecutionReleaseResult>;
  acknowledge(request: {
    lease: ExecutionLease;
    outcome: WorkflowRunOutcome;
  }): Promise<ExecutionAcknowledgeResult>;
}
```

`@notation/durable/runtime` also exports two type guards for execution status and lease loss:

```ts [@notation/durable/runtime]
export function isLeaseLost(value: unknown): value is LeaseLost;

export function isTerminalExecutionStatus(
  status: ExecutionStatus
): status is "completed" | "failed" | "cancelled";
```

`ExecutionListRequest.limit` defaults to `50` and is capped at `1000`. See [Execution Client](../authoring/execution-client.md) for the full adapter contract.

### HeapClient

`HeapClient` persists step outcomes and cancellation deliveries under lease fencing:

```ts [@notation/durable/runtime]
export type HeapRecord = {
  stepResponseJson: string;
  meta: { attempt: number; done: boolean };
};

export abstract class HeapClient {
  abstract readStep(params: {
    executionId: string;
    stepKey: string;
  }): Promise<HeapRecord | null>;

  abstract writeStep(params: {
    lease: ExecutionLease;
    stepKey: string;
    stepAttempt: number;
    done: boolean;
    stepResponseJson: string;
    deleteWaitersOf?: readonly string[];
  }): Promise<HeapRecord | LeaseLost>;

  abstract recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly {
      stepKey: string;
      stepAttempt: number;
      stepResponseJson: string;
    }[];
  }): Promise<(HeapRecord | null)[] | LeaseLost>;
}
```

See [Heap Client](../authoring/heap-client.md) for fencing and waiter-deletion guarantees.

### StoreClient and CasStoreClient

`StoreClient` defines store reads, conditional updates, takes, waiter registration, listing, and deletion. `CasStoreClient` implements optimistic compare-and-swap loops over five storage primitives:

```ts [@notation/durable/runtime]
export type Draft<T> = T extends Date
  ? T
  : T extends ReadonlyMap<infer K, infer V>
  ? Map<Draft<K>, Draft<V>>
  : T extends ReadonlySet<infer U>
  ? Set<Draft<U>>
  : T extends object
  ? { -readonly [K in keyof T]: Draft<T[K]> }
  : T;

export type StoreVersion = number;
export type StorePath = readonly (string | number)[];
export type StoreState<Schema extends StandardSchemaV1> = StandardSchemaV1.InferOutput<Schema>;
export type StoreKey = { storeName: string; storeId: string };
export type StoreStepId = { executionId: string; stepKey: string };
export type StoreSelector<T, R> = (state: Readonly<T>) => R;

export type StoreDefinition<Schema extends StandardSchemaV1 = StandardSchemaV1> = {
  name: string;
  schema: Schema;
  initial: (id: string) => StoreState<Schema>;
};

export type StoreSnapshot<T> = {
  state: T;
  instanceId: string;
  version: StoreVersion;
};

export type StoreReadSnapshot<T> = StoreSnapshot<T> & { created: boolean };

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

export type StoreTakeResult<R> =
  | {
      matched: true;
      selected: NonNullable<R>;
      instanceId: string;
      version: StoreVersion;
    }
  | {
      matched: false;
      instanceId: string;
      version: StoreVersion;
      readPaths: StorePath[];
    };

export type StoreWaiter = {
  workflowId: string;
  executionId: string;
  stepKey: string;
  storeName: string;
  storeId: string;
  instanceId: string;
  sinceVersion: StoreVersion;
  readPaths: StorePath[];
};

export type StoreMutationCommitResult =
  | { status: "committed" }
  | { status: "already-applied"; result: unknown }
  | { status: "conflict"; snapshot: StoreSnapshot<unknown> }
  | { status: "missing" };

export type StoreMutation = {
  definition: StoreDefinition;
  id: string;
  expected: StoreReadSnapshot<unknown>;
  nextState: unknown;
  changedPaths: StorePath[];
  stepId?: StoreStepId;
  result: unknown;
};

export type StoreDeletion = {
  definition: StoreDefinition;
  id: string;
  expected: StoreSnapshot<unknown>;
  stepId?: StoreStepId;
  result: StoreDeleteFromResult;
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

export type DurableStoreSchema<Schema extends StandardSchemaV1> = IfDurable<
  StoreState<Schema>,
  StandardSchemaV1<StandardSchemaV1.InferInput<Schema>, Durable<StoreState<Schema>>>
>;

export function defineStore<
  Schema extends StandardSchemaV1<any, State>,
  State = StoreState<Schema>,
>(
  name: string,
  schema: Schema & StandardSchemaV1<any, State> & DurableStoreSchema<Schema>,
  options: { initial: (id: string) => NoInfer<Durable<State>> }
): StoreDefinition<Schema>;

export abstract class StoreClient {
  store<Schema extends StandardSchemaV1>(
    definition: StoreDefinition<Schema>,
    id: string
  ): RuntimeStore<StoreState<Schema>>;

  abstract getStore<Schema extends StandardSchemaV1>(params: {
    definition: StoreDefinition<Schema>;
    id: string;
  }): Promise<StoreSnapshot<StoreState<Schema>>>;

  abstract updateStore<Schema extends StandardSchemaV1>(params: {
    definition: StoreDefinition<Schema>;
    id: string;
    updater: (draft: Draft<StoreState<Schema>>) => void | StoreState<Schema>;
    stepId?: StoreStepId;
  }): Promise<StoreUpdateResult<StoreState<Schema>>>;

  abstract updateStoreFrom<Schema extends StandardSchemaV1>(params: {
    definition: StoreDefinition<Schema>;
    id: string;
    snapshot: StoreSnapshot<StoreState<Schema>>;
    updater: (draft: Draft<StoreState<Schema>>) => void | StoreState<Schema>;
    stepId?: StoreStepId;
  }): Promise<StoreUpdateFromResult<StoreState<Schema>>>;

  abstract takeFromStore<Schema extends StandardSchemaV1, R>(params: {
    definition: StoreDefinition<Schema>;
    id: string;
    selector: StoreSelector<StoreState<Schema>, R>;
    claim: (draft: Draft<StoreState<Schema>>, selected: NonNullable<R>) => void;
    stepId?: StoreStepId;
  }): Promise<StoreTakeResult<R>>;

  abstract registerWaiter(waiter: StoreWaiter): Promise<void>;
  abstract listStores(definition: StoreDefinition): Promise<string[]>;
  abstract deleteStore(params: { definition: StoreDefinition; id: string }): Promise<void>;
  abstract deleteStoreFrom(params: {
    definition: StoreDefinition;
    id: string;
    snapshot: StoreSnapshot<unknown>;
    stepId?: StoreStepId;
  }): Promise<StoreDeleteFromResult>;
}

export abstract class CasStoreClient extends StoreClient {
  protected abstract readStore(params: {
    definition: StoreDefinition;
    id: string;
  }): Promise<StoreSnapshot<unknown> | undefined>;

  protected abstract insertStore(params: {
    definition: StoreDefinition;
    id: string;
    snapshot: StoreSnapshot<unknown>;
  }): Promise<StoreSnapshot<unknown>>;

  protected abstract getAppliedStoreStep(params: {
    definition: StoreDefinition;
    id: string;
    stepId: StoreStepId;
  }): Promise<{ result: unknown } | undefined>;

  protected abstract commitStoreMutation(
    mutation: StoreMutation
  ): Promise<StoreMutationCommitResult>;

  protected abstract commitStoreDeletion(
    deletion: StoreDeletion
  ): Promise<StoreMutationCommitResult>;

  protected createInstanceId(): string;
}
```

`StandardSchemaV1` is re-exported from `@standard-schema/spec`. See [Store Client](../authoring/store-client.md) for CAS loop semantics and waiter wake rules.

### EffectClient

`EffectClient` records external effect begins, settlements, watches, and conflict reports:

```ts [@notation/durable/runtime]
export type EffectBeginRequest = {
  operation: string;
  effectId: string;
  input: DurableObject;
  executionId: string;
  stepKey: string;
};

export type EffectOutcome =
  | { status: "settled"; result: DurableValue }
  | { status: "failed"; error: Error };

export type EffectBeginResult =
  | { status: "new" }
  | { status: "pending" }
  | EffectOutcome
  | { status: "rejected"; code: "EFFECT_REQUEST_MISMATCH" };

export type EffectSettleRequest = {
  operation: string;
  effectId: string;
} & EffectOutcome;

export type EffectSettleResult =
  | EffectOutcome
  | {
      status: "conflict";
      outcome: EffectOutcome;
      conflictingOutcome: EffectOutcome;
    };

export type EffectWatchRequest = {
  effectId: string;
  executionId: string;
  stepKey: string;
};

export type EffectConflictReport = {
  outcome: EffectOutcome;
  conflictingOutcome: EffectOutcome;
  reportedAt: number;
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

export type WorkflowEffectErrorCode =
  | "INVALID_EFFECT_REQUEST"
  | "EFFECT_REQUEST_MISMATCH"
  | "EFFECT_OUTCOME_UNKNOWN";

export type EffectDeliveryErrorCode =
  | "EFFECT_CLIENT_UNAVAILABLE"
  | "EFFECT_HANDLER_INVALID"
  | "EFFECT_RUNTIME_INVARIANT";

export interface EffectClient {
  begin(request: EffectBeginRequest): Promise<EffectBeginResult>;
  settle(request: EffectSettleRequest): Promise<EffectSettleResult>;
  watch(request: EffectWatchRequest): Promise<void>;
  getEffect(effectId: string): Promise<EffectRecord | undefined>;
}
```

See [Effect Client](../authoring/effect-client.md) for settlement and conflict guarantees.

### JoinClient

`JoinClient` reads a target execution's terminal outcome or registers a join waiter atomically:

```ts [@notation/durable/runtime]
export type JoinWaiter = {
  targetExecutionId: string;
  executionId: string;
  stepKey: string;
};

export type JoinResult =
  | { status: "terminal"; outcome: ExecutionOutcome }
  | { status: "waiting" }
  | { status: "not-found" };

export type WorkflowJoinErrorCode =
  | "JOINED_EXECUTION_FAILED"
  | "JOINED_EXECUTION_CANCELLED"
  | "JOIN_TARGET_NOT_FOUND"
  | "JOIN_SELF";

export interface JoinClient {
  join(waiter: JoinWaiter): Promise<JoinResult>;
}
```

See [Join Client](../authoring/join-client.md) for waiter registration and terminal wake semantics.

## Running workflows

### WorkflowRunner

`WorkflowRunner` executes a single workflow delivery against a router and returns a `WorkflowRunOutcome`:

```ts [@notation/durable/runtime]
export class WorkflowRunner<
  Router extends Record<string, WorkflowGenerator<any, any, any>>
> {
  constructor(runtime: Runtime, options: { router: Router; logger: Logger });

  run: (
    event: WorkflowEvent,
    options: DeliveryOptions & { logger?: Logger }
  ) => Promise<WorkflowRunOutcome>;
}
```

`run` rejects only with `InfrastructureError` when a delivery fails without an outcome, such as lease loss or signal abortion. Throwing `WorkflowCancelledError` resolves as cancelled only when a cancellation request was attached to the delivery; otherwise it resolves as failed.

### ExecutionDispatcher

`ExecutionDispatcher` claims due executions, renews leases, delegates deliveries to a `WorkflowInvoker`, and acknowledges outcomes:

```ts [@notation/durable/runtime]
export const DEFAULT_DISPATCHER_CONCURRENCY = 10;
export const DEFAULT_ABORT_GRACE_PERIOD = 1_000;

export interface DispatcherOptions {
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

export class ExecutionDispatcher {
  constructor(runtime: Runtime, options: DispatcherOptions);

  readonly activeDeliveries: number;

  start(): void;
  stop(): Promise<void>;
  tick(): Promise<number>;
  drain(): Promise<void>;
}
```

| Option | Description |
| :--- | :--- |
| `invoker` | `WorkflowInvoker` that executes each claimed delivery |
| `logger` | Pino logger instance |
| `concurrency` | Maximum concurrent deliveries managed by this dispatcher |
| `pollInterval` | Milliseconds between claim attempts when no work is due |
| `keepAlive` | Whether dispatcher timers keep the Node or Bun event loop alive |
| `leaseDuration` | Milliseconds a claimed lease lasts unless renewed |
| `renewInterval` | Milliseconds between lease renewal requests |
| `abortGracePeriod` | Milliseconds to wait after lease loss before freeing the delivery slot |
| `filter` | `ExecutionClaimFilter` restricting which execution IDs are claimed |

Defaults and validation rules for `ExecutionDispatcher` options:

- **`concurrency`** – Defaults to `10`. Throws `RangeError` if not a positive integer.
- **`pollInterval`** – Defaults to `10` ms.
- **`keepAlive`** – Defaults to `true`.
- **`leaseDuration`** – Defaults to `30_000` ms. Throws `RangeError` if not positive.
- **`renewInterval`** – Defaults to `leaseDuration / 3`. Throws `RangeError` if not positive and strictly less than `leaseDuration`.
- **`abortGracePeriod`** – Defaults to `1_000` ms. Throws `RangeError` if negative.
- **`filter`** – Claims any due execution by default. Throws `RangeError` if `filter.executionIds` is an empty array.

### WorkflowInvoker and DeliveryOptions

```ts [@notation/durable/runtime]
export type DeliveryOptions = {
  lease: ExecutionLease;
  signal: AbortSignal;
  cancellation?: ExecutionCancellation;
};

export type WorkflowInvoker = {
  workflowEndEmitter: EventEmitter;
  deliver(event: WorkflowEvent, options: DeliveryOptions): Promise<WorkflowRunOutcome>;
};

export class WorkerExitedError extends Error {
  constructor(message: string);
  static is(value: unknown): value is WorkerExitedError;
}
```

When `invoker.deliver` rejects, `ExecutionDispatcher` handles the failure based on the rejection error:

| Rejection | Dispatcher action |
| :--- | :--- |
| Worker exited with `WorkerExitedError` | Leaves the lease to expire so in-flight writes finish before redelivery |
| Any other error | Releases the lease immediately and schedules redelivery after backoff |

`workflowEndEmitter` is deprecated; outcomes are persisted on execution records and only emitted locally as a fast path.

### Workflow generator and step responses

```ts [@notation/durable/runtime]
export type WorkflowGenerator<
  Params extends EventParams,
  Result,
  Context extends EventContext
> = (
  genParams: WorkflowGeneratorParams<Params, Context>
) => AsyncGenerator<StepResponse, WorkflowResult<Result>, StepResponse>;

export type WorkflowGeneratorReturnType<CG> = CG extends WorkflowGenerator<
  infer Params,
  infer Result,
  infer Context
>
  ? Result
  : never;

export type WorkflowRouter = Record<string, WorkflowGenerator<any, any, any>>;
export type EventOfWorkflow<W extends WorkflowGenerator<any, any, any>> = Parameters<W>[0]["event"];
export type EventParamsOf<W extends WorkflowGenerator<any, any, any>> = Parameters<W>[0]["event"]["params"];
```

`@notation/durable/runtime` also exports `StepResponse`, `StepSchemaVerdict`, and every concrete step-response class:

| Class | Role |
| :--- | :--- |
| `StepKey` | Declares step identity and marks cancellation check boundaries |
| `StepCacheCheck` | Requests a cached outcome lookup before running a step |
| `StepInvalid` | Records a non-step or invalid yielded value |
| `StepResult` | Holds a completed step's durable result and schema verdict |
| `StepError` | Holds a failed or retrying step error and backoff policy |
| `StepDelay` | Suspends until an absolute clock wake timestamp |
| `StepStoreWait` | Records an unfinished store wait and optional deadline |
| `StepJoinWait` | Records an unfinished join wait and optional deadline |
| `StepEffectWait` | Records an uncertain external effect poll timestamp |
| `WorkflowResult` | Holds the workflow's terminal return value |
| `WorkflowDelay` | Represents a workflow-level delay response |
| `WorkflowRestart` | Signals a delivery restart boundary |

## Observation and control

`createExecutionObserver` and `createExecutionControl` build the read and cancellation APIs shared by the local SDK and `@notation/durable/http`:

```ts [@notation/durable/runtime]
export type ExecutionNotifier = {
  on(executionId: string, listener: (...args: any[]) => void): unknown;
  off(executionId: string, listener: (...args: any[]) => void): unknown;
};

export type ExecutionObserverOptions = {
  pollInterval?: number;
  notifier?: ExecutionNotifier;
};

export function createExecutionObserver(
  executions: Pick<ExecutionClient, "get" | "list">,
  options?: ExecutionObserverOptions
): ExecutionObserver;

export function createExecutionControl(
  executions: Pick<ExecutionClient, "cancel" | "get">
): ExecutionControl;

export function toExecutionView(record: ExecutionRecord): ExecutionView;

export function isTerminalExecutionView(
  view: ExecutionView
): view is TerminalExecutionView;

export function isDuration(value: unknown): value is number;
export function isPositiveInterval(value: unknown): value is number;
```

```ts [@notation/durable/runtime]
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

export interface ExecutionControl {
  cancel(executionId: string, options?: CancelOptions): Promise<CancelResult | undefined>;
}

export interface ExecutionObserver {
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(executionId: string, options?: ExecutionWaitOptions): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
}
```

Observer polling defaults to 25 ms and throws `RangeError` if non-positive or non-finite; `wait` throws `RangeError` if timeout is negative or `NaN`.

## Events and middleware

```ts [@notation/durable/runtime]
export type EventParams = Record<string, any> | void;
export type EventContext = ReadonlyMap<any, any>;

export type TriggerEvent<
  WorkflowId extends string,
  Params extends EventParams = EventParams
> = (Params extends void
  ? { workflowId: WorkflowId; params?: undefined; context?: Record<string, any> }
  : { workflowId: WorkflowId; params: Params; context?: Record<string, any> }) &
  (
    | { executionId?: string; idempotent?: false }
    | { executionId: string; idempotent: true }
  );

export type TriggerAck = {
  executionId: string;
  status: "accepted" | "existing";
};

export type ExecutionEvent<Params extends EventParams = EventParams> = {
  workflowId: string;
  executionId: string;
  params: Params;
  context?: Record<string, any>;
};

export type WorkflowEvent<
  Params extends EventParams = EventParams,
  Context extends ReadonlyMap<any, any> = ReadonlyMap<any, any>
> = {
  workflowId: string;
  executionId: string;
  params: Params;
  context: Context;
};

export type MiddlewareEvent<
  Params extends EventParams = EventParams,
  Context extends FreezableMap<any, any> = FreezableMap<any, any>
> = Omit<ExecutionEvent<Params>, "executionId" | "context"> & {
  executionId?: string;
  context: Context;
};

export type MiddlewareNext = () => Promise<Response>;

export type MiddlewareFunction = (
  req: Request,
  event: MiddlewareEvent,
  next: MiddlewareNext,
  logger: Logger
) => Promise<Response>;
```

`@notation/durable/runtime` also exports `ReadOnlyMap`, `FreezableMap`, and `errorWithOriginalStack(error, fn)`.

## Adapter state helpers

Runtime adapters use these pure transition, encoding, and validation helpers so every backend enforces identical lease, wake, list, effect, join, and store semantics.

### Execution state and constants

| Constant | Value |
| :--- | :--- |
| `DEFAULT_LEASE_DURATION` | `30000` |
| `REDELIVERY_DELAY_BASE` | `1000` |
| `REDELIVERY_DELAY_MAX` | `300000` |
| `DEFAULT_EXECUTION_LIST_LIMIT` | `50` |
| `MAX_EXECUTION_LIST_LIMIT` | `1000` |

```ts [@notation/durable/runtime]
export type ExecutionState = {
  executionId: string;
  workflowId: string;
  params: string;
  context: string;
  status: Exclude<ExecutionStatus, "running">;
  outcome: string | null;
  admittedAt: number;
  updatedAt: number;
  dueAt: number | null;
  claimedDueAt: number | null;
  leaseToken: number;
  leaseExpiresAt: number | null;
  unacknowledgedDeliveryCount: number;
  redeliverAt: number | null;
  parent: ExecutionParent | null;
  cancellation: ExecutionCancellationState | null;
};

export type ExecutionCancellationState = {
  reason: string | null;
  source: string | null;
  forced: boolean;
  requestedAt: number;
  deliveredAt: number | null;
};

export type CancellationRequest = {
  reason?: string;
  source?: string;
  force: boolean;
};

export type StartRequestState = {
  callerExecutionId: string;
  stepKey: string;
  workflowId: string;
  params: string | null;
  detached: boolean;
  executionId: string;
};

export type PreparedExecutionStart = {
  callerExecutionId: string;
  stepKey: string;
  workflowId: string;
  params: string | null;
  detached: boolean;
};

export type PreparedExecutionListRequest = {
  workflowId?: string;
  status?: ExecutionStatus;
  beforeSequence?: number;
  limit: number;
};

export type SequencedExecutionState = {
  state: ExecutionState;
  admissionSequence: number;
};
```

| Helper | Purpose |
| :--- | :--- |
| `prepareExecutionAdmission` | Validates an admission request and encodes `params` and `context` |
| `matchExecutionAdmission` | Resolves an admission whose `executionId` already exists |
| `createExecutionState` | Builds initial `admitted` state due immediately at `now` |
| `prepareExecutionStart` | Validates a child workflow `start` request and encodes `params` |
| `matchStartRequest` | Resolves a repeated `start` request against a stored `StartRequestState` |
| `createStartedExecution` | Builds child target state and `StartRequestState` with inherited context |
| `cancellationForStartedChild` | Computes inherited cancellation for a newly started non-detached child |
| `claimExecution` | Leases a claimable state with an incremented `leaseToken` |
| `isExecutionClaimable` | Checks whether the next claim timestamp is at or before `now` |
| `nextClaimAt` | Computes earliest claim timestamp or `null` when nothing is pending |
| `renewExecutionLease` | Extends `leaseExpiresAt` if the lease token matches the stored lease |
| `releaseExecutionLease` | Ends a lease unacknowledged and schedules `redeliverAt` backoff |
| `acknowledgeExecution` | Records outcome, clears covered wakes, and ends the lease |
| `isLeaseCurrent` | Checks whether `lease` matches the held `leaseToken` |
| `requestExecutionWake` | Merges an external wake into `dueAt` on non-terminal executions |
| `prepareExecutionCancel` | Validates `{ executionId, reason?, force? }` before touching storage |
| `requestExecutionCancellation` | Applies graceful or forced cancellation to `ExecutionState` |
| `propagatedCancellation` | Derives child `CancellationRequest` from a cancelled parent |
| `markCancellationDelivered` | Sets `cancellation.deliveredAt` during fenced cancellation delivery |
| `toExecutionCancellation` | Converts `ExecutionCancellationState` to `ExecutionCancellation` |
| `prepareExecutionListRequest` | Validates list filters, cursor, and limit |
| `matchesExecutionListRequest` | Checks whether an `ExecutionState` matches `PreparedExecutionListRequest` |
| `toExecutionListPage` | Builds `{ executions, cursor? }` from `limit + 1` sequenced rows |
| `executionStatusAt` | Returns `"running"` while `leaseExpiresAt > now`, else `state.status` |
| `unacknowledgedDeliveryCountAt` | Counts consecutive unacknowledged deliveries including an expired lease |
| `redeliveryDelay` | Computes exponential backoff between `1000` and `300000` ms |
| `toExecutionRecord` | Decodes `ExecutionState` into an `ExecutionRecord` |
| `executionNotFound` | Builds `{ status: "rejected", code: "EXECUTION_NOT_FOUND", message }` |
| `executionIdConflict` | Builds `{ status: "rejected", code: "EXECUTION_ID_CONFLICT", message }` |
| `encodeExecutionEvent` | Encodes `{ params, context }` or throws `ExecutionEventEncodingError` |
| `decodeExecutionEvent` | Decodes stored `{ params, context }` strings |
| `encodeExecutionOutcome` | Encodes a terminal `ExecutionOutcome` |
| `decodeExecutionOutcome` | Decodes a stored outcome string into `ExecutionOutcome` |

### Effect state helpers

```ts [@notation/durable/runtime]
export type EffectOutcomeStatus = EffectOutcome["status"];

export type EffectConflictState = {
  status: EffectOutcomeStatus;
  result: string;
  conflictingStatus: EffectOutcomeStatus;
  conflictingResult: string;
  reportedAt: number;
};

export type EffectState = {
  effectId: string;
  operation: string;
  input: string;
  status: "pending" | EffectOutcomeStatus;
  result?: string;
  conflicts: EffectConflictState[];
};

export type PreparedEffectBegin = {
  effectId: string;
  operation: string;
  input: string;
  executionId: string;
  stepKey: string;
};

export type PreparedEffectSettle = {
  effectId: string;
  operation: string;
  status: EffectOutcomeStatus;
  result: string;
};
```

| Helper | Purpose |
| :--- | :--- |
| `validateEffectRequest` | Validates `effectId`, `operation`, and durable plain-object `input` |
| `prepareEffectBegin` | Validates and canonically encodes a `begin` request |
| `prepareEffectSettle` | Validates and canonically encodes a `settle` request |
| `prepareEffectWatch` | Validates `effectId`, `executionId`, and `stepKey` on a `watch` request |
| `createEffectState` | Creates a `pending` `EffectState` |
| `matchEffectBegin` | Matches an incoming `begin` against an existing `EffectState` |
| `settleEffect` | Applies a settlement, records conflicts, and returns `{ state, result, wake }` |
| `toEffectRecord` | Decodes `EffectState` into an `EffectRecord` |
| `effectNotFound` | Builds an `EffectClientInvariantError` for an unknown `effectId` |

### Join and store helpers

| Helper | Purpose |
| :--- | :--- |
| `readJoinTarget` | Returns `terminal`, `waiting`, or `not-found` from target `ExecutionState` |
| `validateStoreState` | Runs the store's Standard Schema and validates durable output |
| `encodeStoreValue` | Encodes store state canonically or throws `Error` |
| `decodeStoreValue` | Decodes canonical store state string |
| `cloneStoreState` | Clones store state as `durableValueCodec` reads it back |
| `diffStorePaths` | Computes changed `StorePath[]` between two states |
| `storePathsIntersect` | Checks prefix intersection between `readPaths` and `writePaths` |
| `trackStoreSelector` | Runs a selector under a read-tracking proxy to capture `readPaths` |
| `trackStoreUpdater` | Wraps a draft in a write-tracking proxy and returns `TrackedStoreUpdater<T>` |
| `unwrapTrackedValue` | Unwraps a tracking proxy one level back to its target |
| `isStoreSelectorMatch` | Returns true when a selector result is neither `undefined`, `null`, nor `false` |
| `assertSynchronousClaim` | Throws `Error` if a store take `claim` callback returns a Promise |
| `assertSynchronousUpdater` | Throws `Error` if a store `updater` callback returns a Promise |

`CasStoreClient` already calls the store tracking and validation helpers internally; custom adapters extending `CasStoreClient` do not need to call them directly.

## Durable value codec

```ts [@notation/durable/runtime]
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

export type DurableValueEncoding =
  | { status: "encoded"; canonical: string }
  | { status: "invalid"; message: string };

export type DurableDecoding =
  | { status: "decoded"; value: DurableValue }
  | { status: "invalid"; message: string };

export interface DurableValueCodec {
  encode(value: unknown): DurableValueEncoding;
  decode(canonical: string): DurableDecoding;
}

export const durableValueCodec: DurableValueCodec;

export function decodeParsedValue(parsed: unknown, path?: string): DurableValue;
export function encodeThrownValue(value: unknown): string;

export class DurableValueError extends Error {}
```

The runtime subpath also exports `Durable<T>` and `IfDurable<T, Otherwise>`. `encodeThrownValue` serializes durable thrown values directly or coerces non-durable throws into plain `Error` instances.

## Errors

`@notation/durable/runtime` exports the workflow, SDK, and runtime error classes along with `registerErrorClass` and `WORKFLOW_RESULT_STEP_KEY`:

```ts [@notation/durable/runtime]
export const WORKFLOW_RESULT_STEP_KEY = "$$workflow-result$$";

export type ErrorClass = abstract new (...args: any[]) => Error;
export type WorkflowValueErrorCode = "INVALID_STEP_VALUE" | "STEP_SCHEMA_INVALID";

export function registerErrorClass(name: string, errorClass: ErrorClass): void;
```

| Error class | Role |
| :--- | :--- |
| `InfrastructureError` | Wraps delivery-aborting runtime errors and copies string `code` from `cause` |
| `LeaseLostError` | Abort reason when a fenced write or renewal returns `lost` |
| `StepConflictError` | Raised when a step attempt already exists with a different value |
| `ExecutionEventEncodingError` | Thrown by `encodeExecutionEvent` for non-durable `params` or `context` |
| `ExecutionAdmissionError` | Admission rejection with `ExecutionAdmissionErrorCode` |
| `ExecutionListRequestError` | Invalid filter, limit, or cursor passed to `list` |
| `ExecutionCancelRequestError` | Invalid arguments passed to `ExecutionControl.cancel` |
| `WorkflowStartError` | Raised in workflow code by `step.start` on rejected start parameters or mismatch |
| `WorkflowJoinError` | Raised in workflow code by `step.join` or `step.call` |
| `WorkflowCancelledError` | Thrown into workflow code when a delivery carries a cancellation request |
| `WorkflowEffectError` | Raised in workflow code by `step.effect` on invalid, mismatched, or unknown effects |
| `EffectDeliveryError` | Infrastructure cause when `step.effect` fails without recording a step |
| `EffectClientInvariantError` | Thrown by `EffectClient` on invalid requests or broken storage invariants |
| `WorkflowValueError` | Raised when a step or workflow produces a non-durable or schema-invalid value |
| `WorkflowRaceError` | Raised by `step.race` when all branches fail or when no branches are given |
| `StoreValidationError` | Thrown when store state fails its Standard Schema validation |
| `WorkerExitedError` | Raised by an invoker when a worker exits before replying |
| `DurableValueError` | Thrown by `decodeParsedValue` on malformed codec encodings |

For the complete error catalog and custom class registration guide, see [Errors](./errors.md).
