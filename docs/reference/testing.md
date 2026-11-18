# @notation/durable/testing

`@notation/durable/testing` exports an in-memory test SDK, an in-memory runtime, and an in-process workflow invoker for executing and inspecting workflows without worker processes or database files.

## `createTestSdk`

`createTestSdk` builds a local SDK wired to a fresh `MemoryRuntime`, a `WorkflowRunner`, an in-process invoker, and an `ExecutionDispatcher` whose timers do not keep the test process alive:

```ts [@notation/durable/testing]
import type { Logger } from "pino";

export function createTestSdk<W extends WorkflowRouter>(
  router: W,
  options?: {
    logger?: Logger;
    clock?: () => number;
  }
): {
  trigger<
    K extends keyof W & string,
    Params extends EventParamsOf<W[K]> = EventParamsOf<W[K]>,
  >(event: TriggerEvent<K, Params>): Promise<TriggerAck>;
  triggerAndWait<
    K extends keyof W & string,
    Params extends EventParamsOf<W[K]> = EventParamsOf<W[K]>,
  >(event: TriggerEvent<K, Params>): Promise<WorkflowGeneratorReturnType<W[K]>>;
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
  runtime: MemoryRuntime;
  dispatcher: ExecutionDispatcher;
};
```

| Option | Description |
| :--- | :--- |
| `logger` | Pino logger used by the runner, invoker, and dispatcher |
| `clock` | Clock function read by every runtime client and step wake calculation |

`logger` defaults to an error-level Pino logger, and `clock` evaluates `Date.now()` lazily on each read so fake timers installed after `createTestSdk` still take effect.

`createTestSdk` sets both the dispatcher and local SDK poll interval to 1 ms and sets `keepAlive` to false so tests can exit cleanly without stopping timers. Calling `trigger` or `triggerAndWait` starts the dispatcher automatically if idle.

| Member | Description |
| :--- | :--- |
| `trigger` | Starts the dispatcher if needed and admits an execution |
| `triggerAndWait` | Starts the dispatcher if needed, admits an execution, and awaits its outcome |
| `poll` | Reads the current `ExecutionView` from `runtime.executions` |
| `wait` | Waits for a terminal execution outcome or timeout |
| `list` | Pages through execution views newest first |
| `cancel` | Requests graceful or forced cancellation |
| `runtime` | The `MemoryRuntime` instance backing this SDK |
| `dispatcher` | The `ExecutionDispatcher` instance polling `runtime.executions` |

```ts [test/order.test.ts]
import { workflow, createWorkflowRouter } from "@notation/durable";
import { createTestSdk } from "@notation/durable/testing";

const orderRouter = createWorkflowRouter({
  fulfill: workflow<{ orderId: string }>(async function* (step, event) {
    const charge = yield* step.run("charge", async () => ({
      orderId: event.params.orderId,
      paid: true,
    }));
    return charge;
  }),
});

const sdk = createTestSdk(orderRouter);
const result = await sdk.triggerAndWait({
  workflowId: "fulfill",
  params: { orderId: "ord-1" },
});
await sdk.dispatcher.stop();
```

See [Testing](../core/testing.md) for patterns covering fake clocks, cancellation, and external store inspection.

## `MemoryRuntime`

`MemoryRuntime` extends `Runtime` with synchronous in-memory implementations of all five runtime clients:

```ts [@notation/durable/testing]
export class MemoryRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: MemoryExecutionClient;
  readonly effects: MemoryEffectClient;
  readonly heap: MemoryHeapClient;
  readonly joins: MemoryJoinClient;
  readonly stores: MemoryStoreClient;

  constructor(options?: RuntimeOptions);
}
```

`RuntimeOptions` accepts `{ clock?: () => number }`, defaulting to `() => Date.now()`.

| Property | Description |
| :--- | :--- |
| `clock` | Clock function returning current epoch milliseconds |
| `executions` | In-memory `ExecutionClient` tracking records, leases, and wakes |
| `effects` | In-memory `EffectClient` tracking external effects and conflict reports |
| `heap` | In-memory `HeapClient` storing step records and deleting waiters on completion |
| `joins` | In-memory `JoinClient` tracking execution join waiters |
| `stores` | In-memory `StoreClient` supporting `store`, mutations, and store waiters |

All operations on `MemoryRuntime` clients execute their state transitions atomically in memory. Terminal acknowledgements wake join waiters immediately, forced cancellations remove all store, effect, and join waiters for the execution, and completed step writes delete the corresponding step waiters.

In tests, `runtime.stores.store` returns a handle for seeding or reading state, `runtime.effects.getEffect` inspects recorded effects, and `runtime.executions.get` reads the full `ExecutionRecord`.

## `createInProcessInvoker`

`createInProcessInvoker` creates a `WorkflowInvoker` that executes workflow deliveries in the calling process through a `WorkflowRunner`:

```ts [@notation/durable/testing]
import type { Logger } from "pino";

export function createInProcessInvoker(params: {
  logger: Logger;
  runner: WorkflowRunner<any>;
}): WorkflowInvoker;
```

| Parameter | Description |
| :--- | :--- |
| `logger` | Pino logger passed into `runner.run` and used to log delivery starts |
| `runner` | `WorkflowRunner` instance executing workflow generators |

`createInProcessInvoker` wires `workflowEndEmitter` for fast-path wake notifications and executes deliveries directly through `runner.run`.

Because step functions run in the dispatcher's own JavaScript runtime without child process isolation, `createInProcessInvoker` cannot interrupt a synchronous or hung step function when `options.signal` aborts. Instead, `ExecutionDispatcher` abandons the delivery after `abortGracePeriod` elapses; see [Leases & Delivery](../authoring/delivery-guarantees.md) for lease abandonment behavior.

Assemble `MemoryRuntime` and `createInProcessInvoker` directly when a test needs custom `ExecutionDispatcher` or `createLocalSdk` options:

```ts [test/custom-dispatcher.test.ts]
import pino from "pino";
import {
  ExecutionDispatcher,
  WorkflowRunner,
  createLocalSdk,
} from "@notation/durable";
import {
  MemoryRuntime,
  createInProcessInvoker,
} from "@notation/durable/testing";

const logger = pino({ level: "silent" });
const runtime = new MemoryRuntime();
const runner = new WorkflowRunner(runtime, { router: orderRouter, logger });
const invoker = createInProcessInvoker({ runner, logger });

const dispatcher = new ExecutionDispatcher(runtime, {
  invoker,
  logger,
  concurrency: 2,
  leaseDuration: 5_000,
  renewInterval: 1_000,
  pollInterval: 1,
  keepAlive: false,
});

const sdk = createLocalSdk(runtime, { invoker, pollInterval: 1 });
dispatcher.start();
```
