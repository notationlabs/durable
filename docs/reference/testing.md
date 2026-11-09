# @notation/durable/testing

`@notation/durable/testing` runs workflows inside the test process, with records in memory. See [Testing](../manual/testing.md) for how to use it.

```ts
import { createTestSdk } from "@notation/durable/testing";
```

## `createTestSdk(router, options?)`

Returns a [local SDK](../manual/sdk-local.md) over a new `MemoryRuntime`, with two more properties:

| Property | Holds |
| --- | --- |
| `runtime` | The `MemoryRuntime` |
| `dispatcher` | The `ExecutionDispatcher`. It starts on the first `trigger` or `triggerAndWait` |

| Option | Default | Does |
| --- | --- | --- |
| `clock` | `Date.now` | The clock of the runtime and of the steps |
| `logger` | A pino logger at level `"error"` | The logger of the dispatcher and the runner |

The dispatcher and the SDK read the records every millisecond. The dispatcher's timers do not keep the process alive.

## `MemoryRuntime`

A runtime that keeps its records in memory, in the current process. `new MemoryRuntime({ clock? })` takes the same options as [`Runtime`](./runtime.md#runtime). Two runtimes share no records.

## `createInProcessInvoker({ runner, logger })`

Returns an invoker that runs each delivery with `runner`, a `WorkflowRunner`, in the current process. An `ExecutionDispatcher` takes it as its `invoker`. See [Building a test setup by hand](../manual/testing.md#building-a-test-setup-by-hand).

**The invoker cannot stop a step function** when the delivery's lease is lost, because the function runs in the dispatcher's process. See [Delivery guarantees](../runtime-authors/delivery-guarantees.md).
