# Runtimes

A runtime is the object that holds everything Durable records: executions, step records, stores, effects, and the waiters that wake an execution. Durable ships two:

| Runtime | Keeps its records in | Used by |
| --- | --- | --- |
| `SqliteRuntime` | One SQLite file | A long-running server. See [Running a server](../manual/running-a-server.md) |
| `MemoryRuntime` | The memory of one process | Tests. See [Testing](../manual/testing.md) |

These pages are for someone who writes another runtime, for example one that keeps its records in Postgres so that servers on several hosts can share them.

## Runtime class

A runtime extends `Runtime` from `@notation/durable/runtime`. It has a clock and five clients:

```ts
abstract class Runtime {
  abstract readonly clock: () => number;
  abstract readonly executions: ExecutionClient;
  abstract readonly heap: HeapClient;
  abstract readonly stores: StoreClient;
  abstract readonly effects: EffectClient;
  abstract readonly joins: JoinClient;
}
```

| Client | Holds | Page |
| --- | --- | --- |
| `executions` | Execution records, their leases and when each is next due | [Execution client](./execution-client.md) |
| `heap` | Step records | [Heap client](./heap-client.md) |
| `stores` | Store state, and the waiters of `when` and `take` | [Store client](./store-client.md) |
| `effects` | Effect records, and the waiters of `step.effect` | [Effect client](./effect-client.md) |
| `joins` | The waiters of `step.join` and `step.call` | [Join client](./join-client.md) |

`clock` returns the current time in milliseconds. Every client reads time from it, and so do delays and retries. Tests pass their own clock to control time.

## Who calls the runtime

Several parts of Durable share one runtime, and each calls a different set of methods:

| Caller | Process | Calls |
| --- | --- | --- |
| The local SDK and the HTTP routes | Any process with the runtime | `executions.admit`, `get`, `list` and `cancel` |
| `ExecutionDispatcher` | The server | `executions.claimDue`, `renew`, `release` and `acknowledge` |
| `WorkflowRunner` | The worker | `heap`, `stores`, `effects`, `joins` and `executions.start` |
| Application code with a [store handle](../manual/store-external.md) | Any process with the runtime | `stores` |

The worker builds its own runtime over the same records. So a runtime must work when several runtime objects, in several processes, use the same database at once.

## Writes that go together

Some writes in one client need a write in another client. When a store update satisfies a `when`, the execution that waits on it must become due. If the update is recorded and the wake is lost, the execution never runs again.

**A runtime must make these writes together, so that a crash leaves both or neither:**

| When | The same write also |
| --- | --- |
| A store update satisfies a waiter | Makes the waiter's execution due |
| An effect is settled | Makes every execution that watches it due |
| An execution ends | Makes every execution that joins it due |
| A step's `done` record is written | Deletes the step's store, effect and join waiters |

`SqliteRuntime` keeps every record in one database, so each of these is one transaction. A runtime that spreads its clients across several databases must still keep these guarantees, for example with an outbox that applies the second write after the first commits.

## State helpers

Durable decides what each operation does, and the runtime decides how to store the result. `@notation/durable/runtime` exports a function for each decision, such as `claimExecution` and `acknowledgeExecution`. Each takes the stored state and returns the new state and the result. The runtime reads the state, calls the function, and writes the new state in one transaction.

`SqliteJoinClient` shows the pattern. `readJoinTarget` decides the answer from the target's stored state, and the transaction makes the read and the registration one step:

```ts
async join(waiter: JoinWaiter): Promise<JoinResult> {
  return runTransaction(this.db, () => {
    const result = readJoinTarget(this.executionsDao.getState(waiter.targetExecutionId));
    if (result.status === "waiting") this.joinWaitersDao.upsertWaiter(waiter);
    return result;
  });
}
```

Each client's page lists its helpers. Runtimes that use them behave the same way, because the same code makes every decision.

## Writing a runtime

`MemoryRuntime` is about 1,000 lines, in `packages/test-runtime/src` of the repository. It is the shortest complete runtime, so start from it. `SqliteRuntime`, in `packages/sqlite-runtime/src`, shows the same clients over a database.

Then check the runtime with the suites in `@notation/durable-conformance`. See [Conformance](./conformance.md).

[Delivery guarantees](./delivery-guarantees.md) explains leases and tokens, which the execution client and the heap client rely on.
