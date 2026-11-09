# @notation/durable-conformance

`@notation/durable-conformance` exports Vitest suites that check a runtime. See [Conformance](../runtime-authors/conformance.md) for how to use them.

```sh
@pm.dev("@notation/durable-conformance vitest")
```

Its peer dependencies are `@notation/durable`, with the same minor version, and Vitest 4.1.12 or newer.

## Suites

Each function registers its tests with Vitest in the file that calls it. Each takes `{ name, create }`, where `name` names the runtime in the test names, and `create` returns a harness for one test.

| Function | Harness type | `create` receives | Failure hooks |
| --- | --- | --- | --- |
| `registerExecutionClientConformance` | `ExecutionConformanceHarness` | `{ clock }` | `failStartCommits` |
| `registerStoreClientConformance` | `StoreConformanceHarness` | Nothing | None |
| `registerEffectClientConformance` | `EffectConformanceHarness` | `{ clock }` | `failEffectWatches` |
| `registerJoinClientConformance` | `JoinConformanceHarness` | `{ clock }` | `failExecutionWrites` |
| `registerCancellationClientConformance` | `CancellationConformanceHarness` | `{ clock }` | `failExecutionWrites` |

Every harness has `runtime`, and may have `createPeer`, `restart` and `dispose`. See [Harness](../runtime-authors/conformance.md#harness) and [Failure hooks](../runtime-authors/conformance.md#failure-hooks).

## Types

| Type | Is |
| --- | --- |
| `ExecutionConformanceOptions`, `StoreConformanceOptions`, `EffectConformanceOptions`, `JoinConformanceOptions`, `CancellationConformanceOptions` | The argument of each function |
| `ExecutionConformanceHarnessOptions`, `EffectConformanceHarnessOptions`, `JoinConformanceHarnessOptions`, `CancellationConformanceHarnessOptions` | What `create` receives: `{ clock }` |
| `StoreConformanceFactory` | The type of the store suite's `create` |
