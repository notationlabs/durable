import { Runtime, type RuntimeOptions } from "@yieldstar/core";
import { MemoryEffectClient } from "./memory-effect";
import { MemoryExecutionClient } from "./memory-execution";
import { MemoryHeapClient } from "./memory-heap";
import { MemoryJoinClient } from "./memory-join";
import { MemoryStoreClient } from "./memory-store";

export class MemoryRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: MemoryExecutionClient;
  readonly effects: MemoryEffectClient;
  readonly heap: MemoryHeapClient;
  readonly joins: MemoryJoinClient;
  readonly stores: MemoryStoreClient;

  constructor(options?: RuntimeOptions) {
    super();
    // Read Date.now lazily so fake timers installed later still apply.
    this.clock = options?.clock ?? (() => Date.now());
    this.executions = new MemoryExecutionClient({
      clock: this.clock,
      onTerminal: (executionId) => this.joins.wakeWaiters(executionId),
      onForceCancelled: (executionId) => {
        this.stores.deleteWaitersForExecution(executionId);
        this.effects.deleteWaitersForExecution(executionId);
        this.joins.deleteWaitersForExecution(executionId);
      },
    });
    this.joins = new MemoryJoinClient({ executions: this.executions });
    this.stores = new MemoryStoreClient({ executions: this.executions });
    this.effects = new MemoryEffectClient({
      executions: this.executions,
      clock: this.clock,
    });
    this.heap = new MemoryHeapClient({
      isLeaseCurrent: (lease) => this.executions.isLeaseCurrent(lease),
      onStepDone: (step) => {
        this.stores.deleteWaitersForStep(step);
        this.effects.deleteWaitersForStep(step);
        this.joins.deleteWaitersForStep(step);
      },
      markCancellationDelivered: (executionId) =>
        this.executions.markCancellationDelivered(executionId),
    });
  }
}
