import { EventEmitter } from "node:events";
import type { Logger } from "pino";
import type {
  HeapRecord,
  Runtime,
  WorkflowInvoker,
  WorkflowRouter,
  WorkflowRunner,
} from "@durable/core";
import { createIsolatedDeliver, runDelivery } from "../packages/cloudflare/src/delivery";

type MemoryHeap = {
  cache: Record<string, HeapRecord[]>;
  getKey(params: { executionId: string; stepKey: string }): string;
};
type SqliteHeap = { readSteps(executionId: string): Iterable<[string, HeapRecord]> };

function* memorySteps(heap: MemoryHeap, executionId: string): Iterable<[string, HeapRecord]> {
  const prefix = heap.getKey({ executionId, stepKey: "" });
  for (const [key, attempts] of Object.entries(heap.cache)) {
    if (key.startsWith(prefix)) yield [key.slice(prefix.length), structuredClone(attempts[0])];
  }
}

/**
 * Stands in for `@durable/test-invoker` when `DURABLE_TEST_DELIVERIES` is
 * `isolated` (see vitest.config.ts). Each delivery then runs the way a
 * Cloudflare `WorkflowObject` runs it in another instance: the request and
 * the reply are encoded and cloned, and the workflow reaches the runner's
 * runtime through the remote runtime. So every suite that uses the test
 * invoker also checks that path.
 *
 * A delivery in another instance differs from one in the same process in
 * three ways, and the tests that depend on them are skipped in this mode:
 *
 * - Its signal never aborts. The object aborts the instance instead.
 * - It checks a start or effect request before sending it, so an invalid
 *   request does not reach the runtime's client.
 * - It calls the store client's compare-and-swap methods, not `updateStore`.
 */
export const ISOLATED_DELIVERIES = process.env.DURABLE_TEST_DELIVERIES === "isolated";

/** See the differences listed above `ISOLATED_DELIVERIES`. */
export function createWorkflowInvoker(params: {
  logger: Logger;
  runner: WorkflowRunner<any>;
}): WorkflowInvoker {
  const { logger, runner } = params;
  const { runtime, router } = runner as unknown as {
    runtime?: Runtime;
    router: WorkflowRouter;
  };
  // A suite that passes its own object in place of a runner tests the
  // dispatcher, not a delivery.
  if (!runtime) {
    return {
      workflowEndEmitter: new EventEmitter(),
      deliver: (event, options) => runner.run(event, { ...options, logger }),
    };
  }
  const heap = runtime.heap as unknown as Partial<MemoryHeap & SqliteHeap>;
  const instances = new Map<number, { aborted: boolean }>();
  return {
    workflowEndEmitter: new EventEmitter(),
    deliver: createIsolatedDeliver({
      runtime,
      readSteps: heap.readSteps
        ? (executionId) => heap.readSteps!(executionId)
        : heap.cache
          ? (executionId) => memorySteps(heap as MemoryHeap, executionId)
          : undefined,
      getRunner: (slot) => ({
        durableRunDelivery: async (request, host) => {
          const instance = { aborted: false };
          instances.set(slot, instance);
          // An aborted instance runs no more code. Here its step function
          // cannot be stopped, so its calls to the runtime reject instead.
          const call: typeof host.call = async (...args) => {
            if (instance.aborted) throw new Error("The instance was aborted");
            return host.call(...args);
          };
          const reply = await runDelivery(
            structuredClone(request),
            { ...host, call },
            { router, logger, clock: runtime.clock, onOrphaned() {} }
          );
          instances.delete(slot);
          return structuredClone(reply);
        },
        async durableStopDelivery() {
          const instance = instances.get(slot);
          if (instance) instance.aborted = true;
        },
      }),
    }),
  };
}
