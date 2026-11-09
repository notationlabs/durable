import type {
  TriggerAck,
  WorkflowRouter,
  TriggerEvent,
  WorkflowGeneratorReturnType,
  EventParamsOf,
} from "@durable/core";
import type { Logger } from "pino";
import pino from "pino";
import { ExecutionDispatcher, WorkflowRunner } from "@durable/core";
import { createWorkflowInvoker } from "@durable/test-invoker";
import { MemoryRuntime } from "@durable/test-runtime";
import { createLocalSdk } from "@durable/sdk";

/**
 * Builds a local SDK over a fresh in-memory runtime. Executions run in the
 * calling process, and the dispatcher starts on the first trigger.
 */
export function createTestSdk<W extends WorkflowRouter>(
  router: W,
  options?: {
    /** @default a pino logger at level "error" */
    logger?: Logger;
    /** Every client of the runtime reads time from this clock. @default Date.now */
    clock?: () => number;
  }
) {
  const logger = options?.logger ?? pino({ level: "error" });
  const runtime = new MemoryRuntime({ clock: options?.clock });
  const runner = new WorkflowRunner(runtime, { router, logger });
  const invoker = createWorkflowInvoker({ runner, logger });

  // The dispatcher's timers do not keep the test process alive, so a test
  // does not need to stop it.
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    pollInterval: 1,
    keepAlive: false,
  });

  const sdk = createLocalSdk<W>(runtime, { invoker, pollInterval: 1 });

  return {
    ...sdk,
    async trigger<
      K extends keyof W & string,
      Params extends EventParamsOf<W[K]> = EventParamsOf<W[K]>
    >(event: TriggerEvent<K, Params>): Promise<TriggerAck> {
      dispatcher.start();
      return sdk.trigger(event as TriggerEvent<K, EventParamsOf<W[K]>>);
    },
    async triggerAndWait<
      K extends keyof W & string,
      Params extends EventParamsOf<W[K]> = EventParamsOf<W[K]>
    >(event: TriggerEvent<K, Params>): Promise<WorkflowGeneratorReturnType<W[K]>> {
      dispatcher.start();
      return sdk.triggerAndWait(event as TriggerEvent<K, EventParamsOf<W[K]>>);
    },
    runtime,
    dispatcher,
  };
}

export function createTestSdkFactory(params?: {
  logger?: Logger;
  /** Passed to the memory runtime, e.g. a fixed clock. */
  runtime?: ConstructorParameters<typeof MemoryRuntime>[0];
}) {
  return <W extends WorkflowRouter>(workflowRouter: W) => {
    const sdk = createTestSdk(workflowRouter, {
      logger: params?.logger,
      clock: params?.runtime?.clock,
    });
    const { runtime } = sdk;
    return {
      ...sdk,
      store: runtime.stores.store.bind(runtime.stores),
      storeClient: runtime.stores,
      heapClient: runtime.heap,
    };
  };
}
