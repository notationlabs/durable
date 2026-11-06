import type { EffectClient } from "./effect";
import type { ExecutionClient } from "./execution";
import type { HeapClient } from "./heap";
import type { JoinClient } from "./join";
import type { StoreClient } from "./store";

export interface RuntimeOptions {
  /** Every client of the runtime reads time from this clock. Defaults to `Date.now`. */
  clock?: () => number;
  /**
   * Parks an execution whose replay reaches a keyless step that has no
   * record, with divergence code `STEP_KEY_REQUIRED`. A keyless step that
   * already has a record still replays. The runner reads it from the runtime
   * it was constructed with, which under the worker invoker is the worker's.
   * Off by default; it will default to on in the next major version.
   */
  requireStepKeys?: boolean;
}

/**
 * A runtime is defined by the guarantees its clients keep, not by how many
 * databases it uses:
 *
 * - Where one write must accompany another, such as the wakes for a store
 *   commit, the second is durable once the first commits and is applied
 *   eventually (in one transaction or through an outbox).
 * - A store or effect waiter is deleted atomically with its step's result.
 * - An effect's settlement and the wakes of its waiters are one atomic write.
 * - A terminal acknowledgement and the wakes of its join waiters are one
 *   atomic write. A join waiter is deleted atomically with its step's result.
 * - Every client reads time from the runtime's clock.
 */
export abstract class Runtime {
  /** The clock every client, and every step's wake time, reads. */
  abstract readonly clock: () => number;
  /** As in `RuntimeOptions.requireStepKeys`. */
  readonly requireStepKeys: boolean = false;
  abstract readonly executions: ExecutionClient;
  abstract readonly effects: EffectClient;
  abstract readonly heap: HeapClient;
  abstract readonly joins: JoinClient;
  abstract readonly stores: StoreClient;
}
