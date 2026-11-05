import type { ExecutionLease, LeaseLost } from "./execution";

export type HeapRecord = {
  stepResponseJson: string;
  meta: { attempt: number; done: boolean };
};

export abstract class HeapClient {
  abstract readStep(params: {
    executionId: string;
    stepKey: string;
  }): Promise<HeapRecord | null>;

  /**
   * Records one attempt of a step, fenced by `lease`. The token check and the
   * write are atomic: if the lease is not current, nothing changes and the
   * write returns `lost`. A `done` record also deletes the step's store,
   * effect and join waiters in the same write.
   *
   * Records are keyed by execution, step key and attempt, and never change
   * once written. Rewriting an existing record succeeds without changing it,
   * and resolves with the record as stored, which may hold a different value
   * from the one written: callers compare it to detect a conflicting write.
   */
  abstract writeStep(params: {
    lease: ExecutionLease;
    stepKey: string;
    stepAttempt: number;
    done: boolean;
    stepResponseJson: string;
  }): Promise<HeapRecord | LeaseLost>;

  /**
   * Records the delivery of the execution's cancellation request, fenced by
   * `lease`, all-or-nothing: writes each step's `done` record, deletes those
   * steps' store, effect and join waiters, and sets the request's
   * `deliveredAt`. Throws if the execution has no request recorded.
   *
   * A record that already exists at its attempt is kept, as in `writeStep`,
   * and the results list the records as stored, in the order of `steps`.
   */
  abstract recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly {
      stepKey: string;
      stepAttempt: number;
      stepResponseJson: string;
    }[];
  }): Promise<HeapRecord[] | LeaseLost>;
}
