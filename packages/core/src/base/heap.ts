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
   * effect and join waiters in the same write, and so do the steps named in
   * `deleteWaitersOf`.
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
    /**
     * Other steps whose waiters a `done` record deletes in the same write,
     * such as the steps of branches that a `step.race` abandons. Ignored when
     * the record is not done or already exists.
     */
    deleteWaitersOf?: readonly string[];
  }): Promise<HeapRecord | LeaseLost>;

  /**
   * Records the delivery of the execution's cancellation request, fenced by
   * `lease`, all-or-nothing: writes each step's `done` record, deletes those
   * steps' store, effect and join waiters, and sets the request's
   * `deliveredAt`. Throws if the execution has no request recorded.
   *
   * The results list the records as stored, in the order of `steps`. A
   * record that already exists at its attempt and equals the requested one
   * (the same JSON, and done) counts as written, as in `writeStep`. When an
   * existing record differs, nothing changes: no records are written, no
   * waiters are deleted and `deliveredAt` is not set. The results then hold
   * each step's existing record, or `null` for a step without one, and callers
   * compare them to detect the conflict.
   */
  abstract recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly {
      stepKey: string;
      stepAttempt: number;
      stepResponseJson: string;
    }[];
  }): Promise<(HeapRecord | null)[] | LeaseLost>;
}
