import type {
  ExecutionLease,
  HeapClient,
  HeapRecord,
  HeapStep,
  LeaseLost,
} from "@yieldstar/core";

export class MemoryHeapClient implements HeapClient {
  /** Each step's records, latest attempt first. */
  cache: Record<string, HeapRecord[]> = {};
  private isLeaseCurrent: (lease: ExecutionLease) => boolean;
  private onStepDone?: (step: { executionId: string; stepKey: string }) => void;
  private markCancellationDelivered?: (executionId: string) => void;
  private recordWorkflowVersion?: (executionId: string, workflowVersion: number | undefined) => void;

  constructor(params: {
    /**
     * Checks a lease synchronously, so the token check and the write happen
     * in one critical section.
     */
    isLeaseCurrent: (lease: ExecutionLease) => boolean;
    /**
     * Called synchronously when a step's result is recorded, so the runtime
     * deletes the step's store, effect and join waiters in the same write.
     */
    onStepDone?: (step: { executionId: string; stepKey: string }) => void;
    /**
     * Sets `deliveredAt` on the execution's request synchronously, in the
     * same critical section as the delivery's step records. Throws if the
     * execution has no request.
     */
    markCancellationDelivered?: (executionId: string) => void;
    /**
     * Records the workflow version on an execution that has none,
     * synchronously, in the same critical section as a step record.
     */
    recordWorkflowVersion?: (executionId: string, workflowVersion: number | undefined) => void;
  }) {
    this.isLeaseCurrent = params.isLeaseCurrent;
    this.onStepDone = params.onStepDone;
    this.markCancellationDelivered = params.markCancellationDelivered;
    this.recordWorkflowVersion = params.recordWorkflowVersion;
  }

  getKey(params: { executionId: string; stepKey: string }) {
    return `${params.executionId}:${params.stepKey}`;
  }

  async readStep(params: { executionId: string; stepKey: string }) {
    const attempts = this.cache[this.getKey(params)];
    if (!attempts) return null;
    return clone(attempts[0]);
  }

  async listSteps(params: { executionId: string }): Promise<HeapStep[]> {
    const prefix = `${params.executionId}:`;
    const steps: HeapStep[] = [];
    for (const [key, attempts] of Object.entries(this.cache)) {
      if (!key.startsWith(prefix) || attempts.length === 0) continue;
      const kind = attempts[0].meta.kind;
      steps.push({ stepKey: key.slice(prefix.length), ...(kind !== undefined ? { kind } : {}) });
    }
    return steps;
  }

  async writeStep(params: {
    lease: ExecutionLease;
    stepKey: string;
    stepAttempt: number;
    done: boolean;
    stepResponseJson: string;
    stepKind?: string;
    workflowVersion?: number;
    deleteWaitersOf?: readonly string[];
  }): Promise<HeapRecord | LeaseLost> {
    const { lease, stepKey, stepAttempt, done, stepResponseJson } = params;
    if (!this.isLeaseCurrent(lease)) return { status: "lost" };

    const step = { executionId: lease.executionId, stepKey };
    const { record, inserted } = this.insert(step, stepAttempt, done, stepResponseJson, params.stepKind);
    this.recordWorkflowVersion?.(lease.executionId, params.workflowVersion);
    if (inserted && done) {
      this.onStepDone?.(step);
      for (const other of params.deleteWaitersOf ?? []) {
        this.onStepDone?.({ executionId: lease.executionId, stepKey: other });
      }
    }
    return clone(record);
  }

  async recordCancellation(params: {
    lease: ExecutionLease;
    steps: readonly {
      stepKey: string;
      stepAttempt: number;
      stepResponseJson: string;
      stepKind?: string;
    }[];
    workflowVersion?: number;
  }): Promise<(HeapRecord | null)[] | LeaseLost> {
    const { lease, steps } = params;
    if (!this.isLeaseCurrent(lease)) return { status: "lost" };
    if (!this.markCancellationDelivered) {
      throw new Error("This heap cannot record a cancellation: no execution client is attached");
    }
    const stored = steps.map(({ stepKey, stepAttempt }) =>
      this.read({ executionId: lease.executionId, stepKey }, stepAttempt)
    );
    const conflicting = stored.some(
      (record, i) =>
        record !== undefined &&
        (record.stepResponseJson !== steps[i].stepResponseJson || !record.meta.done)
    );
    if (conflicting) return stored.map((record) => (record ? clone(record) : null));
    // Throws before any record is written when no request is recorded.
    this.markCancellationDelivered(lease.executionId);
    this.recordWorkflowVersion?.(lease.executionId, params.workflowVersion);
    return steps.map(({ stepKey, stepAttempt, stepResponseJson, stepKind }) => {
      const step = { executionId: lease.executionId, stepKey };
      const { record } = this.insert(step, stepAttempt, true, stepResponseJson, stepKind);
      this.onStepDone?.(step);
      return clone(record);
    });
  }

  private read(step: { executionId: string; stepKey: string }, attempt: number) {
    return this.cache[this.getKey(step)]?.find((record) => record.meta.attempt === attempt);
  }

  /** A record never changes once written: an existing one is returned as stored. */
  private insert(
    step: { executionId: string; stepKey: string },
    attempt: number,
    done: boolean,
    stepResponseJson: string,
    kind: string | undefined
  ): { record: HeapRecord; inserted: boolean } {
    const existing = this.read(step, attempt);
    if (existing) return { record: existing, inserted: false };
    const attempts = (this.cache[this.getKey(step)] ??= []);
    const record: HeapRecord = {
      stepResponseJson,
      meta: { attempt, done, ...(kind !== undefined ? { kind } : {}) },
    };
    attempts.unshift(record);
    attempts.sort((a, b) => b.meta.attempt - a.meta.attempt);
    return { record, inserted: true };
  }
}

function clone(record: HeapRecord): HeapRecord {
  return { stepResponseJson: record.stepResponseJson, meta: { ...record.meta } };
}
