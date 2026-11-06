import {
  acknowledgeExecution,
  claimExecution,
  clearExecutionDivergence,
  createExecutionState,
  createStartedExecution,
  executionNotFound,
  isExecutionClaimable,
  isLeaseCurrent,
  isTerminalExecutionStatus,
  markCancellationDelivered,
  matchExecutionAdmission,
  matchesExecutionListRequest,
  matchStartRequest,
  nextClaimAt,
  prepareExecutionAdmission,
  prepareExecutionCancel,
  prepareExecutionListRequest,
  prepareExecutionStart,
  propagatedCancellation,
  recordWorkflowVersion,
  releaseExecutionLease,
  renewExecutionLease,
  requestExecutionCancellation,
  requestExecutionWake,
  toExecutionListPage,
  toExecutionRecord,
  type ExecutionAcknowledgeResult,
  type ExecutionAdmissionRequest,
  type ExecutionAdmissionResult,
  type CancellationRequest,
  type ExecutionCancelRequest,
  type ExecutionCancelResult,
  type ExecutionClaimFilter,
  type ExecutionClearDivergenceResult,
  type ExecutionClient,
  type ExecutionLease,
  type ExecutionListPage,
  type ExecutionListRequest,
  type ExecutionRecord,
  type ExecutionStartRequest,
  type ExecutionStartResult,
  type ExecutionReleaseResult,
  type ExecutionRenewResult,
  type ExecutionState,
  type ExecutionWakeResult,
  type SequencedExecutionState,
  type StartRequestState,
  type WorkflowRunOutcome,
} from "@yieldstar/core";

/**
 * In-memory execution records. Every operation reads, transitions and writes
 * a record synchronously, so each one is atomic like a database transaction.
 * Records are stored encoded, so reads are isolated from caller mutation.
 */
export class MemoryExecutionClient implements ExecutionClient {
  /**
   * In admission order, since nothing deletes from it and `set` on an
   * existing key keeps its position. An execution's admission sequence is its
   * position plus one.
   */
  private executions = new Map<string, ExecutionState>();
  /** Keyed by startRequestKey. */
  private startRequests = new Map<string, StartRequestState>();
  private clock: () => number;
  private onTerminal?: (executionId: string) => void;
  private onForceCancelled?: (executionId: string) => void;

  constructor(options?: {
    clock?: () => number;
    /**
     * Called synchronously when an acknowledgement or a forced cancel makes
     * an execution terminal, so the runtime wakes its join waiters in the
     * same write.
     */
    onTerminal?: (executionId: string) => void;
    /**
     * Called synchronously when a forced cancel makes an execution terminal,
     * so the runtime deletes the store, effect and join waiters it holds in
     * the same write.
     */
    onForceCancelled?: (executionId: string) => void;
  }) {
    // Read Date.now lazily so fake timers installed later still apply.
    this.clock = options?.clock ?? (() => Date.now());
    this.onTerminal = options?.onTerminal;
    this.onForceCancelled = options?.onForceCancelled;
  }

  async admit(
    request: ExecutionAdmissionRequest
  ): Promise<ExecutionAdmissionResult> {
    const prepared = prepareExecutionAdmission(request);
    if (!prepared.ok) {
      return {
        status: "rejected",
        code: prepared.code,
        message: prepared.message,
      };
    }
    const existing = this.executions.get(prepared.executionId);
    if (existing) return matchExecutionAdmission(existing, prepared);
    this.executions.set(
      prepared.executionId,
      createExecutionState({
        executionId: prepared.executionId,
        workflowId: request.workflowId,
        encoded: prepared.encoded,
        now: this.clock(),
      })
    );
    return { status: "accepted", executionId: prepared.executionId };
  }

  async start(request: ExecutionStartRequest): Promise<ExecutionStartResult> {
    const prepared = prepareExecutionStart(request);
    if (!prepared.ok) return prepared.result;
    const { start } = prepared;
    const key = startRequestKey(start.callerExecutionId, start.stepKey);

    const recorded = this.startRequests.get(key);
    if (recorded) return matchStartRequest(recorded, start);

    const caller = this.executions.get(start.callerExecutionId);
    if (!caller) {
      throw new Error(
        `Cannot start a workflow from execution "${start.callerExecutionId}": it does not exist`
      );
    }
    let created = createStartedExecution({ start, caller, now: this.clock() });
    while (this.executions.has(created.target.executionId)) {
      created = createStartedExecution({ start, caller, now: this.clock() });
    }
    this.executions.set(created.target.executionId, created.target);
    this.startRequests.set(key, created.startRequest);
    return { status: "accepted", executionId: created.target.executionId };
  }

  async get(executionId: string): Promise<ExecutionRecord | undefined> {
    const state = this.executions.get(executionId);
    return state ? toExecutionRecord(state, this.clock()) : undefined;
  }

  async list(request?: ExecutionListRequest): Promise<ExecutionListPage> {
    const prepared = prepareExecutionListRequest(request);
    const now = this.clock();
    const matches: SequencedExecutionState[] = [];
    const states = [...this.executions.values()];
    const before = Math.min(prepared.beforeSequence ?? Infinity, states.length + 1);
    for (
      let sequence = before - 1;
      sequence >= 1 && matches.length <= prepared.limit;
      sequence--
    ) {
      const state = states[sequence - 1];
      if (matchesExecutionListRequest(state, prepared, now)) {
        matches.push({ state, admissionSequence: sequence });
      }
    }
    return toExecutionListPage(matches, prepared, now);
  }

  async requestWake(request: {
    executionId: string;
    wakeAt: number;
  }): Promise<ExecutionWakeResult> {
    const state = this.executions.get(request.executionId);
    if (!state) return executionNotFound(request.executionId);
    const next = requestExecutionWake(state, request.wakeAt, this.clock());
    if (!next) return { status: "ignored" };
    this.executions.set(request.executionId, next);
    return { status: "requested" };
  }

  async cancel(request: ExecutionCancelRequest): Promise<ExecutionCancelResult> {
    const prepared = prepareExecutionCancel(request);
    if (!prepared.ok) throw new TypeError(`Invalid cancel request: ${prepared.message}`);
    const target = this.executions.get(prepared.executionId);
    if (!target) return executionNotFound(prepared.executionId);

    const now = this.clock();
    const pending: { state: ExecutionState; request: CancellationRequest }[] = [
      { state: target, request: prepared.request },
    ];
    let result: ExecutionCancelResult | undefined;
    while (pending.length > 0) {
      const { state, request } = pending.shift()!;
      const applied = requestExecutionCancellation(state, request, now);
      result ??= applied.result;
      if (applied.state !== state) this.executions.set(state.executionId, applied.state);
      if (applied.terminal) {
        this.onForceCancelled?.(state.executionId);
        this.onTerminal?.(state.executionId);
      }
      if (!applied.propagate) continue;
      const propagated = propagatedCancellation(applied.state);
      for (const child of this.children(state.executionId)) {
        pending.push({ state: child, request: propagated });
      }
    }
    return result!;
  }

  async clearDivergence(executionId: string): Promise<ExecutionClearDivergenceResult> {
    const state = this.executions.get(executionId);
    if (!state) return executionNotFound(executionId);
    const cleared = clearExecutionDivergence(state, this.clock());
    this.executions.set(executionId, cleared.state);
    return cleared.result;
  }

  private children(executionId: string): ExecutionState[] {
    return [...this.executions.values()].filter(
      (state) =>
        state.parent?.executionId === executionId &&
        !state.parent.detached &&
        !isTerminalExecutionStatus(state.status)
    );
  }

  /**
   * @internal Synchronously sets `deliveredAt` on the execution's request,
   * for the heap's fenced cancellation write. The caller checks the lease
   * first. Throws if the execution has no request.
   */
  markCancellationDelivered(executionId: string) {
    const state = this.executions.get(executionId);
    if (!state) throw new Error(`Execution "${executionId}" does not exist`);
    this.executions.set(executionId, markCancellationDelivered(state, this.clock()));
  }

  /**
   * @internal Synchronously records the workflow version on an execution
   * that has none, for the heap's fenced writes. The caller checks the lease
   * first.
   */
  recordWorkflowVersion(executionId: string, workflowVersion: number | undefined) {
    const state = this.executions.get(executionId);
    if (!state) return;
    this.executions.set(executionId, recordWorkflowVersion(state, workflowVersion));
  }

  /**
   * Synchronously makes an execution due now, for writes that must request a
   * wake in the same critical section as their own commit (store waiters).
   * Unknown and terminal executions are ignored.
   */
  wakeNow(executionId: string) {
    const state = this.executions.get(executionId);
    if (!state) return;
    const now = this.clock();
    const next = requestExecutionWake(state, now, now);
    if (next) this.executions.set(executionId, next);
  }

  /**
   * Synchronously checks that `lease` is current, for fenced writes that must
   * check the token in the same critical section as their own write (heap
   * step records).
   */
  isLeaseCurrent(lease: ExecutionLease): boolean {
    const state = this.executions.get(lease.executionId);
    return state !== undefined && isLeaseCurrent(state, lease);
  }

  /**
   * @internal Synchronously reads an execution's stored state, for reads that
   * must be atomic with the caller's own write (join registration). Returns
   * the stored object itself; callers must not change it.
   */
  getState(executionId: string): ExecutionState | undefined {
    return this.executions.get(executionId);
  }

  async claimDue(request: {
    limit: number;
    duration: number;
    filter?: ExecutionClaimFilter;
  }): Promise<ExecutionLease[]> {
    const now = this.clock();
    const ids = request.filter?.executionIds && new Set(request.filter.executionIds);
    const claimable = [...this.executions.values()]
      .filter(
        (state) =>
          (!ids || ids.has(state.executionId)) &&
          isExecutionClaimable(state, now)
      )
      .map((state) => ({ state, at: nextClaimAt(state)! }))
      .sort(
        (a, b) =>
          a.at - b.at ||
          (a.state.executionId < b.state.executionId ? -1 : 1)
      )
      .slice(0, Math.max(0, request.limit));

    return claimable.map(({ state }) => {
      const claimed = claimExecution(state, { now, duration: request.duration });
      this.executions.set(state.executionId, claimed.state);
      return claimed.lease;
    });
  }

  async renew(request: {
    lease: ExecutionLease;
    duration: number;
  }): Promise<ExecutionRenewResult> {
    const state = this.executions.get(request.lease.executionId);
    if (!state) return { status: "lost" };
    const renewed = renewExecutionLease(state, {
      lease: request.lease,
      duration: request.duration,
      now: this.clock(),
    });
    this.executions.set(state.executionId, renewed.state);
    return renewed.result;
  }

  async release(request: {
    lease: ExecutionLease;
  }): Promise<ExecutionReleaseResult> {
    const state = this.executions.get(request.lease.executionId);
    if (!state) return { status: "lost" };
    const released = releaseExecutionLease(state, {
      lease: request.lease,
      now: this.clock(),
    });
    this.executions.set(state.executionId, released.state);
    return released.result;
  }

  async acknowledge(request: {
    lease: ExecutionLease;
    outcome: WorkflowRunOutcome;
  }): Promise<ExecutionAcknowledgeResult> {
    const state = this.executions.get(request.lease.executionId);
    if (!state) return { status: "lost" };
    const { state: next, result } = acknowledgeExecution(state, {
      lease: request.lease,
      outcome: request.outcome,
      now: this.clock(),
    });
    this.executions.set(state.executionId, next);
    // The same state comes back when the lease is lost or the record was
    // already terminal.
    if (next !== state && isTerminalExecutionStatus(next.status)) {
      this.onTerminal?.(state.executionId);
    }
    return result;
  }
}

function startRequestKey(callerExecutionId: string, stepKey: string) {
  return JSON.stringify([callerExecutionId, stepKey]);
}
