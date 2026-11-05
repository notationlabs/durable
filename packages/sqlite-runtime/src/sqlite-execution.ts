import {
  acknowledgeExecution,
  becameTerminal,
  claimExecution,
  clearExecutionDivergence,
  createExecutionState,
  createStartedExecution,
  executionNotFound,
  finishExecutionPrune,
  isExecutionClaimable,
  isExecutionExpired,
  isTerminalExecutionStatus,
  matchExecutionAdmission,
  matchStartRequest,
  prepareExecutionAdmission,
  prepareExecutionCancel,
  prepareExecutionListRequest,
  prepareExecutionStart,
  propagatedCancellation,
  pruneExecution,
  prunesExecutionStores,
  releaseExecutionLease,
  renewExecutionLease,
  requestExecutionCancellation,
  requestExecutionWake,
  toExecutionListPage,
  toExecutionRecordOrTombstone,
  toPrunedExecutionRecord,
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
  type PrunedExecutionRecord,
  type RetentionOptions,
  type ExecutionStartRequest,
  type ExecutionStartResult,
  type ExecutionReleaseResult,
  type ExecutionRenewResult,
  type ExecutionState,
  type ExecutionWakeResult,
  type WorkflowRunOutcome,
} from "@yieldstar/core";
import { EffectWaitersDao } from "./dao/effect-waiters-dao";
import { ExecutionsDao } from "./dao/executions-dao";
import { JoinWaitersDao } from "./dao/join-waiters-dao";
import { StartRequestsDao } from "./dao/start-requests-dao";
import { StoreWaitersDao } from "./dao/store-waiters-dao";
import type { SqliteDriver } from "./sqlite-driver";
import { runTransaction } from "./sqlite-transaction";

export type SqliteExecutionClientOptions = {
  /** Reads the current time. Defaults to `Date.now`. */
  clock?: () => number;
};

/**
 * Execution records in the `executions` table. Every operation loads a row,
 * applies a pure transition from @yieldstar/core and writes it back inside one
 * IMMEDIATE transaction, so processes sharing the database file see the same
 * semantics as the memory runtime.
 */
export class SqliteExecutionClient implements ExecutionClient {
  private db: SqliteDriver;
  private executionsDao: ExecutionsDao;
  private startRequestsDao: StartRequestsDao;
  private joinWaitersDao: JoinWaitersDao;
  private storeWaitersDao: StoreWaitersDao;
  private effectWaitersDao: EffectWaitersDao;
  private clock: () => number;

  constructor(db: SqliteDriver, options: SqliteExecutionClientOptions = {}) {
    this.db = db;
    this.executionsDao = new ExecutionsDao(db);
    this.startRequestsDao = new StartRequestsDao(db);
    this.joinWaitersDao = new JoinWaitersDao(db);
    this.storeWaitersDao = new StoreWaitersDao(db);
    this.effectWaitersDao = new EffectWaitersDao(db);
    // Read Date.now lazily so fake timers installed later still apply.
    this.clock = options.clock ?? (() => Date.now());
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

    return runTransaction(this.db, () => {
      const state = createExecutionState({
        executionId: prepared.executionId,
        workflowId: request.workflowId,
        encoded: prepared.encoded,
        now: this.clock(),
      });
      // The record is inserted already due, so it and its first wake commit
      // together. The insert and the read of an existing record share the
      // transaction, so of two concurrent idempotent admissions one is
      // accepted and the other sees its record.
      if (!this.executionsDao.insertState(state)) {
        return matchExecutionAdmission(
          this.executionsDao.getState(prepared.executionId)!,
          prepared
        );
      }
      return { status: "accepted" as const, executionId: prepared.executionId };
    });
  }

  async start(request: ExecutionStartRequest): Promise<ExecutionStartResult> {
    const prepared = prepareExecutionStart(request);
    if (!prepared.ok) return prepared.result;
    const { start } = prepared;

    // The lookup, the target and the start request share one IMMEDIATE
    // transaction, so concurrent starts with one request ID serialize and a
    // new start commits all-or-nothing.
    return runTransaction(this.db, () => {
      const recorded = this.startRequestsDao.get(
        start.callerExecutionId,
        start.stepKey
      );
      if (recorded) return matchStartRequest(recorded, start);

      const caller = this.executionsDao.getState(start.callerExecutionId);
      if (!caller) {
        throw new Error(
          `Cannot start a workflow from execution "${start.callerExecutionId}": it does not exist`
        );
      }
      let created = createStartedExecution({ start, caller, now: this.clock() });
      while (!this.executionsDao.insertState(created.target)) {
        created = createStartedExecution({ start, caller, now: this.clock() });
      }
      this.startRequestsDao.insert(created.startRequest);
      return {
        status: "accepted" as const,
        executionId: created.target.executionId,
      };
    });
  }

  async get(executionId: string): Promise<ExecutionRecord | PrunedExecutionRecord | undefined> {
    const state = this.executionsDao.getState(executionId);
    return state ? toExecutionRecordOrTombstone(state, this.clock()) : undefined;
  }

  /**
   * Selects and tombstones the batch in one IMMEDIATE transaction. The join
   * waiter check is part of the selection, and `join` registers in its own
   * IMMEDIATE transaction, so a join either registers before the tombstone
   * and holds the target back, or reads the tombstone. Processes that prune
   * together serialize on the write lock, and a later one finds the
   * tombstones already written.
   */
  async prune(request: {
    retain: RetentionOptions;
    limit: number;
  }): Promise<PrunedExecutionRecord[]> {
    if (request.limit <= 0) return [];
    return runTransaction(this.db, () => {
      const now = this.clock();
      const tombstones: PrunedExecutionRecord[] = [];
      for (const executionId of this.executionsDao.getExpiredIds(
        request.retain,
        now,
        request.limit
      )) {
        const state = this.executionsDao.getState(executionId);
        if (!state || !isExecutionExpired(state, request.retain, now)) continue;
        const tombstone = pruneExecution(state, {
          now,
          deleteExecutionStore: prunesExecutionStores(request.retain, state.workflowId),
        });
        this.executionsDao.replaceWithTombstone(tombstone);
        this.joinWaitersDao.deleteTargetWaiters(executionId);
        tombstones.push(toPrunedExecutionRecord(tombstone));
      }
      return tombstones;
    });
  }

  async listPendingPrunes(request: {
    limit: number;
  }): Promise<{ executionId: string; deleteExecutionStore: boolean }[]> {
    if (request.limit <= 0) return [];
    return this.executionsDao.getPendingPrunes(request.limit);
  }

  async finishPrune(executionId: string): Promise<void> {
    runTransaction(this.db, () => {
      const state = this.executionsDao.getState(executionId);
      if (!state?.pruned) return;
      this.startRequestsDao.deleteForCaller(executionId);
      const finished = finishExecutionPrune(state);
      if (finished !== state) this.executionsDao.updateState(finished);
    });
  }

  async list(request?: ExecutionListRequest): Promise<ExecutionListPage> {
    const prepared = prepareExecutionListRequest(request);
    const now = this.clock();
    return toExecutionListPage(
      this.executionsDao.listStates(prepared, now),
      prepared,
      now
    );
  }

  async requestWake(request: {
    executionId: string;
    wakeAt: number;
  }): Promise<ExecutionWakeResult> {
    return runTransaction(this.db, () => {
      const state = this.executionsDao.getState(request.executionId);
      if (!state) return executionNotFound(request.executionId);

      const next = requestExecutionWake(state, request.wakeAt, this.clock());
      if (!next) return { status: "ignored" };

      this.executionsDao.updateState(next);
      return { status: "requested" };
    });
  }

  async cancel(request: ExecutionCancelRequest): Promise<ExecutionCancelResult> {
    const prepared = prepareExecutionCancel(request);
    if (!prepared.ok) throw new TypeError(`Invalid cancel request: ${prepared.message}`);

    // The request and its propagation to every descendant share one
    // IMMEDIATE transaction, so they commit all-or-nothing.
    return runTransaction(this.db, () => {
      const target = this.executionsDao.getState(prepared.executionId);
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
        if (applied.state !== state) this.executionsDao.updateState(applied.state);
        if (applied.terminal) this.onForceCancelled(state.executionId);
        if (!applied.propagate) continue;
        const propagated = propagatedCancellation(applied.state);
        for (const child of this.executionsDao.getAttachedChildStates(state.executionId)) {
          if (isTerminalExecutionStatus(child.status)) continue;
          pending.push({ state: child, request: propagated });
        }
      }
      return result!;
    });
  }

  async clearDivergence(executionId: string): Promise<ExecutionClearDivergenceResult> {
    return runTransaction(this.db, () => {
      const state = this.executionsDao.getState(executionId);
      if (!state) return executionNotFound(executionId);
      const cleared = clearExecutionDivergence(state, this.clock());
      if (cleared.state !== state) this.executionsDao.updateState(cleared.state);
      return cleared.result;
    });
  }

  /**
   * Deletes the waiters a force-cancelled execution holds and wakes its join
   * waiters, inside the cancel's transaction. The join waiters stay
   * registered until each joiner records its step.
   */
  private onForceCancelled(executionId: string) {
    this.storeWaitersDao.deleteExecutionWaiters(executionId);
    this.effectWaitersDao.deleteExecutionWaiters(executionId);
    this.joinWaitersDao.deleteExecutionWaiters(executionId);
    this.wakeJoinWaiters(executionId);
  }

  private wakeJoinWaiters(executionId: string) {
    for (const joiner of this.joinWaitersDao.getWaitingExecutionIds(executionId)) {
      this.wakeInTransaction(joiner);
    }
  }

  async claimDue(request: {
    limit: number;
    duration: number;
    filter?: ExecutionClaimFilter;
  }): Promise<ExecutionLease[]> {
    const executionIds = request.filter?.executionIds;
    if (request.limit <= 0 || executionIds?.length === 0) return [];
    // The IMMEDIATE transaction holds the write lock, so concurrent claims from
    // any process sharing the file see each other's leases.
    return runTransaction(this.db, () => {
      const now = this.clock();
      const leases: ExecutionLease[] = [];

      for (const executionId of this.executionsDao.getClaimableIds(
        now,
        request.limit,
        executionIds
      )) {
        const state = this.executionsDao.getState(executionId);
        if (!state || !isExecutionClaimable(state, now)) continue;

        const claimed = claimExecution(state, {
          now,
          duration: request.duration,
        });
        this.executionsDao.updateState(claimed.state);
        leases.push(claimed.lease);
      }

      return leases;
    });
  }

  async renew(request: {
    lease: ExecutionLease;
    duration: number;
  }): Promise<ExecutionRenewResult> {
    return this.transition(request.lease, (state) =>
      renewExecutionLease(state, {
        lease: request.lease,
        duration: request.duration,
        now: this.clock(),
      })
    );
  }

  async release(request: {
    lease: ExecutionLease;
  }): Promise<ExecutionReleaseResult> {
    return this.transition(request.lease, (state) =>
      releaseExecutionLease(state, { lease: request.lease, now: this.clock() })
    );
  }

  async acknowledge(request: {
    lease: ExecutionLease;
    outcome: WorkflowRunOutcome;
  }): Promise<ExecutionAcknowledgeResult> {
    return this.transition(
      request.lease,
      (state) =>
        acknowledgeExecution(state, {
          lease: request.lease,
          outcome: request.outcome,
          now: this.clock(),
        }),
      (before, after) => {
        // The join waiters stay registered until each joiner records its step.
        if (becameTerminal(before, after)) this.wakeJoinWaiters(after.executionId);
      }
    );
  }

  /**
   * Applies a fenced transition in one transaction. The transition checks the
   * lease against the stored token, so the check and the write are atomic.
   * @param afterWrite runs inside the transaction after the state is written,
   * and only when the transition changed it.
   */
  private transition<R>(
    lease: ExecutionLease,
    fn: (state: ExecutionState) => { state: ExecutionState; result: R },
    afterWrite?: (before: ExecutionState, after: ExecutionState) => void
  ): Promise<R | { status: "lost" }> {
    return Promise.resolve(
      runTransaction(this.db, () => {
        const state = this.executionsDao.getState(lease.executionId);
        if (!state) return { status: "lost" as const };
        const next = fn(state);
        if (next.state !== state) {
          this.executionsDao.updateState(next.state);
          afterWrite?.(state, next.state);
        }
        return next.result;
      })
    );
  }

  /**
   * @internal Requests a wake from inside a transaction the caller already
   * holds on the same database, so the wake commits with the caller's write
   * (e.g. a store commit). `wakeAt` defaults to the runtime clock's now.
   * Unknown and terminal executions are ignored.
   */
  wakeInTransaction(executionId: string, wakeAt: number = this.clock()) {
    const state = this.executionsDao.getState(executionId);
    if (!state) return;

    const next = requestExecutionWake(state, wakeAt, this.clock());
    if (next) this.executionsDao.updateState(next);
  }
}
