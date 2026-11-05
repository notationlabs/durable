import { describe, expect, test } from "vitest";
import { ExecutionPrunedError, isPrunedExecution } from "../base/retention";
import { isPrunedExecutionView, toExecutionView } from "../base/observation";
import {
  acknowledgeExecution,
  claimExecution,
  clearExecutionDivergence,
  createExecutionState,
  createStartedExecution,
  encodeExecutionEvent,
  finishExecutionPrune,
  isExecutionClaimable,
  matchExecutionAdmission,
  nextClaimAt,
  prepareExecutionAdmission,
  prepareExecutionStart,
  pruneExecution,
  requestExecutionCancellation,
  requestExecutionWake,
  toExecutionRecord,
  toExecutionRecordOrTombstone,
  type ExecutionState,
} from "./execution-state";
import { matchesExecutionListRequest, prepareExecutionListRequest } from "./execution-list";
import { readJoinTarget } from "./join-state";
import {
  isExecutionExpired,
  prunesExecutionStores,
  retentionPolicyFor,
  validateRetentionOptions,
} from "./retention";

const DAY = 86_400_000;

function admitted(workflowId = "place-order", now = 0): ExecutionState {
  return createExecutionState({
    executionId: "order-42",
    workflowId,
    encoded: encodeExecutionEvent({ params: { orderId: 42 }, context: undefined }),
    now,
  });
}

function completed(workflowId = "place-order", at = 0): ExecutionState {
  const claimed = claimExecution(admitted(workflowId, 0), { now: 0, duration: 30_000 });
  return acknowledgeExecution(claimed.state, {
    lease: claimed.lease,
    outcome: { status: "completed", result: "shipped" },
    now: at,
  }).state;
}

function pruned(state = completed(), now = 10 * DAY) {
  return pruneExecution(state, { now, deleteExecutionStore: false });
}

describe("validateRetentionOptions", () => {
  test.each([
    ["a negative period", { default: { completed: -1 } }],
    ["an infinite period", { default: { failed: Infinity } }],
    ["NaN", { default: { cancelled: NaN } }],
    ["a string period", { workflows: { "charge-card": { completed: "7d" } } }],
    ["a policy that is not an object", { default: 7 }],
    ["workflows that are not an object", { workflows: [] }],
    ["a non-boolean pruneExecutionStores", { default: { pruneExecutionStores: 1 } }],
  ])("rejects %s with a TypeError", (_label, retain) => {
    expect(() => validateRetentionOptions(retain as never)).toThrow(TypeError);
  });

  test("returns a copy, so later changes to the caller's object do not apply", () => {
    const retain = { default: { completed: DAY } };
    const validated = validateRetentionOptions(retain)!;
    retain.default.completed = 0;
    expect(validated).toEqual({ default: { completed: DAY } });
  });

  test("copies a workflow ID of __proto__ as an own entry", () => {
    const retain = JSON.parse(
      '{"default":{"completed":0},"workflows":{"__proto__":{"completed":7}}}'
    );
    const validated = validateRetentionOptions(retain)!;
    expect(Object.getPrototypeOf(validated.workflows)).toBeNull();
    expect(Object.hasOwn(validated.workflows!, "__proto__")).toBe(true);
    expect(retentionPolicyFor(validated, "__proto__")).toEqual({ completed: 7 });
    expect(retentionPolicyFor(validated, "place-order")).toEqual({ completed: 0 });
  });

  test("accepts zero and leaves undefined as undefined", () => {
    expect(validateRetentionOptions({ default: { completed: 0 } })).toEqual({
      default: { completed: 0 },
    });
    expect(validateRetentionOptions(undefined)).toBeUndefined();
  });
});

describe("retentionPolicyFor", () => {
  const retain = {
    default: { completed: 7 * DAY, failed: 30 * DAY, pruneExecutionStores: true },
    workflows: { "charge-card": { completed: 365 * DAY } },
  };

  test("a workflow's entry replaces the default as a whole", () => {
    expect(retentionPolicyFor(retain, "charge-card")).toEqual({ completed: 365 * DAY });
    expect(retentionPolicyFor(retain, "place-order")).toBe(retain.default);
    expect(prunesExecutionStores(retain, "charge-card")).toBe(false);
    expect(prunesExecutionStores(retain, "place-order")).toBe(true);
  });

  test("ignores inherited object keys as workflow IDs", () => {
    expect(retentionPolicyFor(retain, "constructor")).toBe(retain.default);
  });
});

describe("isExecutionExpired", () => {
  const retain = { default: { completed: 7 * DAY } };

  test("starts the clock when the outcome is recorded", () => {
    const state = completed("place-order", DAY);
    expect(isExecutionExpired(state, retain, 8 * DAY - 1)).toBe(false);
    expect(isExecutionExpired(state, retain, 8 * DAY)).toBe(true);
  });

  test("never expires a non-terminal or diverged execution", () => {
    const claimed = claimExecution(admitted(), { now: 0, duration: 30_000 });
    const diverged = acknowledgeExecution(claimed.state, {
      lease: claimed.lease,
      outcome: { status: "diverged", code: "NONDETERMINISTIC_REPLAY" },
      now: 0,
    }).state;
    for (const state of [admitted(), claimed.state, diverged]) {
      expect(isExecutionExpired(state, retain, 1_000 * DAY)).toBe(false);
    }
  });

  test("keeps an execution whose status has no period, or with no policy at all", () => {
    expect(isExecutionExpired(completed(), { default: { failed: 0 } }, 1_000 * DAY)).toBe(false);
    expect(isExecutionExpired(completed(), {}, 1_000 * DAY)).toBe(false);
  });

  test("never expires a tombstone again", () => {
    expect(isExecutionExpired(pruned(), retain, 1_000 * DAY)).toBe(false);
  });
});

test("no transition changes a terminal execution's updatedAt, so it stays the terminal time", () => {
  const state = completed("place-order", DAY);
  expect(requestExecutionWake(state, 2 * DAY, 2 * DAY)).toBeNull();
  expect(requestExecutionCancellation(state, { force: true }, 2 * DAY).state).toBe(state);
  expect(clearExecutionDivergence(state, 2 * DAY).state).toBe(state);
  expect(state.updatedAt).toBe(DAY);
});

describe("pruneExecution", () => {
  test("keeps the ID, workflow ID, terminal status and lease token, and clears the rest", () => {
    const parent = { executionId: "checkout-1", stepKey: "call:order", detached: false };
    const caller = { ...admitted("checkout"), executionId: "checkout-1" };
    const start = prepareExecutionStart({
      requestId: { executionId: "checkout-1", stepKey: "call:order" },
      workflowId: "place-order",
    });
    if (!start.ok) throw new Error(start.result.message);
    const child = createStartedExecution({ start: start.start, caller, now: 0 }).target;
    expect(child.parent).toEqual(parent);
    const cancelled = requestExecutionCancellation(child, { force: true, reason: "x" }, DAY).state;

    const tombstone = pruneExecution(cancelled, { now: 9 * DAY, deleteExecutionStore: true });
    expect(tombstone).toMatchObject({
      executionId: cancelled.executionId,
      workflowId: "place-order",
      status: "cancelled",
      params: "",
      context: "",
      outcome: null,
      dueAt: null,
      claimedDueAt: null,
      leaseExpiresAt: null,
      leaseToken: cancelled.leaseToken,
      parent: null,
      cancellation: null,
      workflowVersion: null,
      divergence: null,
      pruned: { prunedAt: 9 * DAY, cleanupPending: true, deleteExecutionStore: true },
    });
    expect(toExecutionRecordOrTombstone(tombstone, 9 * DAY)).toEqual({
      status: "pruned",
      executionId: cancelled.executionId,
      workflowId: "place-order",
      terminalStatus: "cancelled",
      prunedAt: 9 * DAY,
    });
    expect(() => toExecutionRecord(tombstone, 9 * DAY)).toThrow();
  });

  test("refuses a non-terminal or already pruned execution", () => {
    expect(() => pruneExecution(admitted(), { now: 0, deleteExecutionStore: false })).toThrow();
    expect(() => pruneExecution(pruned(), { now: 0, deleteExecutionStore: false })).toThrow();
  });

  test("finishExecutionPrune clears the cleanup mark only", () => {
    const tombstone = pruned();
    const finished = finishExecutionPrune(tombstone);
    expect(finished.pruned).toEqual({ ...tombstone.pruned, cleanupPending: false });
    expect(finishExecutionPrune(finished)).toBe(finished);
    const live = completed();
    expect(finishExecutionPrune(live)).toBe(live);
  });
});

describe("a tombstone", () => {
  const tombstone = pruned();

  test("is never claimable and ignores wakes", () => {
    expect(nextClaimAt(tombstone)).toBeNull();
    expect(isExecutionClaimable(tombstone, 1_000 * DAY)).toBe(false);
    expect(requestExecutionWake(tombstone, 0, 0)).toBeNull();
  });

  test("is unchanged by cancel, which returns the tombstone", () => {
    const applied = requestExecutionCancellation(tombstone, { force: true }, 11 * DAY);
    expect(applied).toEqual({
      state: tombstone,
      result: {
        status: "pruned",
        executionId: "order-42",
        workflowId: "place-order",
        terminalStatus: "completed",
        prunedAt: 10 * DAY,
      },
      propagate: false,
      terminal: false,
    });
  });

  test("has no divergence to clear", () => {
    expect(clearExecutionDivergence(tombstone, 11 * DAY)).toEqual({
      state: tombstone,
      result: { status: "not-diverged" },
    });
  });

  test("answers admission with an ID conflict, and idempotent admission with EXECUTION_PRUNED", () => {
    const request = {
      executionId: "order-42",
      workflowId: "place-order",
      params: { orderId: 42 },
      context: new Map(),
    } as const;
    const plain = prepareExecutionAdmission(request);
    const idempotent = prepareExecutionAdmission({ ...request, idempotent: true });
    if (!plain.ok || !idempotent.ok) throw new Error("invalid request");
    expect(matchExecutionAdmission(tombstone, plain)).toMatchObject({
      status: "rejected",
      code: "EXECUTION_ID_CONFLICT",
    });
    expect(matchExecutionAdmission(tombstone, idempotent)).toMatchObject({
      status: "rejected",
      code: "EXECUTION_PRUNED",
    });
  });

  test("answers a join with pruned", () => {
    expect(readJoinTarget(tombstone)).toEqual({ status: "pruned" });
  });

  test("cannot start children", () => {
    const start = prepareExecutionStart({
      requestId: { executionId: "order-42", stepKey: "call:ship" },
      workflowId: "ship-order",
    });
    if (!start.ok) throw new Error(start.result.message);
    expect(() => createStartedExecution({ start: start.start, caller: tombstone, now: 0 })).toThrow(
      /pruned/
    );
  });

  test("matches only the pruned list filter", () => {
    const matches = (request: object, state: ExecutionState) =>
      matchesExecutionListRequest(state, prepareExecutionListRequest(request), 11 * DAY);
    expect(matches({}, tombstone)).toBe(false);
    expect(matches({ status: "completed" }, tombstone)).toBe(false);
    expect(matches({ diverged: false }, tombstone)).toBe(false);
    expect(matches({ status: "pruned" }, tombstone)).toBe(true);
    expect(matches({ status: "pruned", workflowId: "other" }, tombstone)).toBe(false);
    expect(matches({ status: "pruned" }, completed())).toBe(false);
    expect(matches({ status: "completed" }, completed())).toBe(true);
  });

  test("maps to the pruned view", () => {
    const record = toExecutionRecordOrTombstone(tombstone, 11 * DAY);
    expect(isPrunedExecution(record)).toBe(true);
    const view = toExecutionView(record);
    expect(isPrunedExecutionView(view)).toBe(true);
    expect(view).toEqual(record);
  });
});

test("ExecutionPrunedError carries the tombstone", () => {
  const error = new ExecutionPrunedError({
    executionId: "order-42",
    workflowId: "place-order",
    terminalStatus: "completed",
    prunedAt: 5,
  });
  expect(error).toMatchObject({
    name: "ExecutionPrunedError",
    code: "EXECUTION_PRUNED",
    executionId: "order-42",
    workflowId: "place-order",
    terminalStatus: "completed",
    prunedAt: 5,
  });
});
