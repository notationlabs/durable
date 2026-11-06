import {
  StepKey,
  StepResponse,
  type ExecutionCancellation,
} from "@yieldstar/core";

/** A branch of `step.all` or `step.race`: any step generator. */
export type Branch<T> = AsyncGenerator<StepResponse, T, any>;

export type BranchResults<B extends readonly Branch<unknown>[]> = {
  -readonly [I in keyof B]: B[I] extends Branch<infer T> ? T : never;
};

export type SettledResult<T> =
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; error: unknown };

export type SettledResults<B extends readonly Branch<unknown>[]> = {
  -readonly [I in keyof B]: B[I] extends Branch<infer T> ? SettledResult<T> : never;
};

export type RaceResult<B extends readonly Branch<unknown>[]> = {
  [I in keyof B]: B[I] extends Branch<infer T>
    ? { index: I extends `${infer N extends number}` ? N : number; value: T }
    : never;
}[number];

export type AllOptions = {
  /** The most branches that may have a step in flight at once. */
  concurrency?: number;
  /**
   * `fail-fast` (the default) throws the first branch failure. `settle` runs
   * every branch to completion or failure and returns a status for each.
   */
  mode?: "fail-fast" | "settle";
};

/** The `StepKey` of a combinator, which is never a cancellation point itself. */
export class BranchesStepKey extends StepKey {}

/** The prefix of a patch marker's step key, which keeps it apart from step keys. */
export const PATCH_STEP_KEY_PREFIX = "$$patch$$:";

/**
 * Yielded by `step.patched`. The driver resumes it with a `StepResult` that
 * holds whether the patch applies, recording a marker when it does.
 */
export class PatchStepKey extends StepKey {
  constructor(readonly patchId: string) {
    super(`${PATCH_STEP_KEY_PREFIX}${patchId}`, { kind: "patched" });
  }
}

/**
 * Yielded by a `step.run` whose schema rejects, on replay, a value it
 * accepted on the first run. The driver ends the delivery as diverged.
 */
export class StepSchemaChanged extends StepResponse {
  static type = "step-schema-changed";
  readonly type = "step-schema-changed";
  constructor(
    readonly stepKey: string,
    readonly message: string
  ) {
    super();
  }
}

/**
 * Yielded by `step.all` and `step.race` after their cache check. The driver
 * drives `branches` concurrently, records the decision under the combinator's
 * key, and resumes the combinator with its value or throws its error.
 */
export class StepBranches extends StepResponse {
  static type = "branches";
  readonly type = "branches";
  constructor(
    readonly request: {
      combinator: "all" | "race";
      mode: "fail-fast" | "settle";
      concurrency: number;
      branches: readonly Branch<unknown>[];
    }
  ) {
    super();
  }
}

/**
 * What a settled combinator recorded. Branch values are not recorded: replay
 * resumes branches through their recorded steps and the decision picks the
 * outcome. Under `rejected` and `won`, replay stops the other branches where
 * the first run stopped them.
 * - `fulfilled`: every branch of a fail-fast `all` returned.
 * - `rejected`: branch `index` failed first in a fail-fast `all`.
 * - `settled`: every branch of a settle-mode `all` returned or failed.
 * - `won`: branch `index` was the first to return in a `race`.
 * - `failed`: every branch of a `race` failed.
 * - `cancelled`: cancellation landed inside the combinator; `err` is thrown
 *   once every branch has finished.
 */
export type BranchesDecision =
  | { decision: "fulfilled" }
  | { decision: "rejected"; index: number }
  | { decision: "settled" }
  | { decision: "won"; index: number }
  | { decision: "failed" }
  | { decision: "cancelled"; err: unknown };

export class StepBranchesDecision extends StepResponse {
  static type = "branches-decision";
  readonly type = "branches-decision";
  constructor(
    readonly decision: BranchesDecision,
    /**
     * For `rejected` and `won`, the step keys that the other branches reached
     * before the decision. On replay, those branches stop at any other key.
     */
    readonly keys: readonly string[] = [],
    /**
     * The subset of `keys` at which a branch was waiting when the decision
     * was made: its generator was not resumed past them. On replay, those
     * branches stop at these keys.
     */
    readonly held: readonly string[] = []
  ) {
    super();
  }
}

export class Limiter {
  private active = 0;
  private queue: (() => void)[] = [];

  constructor(private readonly limit: number) {}

  async acquire() {
    if (this.active < this.limit) {
      this.active++;
      return;
    }
    await new Promise<void>((resolve) => this.queue.push(resolve));
  }

  release() {
    const next = this.queue.shift();
    if (next) next();
    else this.active--;
  }
}

/**
 * Orders a combinator's decision write against the runtime calls in its
 * branches. The write waits for the calls already in flight. Calls made
 * after the write starts wait for it to finish. Work done in shared mode
 * checks first whether its branch has stopped, so no waiter is registered
 * after the decision write deleted the waiters of stopped branches.
 */
export class DecisionLock {
  private readers = 0;
  private writer: Promise<void> | undefined;
  private drained: (() => void) | undefined;

  async shared<T>(fn: () => Promise<T>): Promise<T> {
    while (this.writer) await this.writer;
    this.readers++;
    try {
      return await fn();
    } finally {
      if (--this.readers === 0) this.drained?.();
    }
  }

  async exclusive<T>(fn: () => Promise<T>): Promise<T> {
    while (this.writer) await this.writer;
    let release!: () => void;
    this.writer = new Promise((resolve) => (release = resolve));
    try {
      if (this.readers > 0) await new Promise<void>((resolve) => (this.drained = resolve));
      this.drained = undefined;
      return await fn();
    } finally {
      this.writer = undefined;
      release();
    }
  }
}

/** One combinator being driven in this delivery. */
export class Combinator {
  decision: BranchesDecision | undefined;
  /** Whether the decision was recorded before this delivery. */
  recorded = false;
  /** An error that ends the delivery, raised by one of the branches. */
  fatal: { err: unknown } | undefined;
  /** Step keys each branch and its nested branches reached, by branch index. */
  readonly keys: string[][] = [];
  /** Under a recorded `rejected` or `won`, the record's `keys` and `held`. */
  replayKeys = new Set<string>();
  replayHeld = new Set<string>();
  readonly branches: DrivenBranch[] = [];
  readonly limiter: Limiter;
  readonly lock = new DecisionLock();
  /** Checks whether the combinator can decide or settle. */
  evaluate = () => {};

  constructor(
    readonly owner: DrivenBranch,
    readonly stepKey: string,
    readonly stepAttempt: number,
    concurrency: number
  ) {
    this.limiter = new Limiter(concurrency);
  }

  /**
   * Whether `branch` must stop: the combinator failed with an error that ends
   * the delivery, or it decided `rejected` or `won` on another branch.
   */
  stops(branch: DrivenBranch): boolean {
    if (this.fatal) return true;
    return !this.recorded && this.decidedOn(branch) === false;
  }

  /**
   * Whether `branch` is replayed up to where the recorded decision stopped
   * it, and no further.
   */
  limits(branch: DrivenBranch): boolean {
    return this.recorded && this.decidedOn(branch) === false;
  }

  /** Under `rejected` or `won`, whether the decision is on `branch`. */
  private decidedOn(branch: DrivenBranch): boolean | undefined {
    const { decision } = this;
    if (decision?.decision !== "rejected" && decision?.decision !== "won") return undefined;
    return branch.index === decision.index;
  }

  /** Step keys that the branches other than `index` reached. */
  keysOutside(index: number): string[] {
    return this.keys.flatMap((keys, i) => (i === index ? [] : keys));
  }

  /** Step keys at which the branches other than `index`, or their nested branches, wait. */
  heldOutside(index: number): string[] {
    const held: string[] = [];
    const collect = (branch: DrivenBranch) => {
      if (branch.at === undefined) return;
      held.push(branch.at);
      if (branch.inner?.stepKey === branch.at) branch.inner.branches.forEach(collect);
    };
    this.branches.forEach((branch, i) => i !== index && collect(branch));
    return held;
  }

  /** Whether one of its branches may still reach a recorded step. */
  get replaying(): boolean {
    return this.branches.some((branch) => branch.replaying);
  }
}

/** The workflow itself, or one branch of a combinator. */
export class DrivenBranch {
  private holding = 0;
  private acquired: Promise<void> = Promise.resolve();
  private live = false;
  private done = false;
  /** The combinator this branch reached last. */
  inner: Combinator | undefined;
  /**
   * The step key the branch is at: reached, but the generator is not yet
   * resumed past it.
   */
  at: string | undefined;

  constructor(
    readonly parent?: Combinator,
    readonly index = 0
  ) {}

  /**
   * Whether the branch may still reach a recorded step in this delivery. A
   * branch is caught up once it finishes or reaches a step that needs new
   * work. A combinator with no recorded decision has no recorded steps after
   * it, so a branch inside one is caught up once its branches are.
   */
  get replaying(): boolean {
    if (this.done || this.live) return false;
    if (!this.inner || this.inner.recorded) return true;
    return this.inner.replaying;
  }

  /** Marks the branch as at a step that needs new work. */
  markLive() {
    this.live = true;
    this.notify();
  }

  /** Marks the branch as returned, failed, suspended or stopped. */
  markDone() {
    this.done = true;
    this.notify();
  }

  private notify() {
    for (let combinator = this.parent; combinator; combinator = combinator.owner.parent) {
      combinator.evaluate();
    }
  }

  /**
   * Whether a combinator above this branch decided on another branch, or
   * failed with an error that ends the delivery. A stopped branch starts no
   * new work, and its generator is not resumed.
   */
  get stopped(): boolean {
    for (let branch: DrivenBranch = this; branch.parent; branch = branch.parent.owner) {
      if (branch.parent.stops(branch)) return true;
    }
    return false;
  }

  /**
   * Whether a recorded decision above this branch lets it reach `stepKey`.
   * A branch the decision stopped may reach only the keys listed with it.
   */
  allows(stepKey: string): boolean {
    return this.limits().every((combinator) => combinator.replayKeys.has(stepKey));
  }

  /** Whether a recorded decision above this branch stopped it waiting at `stepKey`. */
  holds(stepKey: string): boolean {
    return this.limits().some((combinator) => combinator.replayHeld.has(stepKey));
  }

  /** Whether a recorded decision above this branch stopped it. */
  get limited(): boolean {
    return this.limits().length > 0;
  }

  /**
   * Whether a combinator above this branch has a decision other than
   * `cancelled` recorded before this delivery. Every branch under it
   * finished or was stopped before the decision was recorded.
   */
  get settled(): boolean {
    for (let branch: DrivenBranch = this; branch.parent; branch = branch.parent.owner) {
      const { decision, recorded } = branch.parent;
      if (recorded && decision !== undefined && decision.decision !== "cancelled") return true;
    }
    return false;
  }

  private limits(): Combinator[] {
    const combinators: Combinator[] = [];
    for (let branch: DrivenBranch = this; branch.parent; branch = branch.parent.owner) {
      if (branch.parent.limits(branch)) combinators.push(branch.parent);
    }
    return combinators;
  }

  reach(stepKey: string) {
    this.at = stepKey;
    for (let branch: DrivenBranch = this; branch.parent; branch = branch.parent.owner) {
      branch.parent.keys[branch.index]!.push(stepKey);
    }
  }

  /** Runs `fn` holding the decision lock of every combinator above this branch in shared mode. */
  shared<T>(fn: () => Promise<T>): Promise<T> {
    const locks: DecisionLock[] = [];
    for (let combinator = this.parent; combinator; combinator = combinator.owner.parent) {
      locks.unshift(combinator.lock);
    }
    const hold = (i: number): Promise<T> =>
      i === locks.length ? fn() : locks[i]!.shared(() => hold(i + 1));
    return hold(0);
  }

  /** Takes a concurrency slot in every combinator above this branch. */
  async acquireSlot() {
    for (let branch: DrivenBranch | undefined = this; branch?.parent; branch = branch.parent.owner) {
      if (branch.holding++ === 0) branch.acquired = branch.parent.limiter.acquire();
      await branch.acquired;
    }
  }

  releaseSlot() {
    for (let branch: DrivenBranch | undefined = this; branch?.parent; branch = branch.parent.owner) {
      if (--branch.holding === 0) branch.parent.limiter.release();
    }
  }
}

export const STOPPED = Symbol("stopped");

type Parked = {
  branch: DrivenBranch;
  stepKey: string;
  stepAttempt: number;
  resolve(err: unknown): void;
  reject(err: unknown): void;
};

/**
 * Delivers a pending cancellation to every unfinished branch at once. Each
 * branch that reaches an unfinished step parks there. When no branch is still
 * replaying, `deliver` records the throw at every parked step in one fenced
 * write, and every parked branch resumes with the same error.
 */
export class CancellationBarrier {
  /** Branches that are replaying or running, and combinator decision writes. */
  private running = 1;
  private parked: Parked[] = [];

  constructor(
    public pending: ExecutionCancellation | undefined,
    private readonly deliver: (
      request: ExecutionCancellation,
      parked: readonly { branch: DrivenBranch; stepKey: string; stepAttempt: number }[]
    ) => Promise<unknown>
  ) {}

  park(branch: DrivenBranch, stepKey: string, stepAttempt: number) {
    return new Promise<unknown>((resolve, reject) => {
      this.parked.push({ branch, stepKey, stepAttempt, resolve, reject });
      this.leave();
    });
  }

  enter(count = 1) {
    this.running += count;
  }

  leave() {
    this.running--;
    if (this.pending && this.running === 0 && this.parked.length > 0) this.flush();
  }

  /** Resumes the parked branches that are now stopped, without the error. */
  releaseStopped() {
    const stopped = this.parked.filter(({ branch }) => branch.stopped);
    if (stopped.length === 0) return;
    this.parked = this.parked.filter(({ branch }) => !branch.stopped);
    this.enter(stopped.length);
    for (const { resolve } of stopped) resolve(STOPPED);
  }

  private flush() {
    const request = this.pending!;
    const parked = this.parked;
    this.pending = undefined;
    this.parked = [];
    this.enter(parked.length);
    this.deliver(request, parked).then(
      (err) => parked.forEach(({ resolve }) => resolve(err)),
      (err) => parked.forEach(({ reject }) => reject(err))
    );
  }
}
