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
  | { status: "rejected"; error: Error };

export type SettledResults<B extends readonly Branch<unknown>[]> = {
  -readonly [I in keyof B]: B[I] extends Branch<infer T> ? SettledResult<T> : never;
};

export type RaceResult<B extends readonly Branch<unknown>[]> = {
  [I in keyof B]: B[I] extends Branch<infer T>
    ? { index: I extends `${infer N extends number}` ? N : never; value: T }
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
 * resumes every branch through its recorded steps and the decision picks the
 * outcome.
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

/** A combinator's recorded decision. */
export class StepBranchesDecision extends StepResponse {
  static type = "branches-decision";
  readonly type = "branches-decision";
  constructor(readonly decision: BranchesDecision) {
    super();
  }
}

/** Limits how many holders run at once. */
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
 * Orders a combinator's decision write after the runtime calls already in
 * flight, and runtime calls made after it behind it. A runtime call made in
 * shared mode checks for abandonment first, so no waiter is registered after
 * the decision write deleted the waiters of its abandoned branches.
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
  /** An error that ends the delivery, raised by one of the branches. */
  fatal: { err: unknown } | undefined;
  readonly limiter: Limiter;

  constructor(
    readonly owner: DrivenBranch,
    readonly stepKey: string,
    readonly stepAttempt: number,
    concurrency: number
  ) {
    this.limiter = new Limiter(concurrency);
  }

  /**
   * Whether the outcome no longer uses branch `index`: a losing race branch,
   * an unfinished branch after a fail-fast failure, or any branch once a
   * branch failed with an error that ends the delivery.
   */
  abandons(index: number): boolean {
    if (this.fatal) return true;
    const { decision } = this;
    if (decision?.decision === "won" || decision?.decision === "rejected") {
      return decision.index !== index;
    }
    return false;
  }

  /** Whether its branches must start no new work. */
  get stopsBranches(): boolean {
    if (this.fatal) return true;
    return this.decision !== undefined && this.decision.decision !== "cancelled";
  }
}

/** The workflow itself, or one branch of a combinator. */
export class DrivenBranch {
  /** Step keys this branch and its nested branches reached. */
  readonly keys: string[] = [];
  private holding = 0;
  private acquired: Promise<void> = Promise.resolve();

  constructor(
    readonly parent?: Combinator,
    readonly index = 0
  ) {}

  /** Whether a combinator above this branch no longer uses its outcome. */
  get abandoned(): boolean {
    for (let branch: DrivenBranch | undefined = this; branch?.parent; branch = branch.parent.owner) {
      if (branch.parent.abandons(branch.index)) return true;
    }
    return false;
  }

  /**
   * Whether the branch is abandoned: a combinator above it settled with a
   * decision other than `cancelled`, or failed with an error that ends the
   * delivery. It replays recorded steps and stops at the first step that
   * needs new work.
   */
  get stopped(): boolean {
    for (let combinator = this.parent; combinator; combinator = combinator.owner.parent) {
      if (combinator.stopsBranches) return true;
    }
    return false;
  }

  reach(stepKey: string) {
    for (let branch: DrivenBranch | undefined = this; branch?.parent; branch = branch.parent.owner) {
      branch.keys.push(stepKey);
    }
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
 * write, and each parked branch resumes with the error.
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
    ) => Promise<unknown[]>
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

  /** Resumes the parked branches that are now abandoned, without the error. */
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
      (errors) => parked.forEach(({ resolve }, i) => resolve(errors[i])),
      (err) => parked.forEach(({ reject }) => reject(err))
    );
  }
}
