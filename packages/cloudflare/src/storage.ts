/**
 * The part of a Durable Object's `ctx.storage` that the driver and the host
 * use. `DurableObjectStorage` from `@cloudflare/workers-types` satisfies it.
 */
export interface DurableObjectStorageLike {
  sql: {
    exec(query: string, ...bindings: any[]): { toArray(): any[] };
  };
  transactionSync<R>(closure: () => R): R;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
}

/** The part of a Durable Object's `ctx` that the host uses. */
export interface DurableObjectStateLike {
  storage: DurableObjectStorageLike;
  waitUntil(promise: Promise<unknown>): void;
  /** Read to find the namespace of the object's own class. */
  id?: { toString(): string; jurisdiction?: string };
  /** Searched, before `env`, for the namespace of the object's own class. */
  exports?: object;
  /** Called to stop a delivery that this instance runs for another. */
  abort?(reason?: string): void;
}

/** The part of a Durable Object namespace that `WorkflowObject` uses. */
export interface DurableObjectNamespaceLike {
  idFromString(id: string): unknown;
  idFromName(name: string): unknown;
  get(id: unknown): unknown;
  jurisdiction?(jurisdiction: string): DurableObjectNamespaceLike;
}
