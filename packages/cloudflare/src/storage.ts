/**
 * The part of a Durable Object's `ctx.storage` that the driver and the host
 * use. `DurableObjectStorage` from `@cloudflare/workers-types` satisfies it.
 */
export interface DurableObjectStorageLike {
  sql: {
    exec(query: string, ...bindings: any[]): { toArray(): any[] };
  };
  transactionSync<R>(closure: () => R): R;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
}

/** The part of a Durable Object's `ctx` that the host uses. */
export interface DurableObjectStateLike {
  storage: DurableObjectStorageLike;
  waitUntil(promise: Promise<unknown>): void;
}
