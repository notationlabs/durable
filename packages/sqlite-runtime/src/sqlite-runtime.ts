import { Runtime, type RuntimeOptions } from "@durable/core";
import { SqliteEffectClient } from "./sqlite-effect";
import { SqliteExecutionClient } from "./sqlite-execution";
import { SqliteHeapClient } from "./sqlite-heap";
import { SqliteJoinClient } from "./sqlite-join";
import { SqliteStoreClient } from "./sqlite-store";
import type { SqliteDriver } from "./sqlite-driver";

export type SqliteRuntimeOptions = RuntimeOptions;

/**
 * Execution records, step records, stores, effects and waiters in one SQLite
 * database, so every guarantee that spans them (wakes for a store commit, an
 * effect settlement or a terminal acknowledgement, deleting a waiter with its
 * step's result) holds in a single transaction.
 */
export class SqliteRuntime extends Runtime {
  readonly clock: () => number;
  readonly executions: SqliteExecutionClient;
  readonly effects: SqliteEffectClient;
  readonly heap: SqliteHeapClient;
  readonly joins: SqliteJoinClient;
  readonly stores: SqliteStoreClient;

  constructor(db: SqliteDriver, options: SqliteRuntimeOptions = {}) {
    super();
    // Read Date.now lazily so fake timers installed later still apply.
    this.clock = options.clock ?? (() => Date.now());
    this.executions = new SqliteExecutionClient(db, { clock: this.clock });
    this.heap = new SqliteHeapClient(db, { clock: this.clock });
    this.joins = new SqliteJoinClient(db);
    this.stores = new SqliteStoreClient({ db, executions: this.executions });
    this.effects = new SqliteEffectClient({
      db,
      executions: this.executions,
      clock: this.clock,
    });
  }
}
