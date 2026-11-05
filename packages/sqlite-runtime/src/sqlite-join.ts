import {
  prepareJoinWaiter,
  readJoinTarget,
  type JoinClient,
  type JoinResult,
  type JoinWaiter,
} from "@yieldstar/core";
import { ExecutionsDao } from "./dao/executions-dao";
import { JoinWaitersDao } from "./dao/join-waiters-dao";
import type { SqliteDriver } from "./sqlite-driver";
import { runTransaction } from "./sqlite-transaction";

/**
 * Join waiters in the same database as the execution records. `join` reads
 * the target and registers in one IMMEDIATE transaction, which serializes with
 * the transaction of a terminal acknowledgement, so a waiter is either seen and
 * woken by the acknowledgement or sees the outcome itself.
 */
export class SqliteJoinClient implements JoinClient {
  private db: SqliteDriver;
  private executionsDao: ExecutionsDao;
  private joinWaitersDao: JoinWaitersDao;

  constructor(db: SqliteDriver) {
    this.db = db;
    this.executionsDao = new ExecutionsDao(db);
    this.joinWaitersDao = new JoinWaitersDao(db);
  }

  async join(request: JoinWaiter): Promise<JoinResult> {
    const waiter = prepareJoinWaiter(request);
    return runTransaction(this.db, () => {
      const result = readJoinTarget(
        this.executionsDao.getState(waiter.targetExecutionId)
      );
      if (result.status === "waiting") this.joinWaitersDao.upsertWaiter(waiter);
      return result;
    });
  }

  /**
   * The waiters registered on the target, ordered by joining execution and
   * then step key. Not part of `JoinClient`; for operators and tests.
   */
  getWaiters(targetExecutionId: string): JoinWaiter[] {
    return this.joinWaitersDao.getWaiters(targetExecutionId);
  }
}
