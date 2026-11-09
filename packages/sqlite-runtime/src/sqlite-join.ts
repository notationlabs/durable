import {
  readJoinTarget,
  type JoinClient,
  type JoinResult,
  type JoinWaiter,
} from "@durable/core";
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

  async join(waiter: JoinWaiter): Promise<JoinResult> {
    return runTransaction(this.db, () => {
      const result = readJoinTarget(
        this.executionsDao.getState(waiter.targetExecutionId)
      );
      if (result.status === "waiting") this.joinWaitersDao.upsertWaiter(waiter);
      return result;
    });
  }
}
