import type { SqliteDriver } from "./sqlite-driver";

/**
 * Runs `fn` in one transaction, rolling back if it throws. A driver with its
 * own `transaction` runs it. Otherwise this is `BEGIN IMMEDIATE … COMMIT`:
 * `fn` is synchronous so no other operation on this connection can
 * interleave, and IMMEDIATE takes the write lock up front so concurrent
 * processes sharing the database file serialize on it.
 */
export function runTransaction<R>(db: SqliteDriver, fn: () => R): R {
  if (db.transaction) return db.transaction(fn);
  db.run("BEGIN IMMEDIATE");
  try {
    const result = fn();
    db.run("COMMIT");
    return result;
  } catch (err) {
    db.run("ROLLBACK");
    throw err;
  }
}
